use serde_bytes::ByteBuf;
use sha1::Digest;
use tracing::{debug, info, warn};

pub mod channel;
pub mod piece_writer;

use crate::{peer_explorer::Peer, wire_protocol::Bitfield};
use channel::PieceManagerMessage;

pub const BLOCK_SIZE: u64 = 16 * 1024;

pub struct PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
{
    pub piece_length: u64,
    pub total_length: u64,
    pub pieces: Vec<Piece>,
    // TODO: expose data from piece writer
    pub piece_writer: W,
    /// Fan-out of everything that completes. Connections subscribe to it so
    /// they can cancel work another peer already did and announce what we
    /// hold; nothing here waits on a subscriber.
    piece_events: channel::PieceEventSender,
}

pub struct Piece {
    block_length: Option<u64>,
    pub blocks: Option<Vec<Block>>,
    hash: [u8; 20],
    pub complete: bool,
    pub requesters: Vec<Peer>,
}

pub struct Block {
    pub complete: bool,
    pub requesters: Vec<Peer>,
}

/// Whether a claim will take a block somebody else is already downloading.
#[derive(Clone, Copy, PartialEq)]
enum Sharing {
    /// One peer per block. Nothing is downloaded twice.
    Exclusive,
    /// Endgame: the same block may be in flight from several peers, so the
    /// tail of a download is not held hostage by one slow one.
    Shared,
}

/// Outcome of asking one piece for work.
enum Grant {
    /// The peer keeps (or takes) the piece. The claim's block list can be
    /// empty when its own requests are still outstanding.
    Held(channel::Claim),
    /// Nothing here for this peer, and nothing of its own pending.
    Exhausted,
}

impl Piece {
    fn ensure_initialized(&mut self, piece_length: u64) {
        if self.blocks.is_some() {
            return;
        }
        let num_blocks = piece_length.div_ceil(BLOCK_SIZE) as usize;
        self.block_length = Some(BLOCK_SIZE);
        self.blocks = Some(
            (0..num_blocks)
                .map(|_| Block {
                    requesters: Vec::new(),
                    complete: false,
                })
                .collect(),
        );
    }
}

impl<E, W> PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
{
    pub fn new(
        piece_hashes: &ByteBuf,
        piece_length: u64,
        total_length: u64,
        piece_writer: W,
    ) -> Self {
        let piceces = piece_hashes
            .chunks(20)
            .map(|hash| Piece {
                hash: hash.to_vec().try_into().unwrap(),
                block_length: None,
                blocks: None,
                complete: false,
                requesters: Vec::new(),
            })
            .collect();

        Self {
            piece_length,
            total_length,
            pieces: piceces,
            piece_writer,
            piece_events: channel::new_piece_event_channel(),
        }
    }

    pub async fn start(
        mut self,
        mut piece_manager_channel_receiver: channel::PieceManagerChannelReceiver,
    ) {
        self.piece_writer.initialize().await.unwrap();
        info!(
            "Piece manager started: {} pieces, {} bytes/piece",
            self.total_pieces(),
            self.piece_length
        );
        while let Some(msg) = piece_manager_channel_receiver.recv().await {
            // TODO: error handling
            match msg {
                PieceManagerMessage::HasPiece {
                    piece_index,
                    response_sender,
                } => {
                    response_sender.send(self.has_piece(piece_index)).unwrap();
                }
                PieceManagerMessage::GetBitfield { response_sender } => {
                    response_sender.send(self.bitfield_snapshot()).unwrap();
                }
                PieceManagerMessage::IsInteresting {
                    bitfield,
                    response_sender,
                } => {
                    response_sender
                        .send(self.is_interesting(&bitfield))
                        .unwrap();
                }
                PieceManagerMessage::ClaimBlocks {
                    piece_index,
                    bitfield,
                    peer,
                    max_blocks,
                    response_sender,
                } => {
                    response_sender
                        .send(self.claim_blocks(piece_index, &bitfield, peer, max_blocks))
                        .unwrap();
                }
                PieceManagerMessage::Release { piece_index, peer } => {
                    self.release(piece_index, peer);
                }
                PieceManagerMessage::ReceiveBlock {
                    piece_index,
                    block_index,
                    block_data,
                    peer,
                } => {
                    self.receive_block(piece_index, block_index, block_data, peer)
                        .await;
                }
                PieceManagerMessage::ReadBlock {
                    piece_index,
                    block_index,
                    response_sender,
                } => {
                    response_sender
                        .send(self.read_block(piece_index, block_index).await)
                        .unwrap();
                }
                PieceManagerMessage::TotalPieces { response_sender } => {
                    response_sender.send(self.total_pieces()).unwrap();
                }
                PieceManagerMessage::IsCompleted { response_sender } => {
                    response_sender.send(self.is_completed()).unwrap();
                }
            }
        }
    }

    /// Bytes in `piece_index`. Every piece is `piece_length` except the last,
    /// which is whatever is left over — treating it as full length makes its
    /// hash, its block count and its block requests all wrong.
    fn piece_size(&self, piece_index: u32) -> u64 {
        let offset = piece_index as u64 * self.piece_length;
        self.piece_length
            .min(self.total_length.saturating_sub(offset))
    }

    fn has_piece(&self, piece_index: u32) -> bool {
        match self.pieces.get(piece_index as usize) {
            Some(piece) => piece.complete,
            None => true,
        }
    }

    /// The bitfield and a subscription taken together, in one turn of the
    /// loop, so nothing can complete between the two.
    fn bitfield_snapshot(&self) -> channel::BitfieldSnapshot {
        channel::BitfieldSnapshot {
            bitfield: self.bitfield(),
            events: self.piece_events.subscribe(),
        }
    }

    fn bitfield(&self) -> Bitfield {
        let mut bytes = vec![0u8; self.pieces.len().div_ceil(8)];
        for (index, piece) in self.pieces.iter().enumerate() {
            if piece.complete {
                bytes[index / 8] |= 1 << (7 - (index % 8));
            }
        }
        Bitfield(bytes)
    }

    fn is_interesting(&self, bitfield: &Bitfield) -> bool {
        self.pieces
            .iter()
            .enumerate()
            .any(|(index, piece)| !piece.complete && bitfield.has_piece(index as u32))
    }

    /// Hands `peer` work to do, registering it in the same turn of the loop
    /// that chooses it. Anything that reports availability and then registers
    /// as a second message leaves a gap two peers can both act on.
    ///
    /// `piece_index` is what the peer already holds. It is topped up until it
    /// runs dry, then handed back so somebody else can take it — in this same
    /// call, because a peer that has moved on must not still be holding it.
    /// The reply says so explicitly rather than leaving the caller to assume.
    fn claim_blocks(
        &mut self,
        piece_index: Option<u32>,
        bitfield: &Bitfield,
        peer: Peer,
        max_blocks: u32,
    ) -> channel::ClaimReply {
        let granted = self.grant_anywhere(piece_index, bitfield, peer, max_blocks);

        let mut released = None;
        if let Some(held) = piece_index
            && granted.as_ref().map(|claim| claim.piece_index) != Some(held)
        {
            self.release(held, peer);
            released = Some(held);
        }

        // One piece per peer, and never one it is not working: everything that
        // frees a piece for somebody else rests on this.
        #[cfg(debug_assertions)]
        {
            let held: Vec<u32> = self
                .pieces
                .iter()
                .enumerate()
                .filter(|(_, piece)| piece.requesters.contains(&peer))
                .map(|(index, _)| index as u32)
                .collect();
            let expected: Vec<u32> = granted.iter().map(|claim| claim.piece_index).collect();
            assert_eq!(held, expected, "peer holds a piece it is not working");
        }

        channel::ClaimReply { released, granted }
    }

    /// Finds this peer something to do, in order of preference: the piece it
    /// already holds, then one nobody holds, then — only once nothing is left
    /// unclaimed anywhere — a piece somebody else is working.
    fn grant_anywhere(
        &mut self,
        held: Option<u32>,
        bitfield: &Bitfield,
        peer: Peer,
        max_blocks: u32,
    ) -> Option<channel::Claim> {
        if let Some(held) = held
            && let Grant::Held(claim) =
                self.grant_blocks(held, peer, max_blocks, Sharing::Exclusive)
        {
            return Some(claim);
        }
        if let Some(next) = self.select_piece(bitfield)
            && let Grant::Held(claim) =
                self.grant_blocks(next, peer, max_blocks, Sharing::Exclusive)
        {
            return Some(claim);
        }

        // Everything below duplicates work, so it waits until there is no
        // untouched piece left for anyone. A peer with a poor bitfield must
        // not start racing others while whole pieces still sit unclaimed.
        if !self.is_endgame() {
            return None;
        }
        if let Some(held) = held
            && let Grant::Held(claim) = self.grant_blocks(held, peer, max_blocks, Sharing::Shared)
        {
            return Some(claim);
        }
        let next = self.select_shared_piece(bitfield, peer)?;
        match self.grant_blocks(next, peer, max_blocks, Sharing::Shared) {
            Grant::Held(claim) => Some(claim),
            Grant::Exhausted => None,
        }
    }

    /// True once every piece we still need is spoken for. That is the same
    /// condition as having more peers than unfinished pieces, but measured
    /// where the pieces are rather than counted from the connection side.
    fn is_endgame(&self) -> bool {
        !self
            .pieces
            .iter()
            .any(|piece| !piece.complete && piece.requesters.is_empty())
    }

    /// Endgame counterpart to `select_piece`: the least crowded piece this
    /// bitfield can serve, so peers spread across the remaining work instead
    /// of piling onto whichever one comes first.
    fn select_shared_piece(&self, bitfield: &Bitfield, peer: Peer) -> Option<u32> {
        self.pieces
            .iter()
            .enumerate()
            .filter(|(index, piece)| {
                !piece.complete
                    && bitfield.has_piece(*index as u32)
                    && !piece.requesters.contains(&peer)
            })
            .min_by_key(|(_, piece)| piece.requesters.len())
            .map(|(index, _)| index as u32)
    }

    /// First piece this bitfield can serve that no peer holds. Working one
    /// piece per peer means a dead connection strands at most one piece.
    fn select_piece(&self, bitfield: &Bitfield) -> Option<u32> {
        self.pieces
            .iter()
            .enumerate()
            .find(|(index, piece)| {
                !piece.complete && piece.requesters.is_empty() && bitfield.has_piece(*index as u32)
            })
            .map(|(index, _)| index as u32)
    }

    /// Registers `peer` against up to `max_blocks` blocks of one piece.
    /// `Exhausted` means the peer should let this piece go: there is nothing
    /// left here for it and nothing of its own still outstanding.
    fn grant_blocks(
        &mut self,
        piece_index: u32,
        peer: Peer,
        max_blocks: u32,
        sharing: Sharing,
    ) -> Grant {
        let piece_length = self.piece_size(piece_index);
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return Grant::Exhausted;
        };
        if piece.complete {
            return Grant::Exhausted;
        }
        // The block layout depends on this piece's own length, so it is built
        // when the piece is first claimed rather than when data first arrives.
        piece.ensure_initialized(piece_length);
        let Some(blocks) = piece.blocks.as_mut() else {
            return Grant::Exhausted;
        };

        let mut outstanding = false;
        let mut candidates: Vec<(usize, usize)> = Vec::new();
        for (index, block) in blocks.iter().enumerate() {
            if block.complete {
                continue;
            }
            // Ours already: not on offer, but the piece still has work in it.
            if block.requesters.contains(&peer) {
                outstanding = true;
                continue;
            }
            if !block.requesters.is_empty() && sharing == Sharing::Exclusive {
                continue;
            }
            candidates.push((index, block.requesters.len()));
        }

        // Least duplicated first. Under `Exclusive` every count is zero and
        // this changes nothing; in endgame it spreads peers over the tail.
        candidates.sort_by_key(|(_, requesters)| *requesters);
        if candidates.len() > max_blocks as usize {
            candidates.truncate(max_blocks as usize);
            outstanding = true;
        }
        for (index, _) in &candidates {
            blocks[*index].requesters.push(peer);
        }
        let granted: Vec<u32> = candidates.iter().map(|(index, _)| *index as u32).collect();

        if granted.is_empty() && !outstanding {
            return Grant::Exhausted;
        }
        if !piece.requesters.contains(&peer) {
            piece.requesters.push(peer);
        }
        Grant::Held(channel::Claim {
            piece_index,
            piece_length,
            blocks: granted,
        })
    }

    /// Drops every registration `peer` holds on a piece. Registrations are
    /// only cleared by data arriving, so a peer that goes away mid-piece has
    /// to give it back explicitly or the piece is locked for good.
    fn release(&mut self, piece_index: u32, peer: Peer) {
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return;
        };
        piece.requesters.retain(|requester| *requester != peer);
        if let Some(blocks) = piece.blocks.as_mut() {
            for block in blocks {
                block.requesters.retain(|requester| *requester != peer);
            }
        }
        debug!("{}: released by {}", piece_index, peer.address);
    }

    async fn receive_block(
        &mut self,
        piece_index: u32,
        block_index: u32,
        block_data: Vec<u8>,
        peer: Peer,
    ) {
        let piece_length = self.piece_size(piece_index);
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return;
        };
        // Endgame asks several peers for the same block, so the losers of that
        // race arrive here after the piece is done. Rewriting and rehashing a
        // finished piece for each one is pure waste.
        if piece.complete {
            return;
        }
        piece.ensure_initialized(piece_length);
        let Some(block_length) = piece.block_length else {
            return;
        };
        let offset = block_index as u64 * block_length;
        self.piece_writer
            .write(piece_index, offset, self.piece_length, block_data)
            .await
            .unwrap(); // TODO: handle errors
        if let Some(blocks) = piece.blocks.as_mut()
            && let Some(block) = blocks.get_mut(block_index as usize)
        {
            block.complete = true;
            block.requesters.retain(|p| *p != peer);
            // Nobody may be listening yet, and a full buffer only costs a
            // duplicate block, so a refused send is not worth reporting.
            let _ = self.piece_events.send(channel::PieceEvent::BlockComplete {
                piece_index,
                block_index,
            });
        }
        if piece
            .blocks
            .as_ref()
            .is_none_or(|blocks| blocks.iter().all(|block| block.complete))
        {
            piece.requesters.retain(|p| *p != peer);
            let data = self
                .piece_writer
                .read(piece_index, 0, self.piece_length, piece_length)
                .await
                .unwrap();
            let hash: [u8; 20] = sha1::Sha1::digest(&data).into();
            if hash != piece.hash {
                warn!("{}: piece failed its hash check", piece_index);
                piece.blocks = None;
                piece.complete = false;
                return;
            }
            debug!("{}: piece complete, hash verified", piece_index);
            piece.complete = true;
            let _ = self
                .piece_events
                .send(channel::PieceEvent::PieceComplete { piece_index });

            if self.is_completed() {
                self.piece_writer.finalize().await.unwrap();
            }
        }
    }

    async fn read_block(&self, piece_index: u32, block_index: u32) -> Vec<u8> {
        let piece_size = self.piece_size(piece_index);
        let Ok(data) = self
            .piece_writer
            .read(piece_index, 0, self.piece_length, piece_size)
            .await
        else {
            return Vec::new();
        };
        let offset = (block_index as u64 * BLOCK_SIZE) as usize;
        if offset >= data.len() {
            return Vec::new();
        }
        let end = (offset + BLOCK_SIZE as usize).min(data.len());
        data[offset..end].to_vec()
    }

    fn total_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }

    fn is_completed(&self) -> bool {
        self.pieces.iter().all(|piece| piece.complete)
    }
}
