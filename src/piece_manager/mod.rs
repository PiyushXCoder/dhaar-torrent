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
                    response_sender.send(self.bitfield()).unwrap();
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
        let mut released = None;
        let mut granted = None;

        if let Some(piece_index) = piece_index {
            match self.grant_blocks(piece_index, peer, max_blocks) {
                Grant::Held(claim) => granted = Some(claim),
                Grant::Exhausted => {
                    self.release(piece_index, peer);
                    released = Some(piece_index);
                }
            }
        }
        if granted.is_none()
            && let Some(piece_index) = self.select_piece(bitfield)
            && let Grant::Held(claim) = self.grant_blocks(piece_index, peer, max_blocks)
        {
            granted = Some(claim);
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

    /// Registers `peer` against up to `max_blocks` unclaimed blocks of one
    /// piece. `Exhausted` means the peer should let this piece go: there is
    /// nothing left here for it and nothing of its own still outstanding.
    fn grant_blocks(&mut self, piece_index: u32, peer: Peer, max_blocks: u32) -> Grant {
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

        let mut granted = Vec::new();
        let mut outstanding = false;
        for (index, block) in blocks.iter_mut().enumerate() {
            if block.complete {
                continue;
            }
            // Ours already, or somebody else's: either way not on offer, but
            // both mean the piece still has work left in it.
            if block.requesters.contains(&peer) {
                outstanding = true;
                continue;
            }
            // TODO: endgame — near the end of a download the same block
            // should be offered to several peers at once, so this is where
            // the one-requester rule needs to relax.
            if !block.requesters.is_empty() {
                continue;
            }
            if granted.len() >= max_blocks as usize {
                outstanding = true;
                continue;
            }
            block.requesters.push(peer);
            granted.push(index as u32);
        }

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
}
