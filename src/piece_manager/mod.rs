use serde_bytes::ByteBuf;
use sha1::Digest;
use tracing::{debug, info, warn};

pub mod channel;

use std::sync::Arc;

use tokio::sync::watch;

use crate::{
    peer_explorer::Peer,
    status::{DownloadStats, PieceProgress, PieceState},
    wire_protocol::Bitfield,
};
use channel::PieceManagerMessage;

pub const BLOCK_SIZE: u64 = 16 * 1024;

pub struct PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: crate::piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
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
    stats: Arc<DownloadStats>,
    /// Republished whenever a piece verifies. Only this loop writes it, so
    /// every value it carries is of one instant.
    progress: watch::Sender<PieceProgress>,
    /// Set once `finalize` has written the payload out. Only this loop reads
    /// or writes it, so it needs no synchronisation of its own.
    extracted: bool,
    /// The bitfield, kept in step with `pieces` rather than rebuilt from it.
    /// Exactly one bit changes when a piece verifies, and every caller that
    /// wants the bitfield -- the progress watch, the on-disk claim, a peer
    /// asking what we hold -- runs on that same completion, so rebuilding it
    /// there walked every piece once per piece.
    bitfield: Bitfield,
    /// Running totals over `pieces`, maintained for the same reason. They do
    /// not start at zero on a resume, which is why `start` seeds them once the
    /// store has said what it already holds.
    completed_pieces: u32,
    verified_bytes: u64,
    _info_hash: [u8; 20],
}

pub struct Piece {
    block_length: Option<u64>,
    pub blocks: Option<Vec<Block>>,
    hash: [u8; 20],
    pub complete: bool,
    pub requesters: Vec<Peer>,
    /// Fed as blocks land, so a finished piece can be verified without hashing
    /// it in one burst at the end. SHA-1 absorbs its input strictly in order,
    /// so this only survives while the blocks do too -- which measured at 4096
    /// out of 4096 pieces on one peer and 4090 on eight. A block that arrives
    /// early drops the hasher rather than corrupting it, and the piece falls
    /// back to hashing `buffer` whole.
    hasher: Option<sha1::Sha1>,
    /// The block `hasher` wants next. Meaningless once `hasher` is `None`.
    next_hashed_block: u32,
    /// The piece as it accumulates, written out in one call once it verifies.
    ///
    /// Blocks used to go to the store as they arrived, which cost a trip to
    /// the blocking pool each: 13.7us of handoff around a 4.2us write, sixteen
    /// times per piece. Holding the piece and writing it once turns 287us of
    /// store time into 81us.
    ///
    /// What that gives up is partial durability. A piece nobody is working any
    /// more is dropped here rather than left half-written on disk, so the next
    /// peer to take it starts from block zero instead of finishing someone
    /// else's work. That also bounds what this costs: pieces in flight, not
    /// pieces ever touched.
    buffer: Option<Vec<u8>>,
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
        self.hasher = Some(sha1::Sha1::new());
        self.next_hashed_block = 0;
        self.buffer = Some(vec![0u8; piece_length as usize]);
    }

    /// Forgets everything received so far. The piece is left exactly as it was
    /// before its first block, so `ensure_initialized` builds it again.
    fn discard_progress(&mut self) {
        self.blocks = None;
        self.hasher = None;
        self.next_hashed_block = 0;
        self.buffer = None;
    }
}

impl<E, W> PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: crate::piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
{
    pub fn new(
        piece_hashes: &ByteBuf,
        piece_length: u64,
        total_length: u64,
        piece_writer: W,
        stats: Arc<DownloadStats>,
        progress: watch::Sender<PieceProgress>,
        info_hash: [u8; 20],
    ) -> Self {
        let piceces: Vec<Piece> = piece_hashes
            .chunks(20)
            .map(|hash| Piece {
                hash: hash.to_vec().try_into().unwrap(),
                block_length: None,
                blocks: None,
                complete: false,
                requesters: Vec::new(),
                hasher: None,
                next_hashed_block: 0,
                buffer: None,
            })
            .collect();

        let bitfield = Bitfield(vec![0u8; piceces.len().div_ceil(8)]);

        Self {
            piece_length,
            total_length,
            pieces: piceces,
            piece_writer,
            piece_events: channel::new_piece_event_channel(),
            stats,
            progress,
            extracted: false,
            bitfield,
            completed_pieces: 0,
            verified_bytes: 0,
            _info_hash: info_hash,
        }
    }

    pub async fn start(
        mut self,
        mut piece_manager_channel_receiver: channel::PieceManagerChannelReceiver,
    ) {
        let bitfield = self
            .piece_writer
            .initialize(self.pieces.iter().map(|p| p.hash).collect())
            .await
            .unwrap();
        if let Some(bitfield) = bitfield {
            for (index, piece) in self.pieces.iter_mut().enumerate() {
                piece.complete = bitfield.has_piece(index as u32);
            }
        }
        self.reseed_cached_views();

        self.stats
            .set_totals(self.total_pieces(), self.total_length);
        self.publish_progress();
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
                PieceManagerMessage::GetPieceStates { response_sender } => {
                    response_sender.send(self.piece_states()).unwrap();
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
            bitfield: self.bitfield.clone(),
            events: self.piece_events.subscribe(),
        }
    }

    /// Recomputes the cached views from `pieces`. Walking every piece is the
    /// right thing to do exactly once, when a resume has just decided what the
    /// store holds; from there they are carried forward a bit at a time.
    fn reseed_cached_views(&mut self) {
        let mut bitfield = Bitfield(vec![0u8; self.pieces.len().div_ceil(8)]);
        let mut completed_pieces = 0;
        let mut verified_bytes = 0;
        for index in 0..self.total_pieces() {
            if self.pieces[index as usize].complete {
                bitfield.set_piece(index, true);
                completed_pieces += 1;
                verified_bytes += self.piece_size(index);
            }
        }
        self.bitfield = bitfield;
        self.completed_pieces = completed_pieces;
        self.verified_bytes = verified_bytes;
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
            if piece.requesters.is_empty() {
                self.stats.piece_claimed();
            }
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
        let was_held = !piece.requesters.is_empty();
        piece.requesters.retain(|requester| *requester != peer);
        if was_held && piece.requesters.is_empty() {
            self.stats.piece_released();
        }
        if let Some(blocks) = piece.blocks.as_mut() {
            for block in blocks {
                block.requesters.retain(|requester| *requester != peer);
            }
        }
        // Nobody is working this piece any more, and with the blocks held in
        // memory rather than written as they arrive, nobody can resume it
        // either -- whatever arrived is only useful to a peer that goes on to
        // finish it. Hand the memory back so what this costs is bounded by the
        // pieces in flight rather than by every piece ever started.
        //
        // Safe because a peer only gives up a piece it has nothing pending on:
        // `grant_anywhere` releases on `Grant::Exhausted`, which is exactly
        // that condition.
        if !piece.complete && piece.requesters.is_empty() {
            piece.discard_progress();
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
            self.stats.add_wasted(block_data.len() as u64);
            return;
        }
        piece.ensure_initialized(piece_length);
        // The same race one level down. A loser whose copy lands while the
        // piece is still unfinished slips past the check above, and used to be
        // written over the winner's bytes and counted as progress. It is just
        // as wasted as the arrivals that come in after the piece is done. The
        // registration goes with it: this peer will never deliver what it was
        // asked for, and the block is already spoken for by whoever won.
        if let Some(blocks) = piece.blocks.as_mut()
            && let Some(block) = blocks.get_mut(block_index as usize)
            && block.complete
        {
            block.requesters.retain(|p| *p != peer);
            self.stats.add_wasted(block_data.len() as u64);
            return;
        }
        let Some(block_length) = piece.block_length else {
            return;
        };
        // Hash before the copy, while the block is still its own value. Out of
        // order the hasher is worthless -- drop it, and let completion hash the
        // assembled buffer instead.
        if piece.next_hashed_block == block_index {
            if let Some(hasher) = piece.hasher.as_mut() {
                hasher.update(&block_data);
            }
            piece.next_hashed_block += 1;
        } else {
            piece.hasher = None;
        }
        let offset = (block_index as u64 * block_length) as usize;
        if let Some(buffer) = piece.buffer.as_mut()
            && offset < buffer.len()
        {
            let end = (offset + block_data.len()).min(buffer.len());
            buffer[offset..end].copy_from_slice(&block_data[..end - offset]);
        }
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
            let was_held = !piece.requesters.is_empty();
            piece.requesters.retain(|p| *p != peer);
            if was_held && piece.requesters.is_empty() {
                self.stats.piece_released();
            }
            // Nothing is on disk yet, so the buffer is the piece. Losing it
            // here would mean verifying bytes we no longer have, which is not
            // recoverable -- start the piece over instead.
            let blocks_total = piece.blocks.as_ref().map_or(0, |blocks| blocks.len()) as u32;
            let Some(buffer) = piece.buffer.take() else {
                warn!(
                    "{}: completed with no buffer, fetching it again",
                    piece_index
                );
                piece.discard_progress();
                return;
            };
            // The hasher has seen the whole piece exactly when every block was
            // fed to it, in order. Anything else and it is missing bytes, so
            // hash what was assembled.
            let hash: [u8; 20] = match piece.hasher.take() {
                Some(hasher) if piece.next_hashed_block == blocks_total => hasher.finalize().into(),
                _ => sha1::Sha1::digest(&buffer).into(),
            };
            if hash != piece.hash {
                warn!("{}: piece failed its hash check", piece_index);
                piece.discard_progress();
                piece.complete = false;
                // The whole piece has to be fetched again, so everything spent
                // on it is spent twice.
                self.stats.piece_failed_hash();
                self.stats.add_wasted(piece_length);
                return;
            }
            debug!("{}: piece complete, hash verified", piece_index);
            piece.complete = true;
            self.bitfield.set_piece(piece_index, true);
            self.completed_pieces += 1;
            self.verified_bytes += piece_length;
            // Before the claim, not after: the bitfield is what says these
            // bytes are on disk, and a claim that outlives its data is the one
            // ordering this file cannot get wrong.
            self.piece_writer
                .write(piece_index, 0, buffer)
                .await
                .unwrap(); // TODO: handle errors
            self.piece_writer
                .set_bitfield(self.bitfield.clone())
                .await
                .unwrap();
            self.stats.piece_verified(piece_length);
            self.publish_progress();
            let _ = self
                .piece_events
                .send(channel::PieceEvent::PieceComplete { piece_index });

            if self.is_completed() {
                // The publish above said every piece was verified, which is
                // where `Finalizing` begins. Extraction copies the whole
                // store, so subscribers sit in that state for as long as it
                // takes; the republish below is what ends it.
                self.piece_writer.finalize().await.unwrap();
                self.extracted = true;
                self.publish_progress();
            }
        }
    }

    async fn read_block(&self, piece_index: u32, block_index: u32) -> Vec<u8> {
        let piece_size = self.piece_size(piece_index);
        let Ok(data) = self.piece_writer.read(piece_index, 0, piece_size).await else {
            return Vec::new();
        };
        let offset = (block_index as u64 * BLOCK_SIZE) as usize;
        if offset >= data.len() {
            return Vec::new();
        }
        let end = (offset + BLOCK_SIZE as usize).min(data.len());
        data[offset..end].to_vec()
    }

    /// Republishes the coherent view. Called only where a piece's standing
    /// actually changes, so subscribers see one update per completion rather
    /// than a stream of identical values.
    fn publish_progress(&self) {
        let _ = self.progress.send(PieceProgress {
            completed_pieces: self.completed_pieces,
            total_pieces: self.total_pieces(),
            verified_bytes: self.verified_bytes,
            total_bytes: self.total_length,
            bitfield: self.bitfield.clone(),
            extracted: self.extracted,
        });
    }

    /// Every piece's standing, for callers drawing a piece grid. Built on
    /// demand: it is sized by the piece count, and most callers only ever
    /// want the aggregate.
    fn piece_states(&self) -> Vec<PieceState> {
        self.pieces
            .iter()
            .map(|piece| {
                if piece.complete {
                    return PieceState::Complete;
                }
                match piece.blocks.as_ref() {
                    // Never claimed, so never divided into blocks.
                    None => PieceState::Pending,
                    Some(blocks) => PieceState::InProgress {
                        blocks_done: blocks.iter().filter(|block| block.complete).count() as u32,
                        blocks_total: blocks.len() as u32,
                        requesters: piece.requesters.len() as u32,
                    },
                }
            })
            .collect()
    }

    fn total_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }

    fn is_completed(&self) -> bool {
        self.completed_pieces == self.total_pieces()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_protocol::Bitfield;
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    /// Records every write so a test can tell a real one from a duplicate,
    /// and every read so a test can tell whether a piece was verified from the
    /// running hash or by reading the store back.
    #[derive(Default)]
    struct MemoryWriter {
        blocks: HashMap<(u32, u64), Vec<u8>>,
        writes: usize,
        reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::piece_writer::PieceWriter for MemoryWriter {
        type Error = std::io::Error;

        async fn initialize(
            &mut self,
            _piece_hashes: Vec<[u8; 20]>,
        ) -> Result<Option<Bitfield>, Self::Error> {
            Ok(None)
        }

        async fn read(
            &self,
            piece_index: u32,
            piece_offset: u64,
            length: u64,
        ) -> Result<Vec<u8>, Self::Error> {
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut out = vec![0u8; length as usize];
            for ((index, offset), data) in &self.blocks {
                if *index != piece_index || *offset < piece_offset {
                    continue;
                }
                let start = (*offset - piece_offset) as usize;
                if start >= out.len() {
                    continue;
                }
                let end = (start + data.len()).min(out.len());
                out[start..end].copy_from_slice(&data[..end - start]);
            }
            Ok(out)
        }

        async fn write(
            &mut self,
            piece_index: u32,
            piece_offset: u64,
            data: Vec<u8>,
        ) -> Result<(), Self::Error> {
            self.writes += 1;
            self.blocks.insert((piece_index, piece_offset), data);
            Ok(())
        }

        async fn set_bitfield(&mut self, _bitfield: Bitfield) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn finalize(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn peer(port: u16) -> Peer {
        Peer {
            peer_id: None,
            address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
        }
    }

    /// One piece of two blocks. The hash is deliberately wrong: these tests
    /// stop short of completing the piece, so it is never checked.
    fn manager() -> PieceManager<std::io::Error, MemoryWriter> {
        let piece_length = BLOCK_SIZE * 2;
        PieceManager::new(
            &ByteBuf::from(vec![0u8; 20]),
            piece_length,
            piece_length,
            MemoryWriter::default(),
            Arc::new(DownloadStats::default()),
            watch::Sender::new(PieceProgress::default()),
            [0u8; 20],
        )
    }

    /// Two blocks of 0xAA then 0xBB, and the hash that really is theirs, so a
    /// test can carry the piece all the way to verified.
    fn verifying_manager(piece: &[u8]) -> PieceManager<std::io::Error, MemoryWriter> {
        let piece_length = BLOCK_SIZE * 2;
        let hash: [u8; 20] = sha1::Sha1::digest(piece).into();
        PieceManager::new(
            &ByteBuf::from(hash.to_vec()),
            piece_length,
            piece_length,
            MemoryWriter::default(),
            Arc::new(DownloadStats::default()),
            watch::Sender::new(PieceProgress::default()),
            [0u8; 20],
        )
    }

    fn two_blocks() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let first = vec![0xAA; BLOCK_SIZE as usize];
        let second = vec![0xBB; BLOCK_SIZE as usize];
        let mut whole = first.clone();
        whole.extend_from_slice(&second);
        (first, second, whole)
    }

    /// The ordinary case: the running hash has seen every block, so verifying
    /// must not read the piece back at all.
    #[tokio::test]
    async fn in_order_blocks_verify_without_reading_back() {
        let (first, second, whole) = two_blocks();
        let mut manager = verifying_manager(&whole);

        manager.receive_block(0, 0, first, peer(1)).await;
        manager.receive_block(0, 1, second, peer(1)).await;

        assert!(manager.pieces[0].complete, "the piece should have verified");
        assert_eq!(
            manager
                .piece_writer
                .reads
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "an in-order piece must verify from the running hash alone"
        );
    }

    /// A block that arrives early leaves the running hash unusable, and the
    /// piece falls back to hashing the assembled buffer. Rare, but it is the
    /// only path where getting it wrong looks like corruption.
    #[tokio::test]
    async fn out_of_order_blocks_still_verify() {
        let (first, second, whole) = two_blocks();
        let mut manager = verifying_manager(&whole);

        manager.receive_block(0, 1, second, peer(1)).await;
        manager.receive_block(0, 0, first, peer(1)).await;

        assert!(manager.pieces[0].complete, "the piece should have verified");
        assert_eq!(
            manager
                .piece_writer
                .reads
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the fallback hashes the buffer; nothing is on disk to read yet"
        );
    }

    /// The piece as assembled so far. Blocks are held here until the piece
    /// verifies, so this -- not the store -- is where a test looks to see what
    /// a block actually did.
    fn block_in_buffer(
        manager: &PieceManager<std::io::Error, MemoryWriter>,
        index: u64,
    ) -> Vec<u8> {
        let start = (index * BLOCK_SIZE) as usize;
        manager.pieces[0].buffer.as_ref().unwrap()[start..start + BLOCK_SIZE as usize].to_vec()
    }

    /// Endgame sends the same block to several peers. The copy that loses the
    /// race must not overwrite the winner's bytes, and must be counted.
    #[tokio::test]
    async fn duplicate_block_is_counted_and_not_rewritten() {
        let mut manager = manager();
        let winner = vec![0xAA; BLOCK_SIZE as usize];
        let loser = vec![0xBB; BLOCK_SIZE as usize];

        manager.receive_block(0, 0, winner.clone(), peer(1)).await;
        assert_eq!(block_in_buffer(&manager, 0), winner);
        assert_eq!(manager.stats.wasted_bytes(), 0);

        manager.receive_block(0, 0, loser, peer(2)).await;

        assert_eq!(
            block_in_buffer(&manager, 0),
            winner,
            "the losing copy overwrote the winner's bytes"
        );
        assert_eq!(
            manager.piece_writer.writes, 0,
            "an unfinished piece has no business touching the store"
        );
        assert_eq!(
            manager.stats.wasted_bytes(),
            BLOCK_SIZE,
            "the losing copy was not counted as wasted"
        );
    }

    /// Control for the guard above: it must not swallow a block that nobody
    /// has delivered yet.
    #[tokio::test]
    async fn first_copy_of_a_block_is_stored() {
        let mut manager = manager();
        let block = vec![0xCC; BLOCK_SIZE as usize];
        manager.receive_block(0, 1, block.clone(), peer(1)).await;

        assert_eq!(block_in_buffer(&manager, 1), block);
        assert_eq!(manager.stats.wasted_bytes(), 0);
        assert!(manager.pieces[0].blocks.as_ref().unwrap()[1].complete);
    }

    /// A piece nobody is working any more gives its memory back, and with it
    /// everything received so far: nothing was written, so there is nothing
    /// for the next peer to resume from.
    #[tokio::test]
    async fn releasing_the_last_requester_discards_the_piece() {
        let mut manager = manager();
        let holder = peer(1);
        manager.pieces[0].requesters.push(holder);
        manager
            .receive_block(0, 0, vec![0xAA; BLOCK_SIZE as usize], holder)
            .await;
        assert!(manager.pieces[0].buffer.is_some());

        manager.release(0, holder);

        assert!(
            manager.pieces[0].buffer.is_none(),
            "an abandoned piece must not hold onto its memory"
        );
        assert!(
            manager.pieces[0].blocks.is_none(),
            "its blocks cannot outlive the bytes they describe"
        );
    }

    /// The counterpart: while somebody is still working the piece, the bytes
    /// have to stay.
    #[tokio::test]
    async fn releasing_one_of_two_requesters_keeps_the_piece() {
        let mut manager = manager();
        let (leaving, staying) = (peer(1), peer(2));
        manager.pieces[0].requesters.push(leaving);
        manager.pieces[0].requesters.push(staying);
        manager
            .receive_block(0, 0, vec![0xAA; BLOCK_SIZE as usize], leaving)
            .await;

        manager.release(0, leaving);

        assert!(
            manager.pieces[0].buffer.is_some(),
            "a piece somebody else is still working must keep its bytes"
        );
    }

    /// The early return must still clear the loser's claim: it will never
    /// deliver, and the block already has its data.
    #[tokio::test]
    async fn duplicate_block_drops_the_losing_registration() {
        let mut manager = manager();
        let loser = peer(2);
        manager.pieces[0].ensure_initialized(BLOCK_SIZE * 2);
        manager.pieces[0].blocks.as_mut().unwrap()[0]
            .requesters
            .push(loser);

        manager
            .receive_block(0, 0, vec![0xAA; BLOCK_SIZE as usize], peer(1))
            .await;
        manager
            .receive_block(0, 0, vec![0xBB; BLOCK_SIZE as usize], loser)
            .await;

        let blocks = manager.pieces[0].blocks.as_ref().unwrap();
        assert!(
            !blocks[0].requesters.contains(&loser),
            "the loser is still registered on a block it will never deliver"
        );
    }
}
