use serde_bytes::ByteBuf;
use tracing::{debug, error, info, warn};

pub mod channel;

use std::sync::Arc;

use tokio::sync::watch;

use crate::{
    peer_explorer::Peer,
    status::{DownloadStats, PieceProgress},
    wire_protocol::Bitfield,
};
use channel::PieceManagerMessage;

pub const BLOCK_SIZE: u64 = 16 * 1024;

/// How many connections may assemble the same piece at once, in endgame.
///
/// A second fetch is enough to stop one slow peer holding up the tail.
/// Uncapped, 32 peers over 64 pieces threw away 127 MiB of a 512 MiB
/// download. The cost is that a peer with nothing to share sits idle until a
/// piece completes.
const MAX_PIECE_SHARERS: usize = 2;

pub struct PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: crate::store::Store<Error = E> + Send + Sync + 'static,
{
    pub piece_length: u64,
    pub total_length: u64,
    pub pieces: Vec<Piece>,
    /// One hash per piece, in index order. Immutable for the life of the
    /// download, and handed out with every claim.
    hashes: Arc<[[u8; 20]]>,
    // TODO: expose data from store
    pub store: Arc<W>,
    /// Fan-out of everything that completes. Nothing here waits on a
    /// subscriber.
    piece_events: channel::PieceEventSender,
    stats: Arc<DownloadStats>,
    /// Republished whenever a piece verifies. Only this loop writes it, so
    /// every value it carries is of one instant.
    progress: watch::Sender<PieceProgress>,
    /// Set once `finalize` has written the payload out. Only this loop reads
    /// or writes it, so it needs no synchronisation of its own.
    extracted: bool,
    /// Kept in step with `pieces` rather than rebuilt: one bit changes per
    /// completed piece, and rebuilding walked every piece once per piece.
    bitfield: Bitfield,
    /// Running totals over `pieces`, for the same reason. Not zero on a
    /// resume, which is why `adopt_stored` seeds them.
    completed_pieces: u32,
    verified_bytes: u64,
    _info_hash: [u8; 20],
}

pub struct Piece {
    pub complete: bool,
    /// Who is assembling this piece. Normally one peer; several only in
    /// endgame, where each fetches the whole piece for itself.
    pub requesters: Vec<Peer>,
}

impl<E, W> PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: crate::store::Store<Error = E> + Send + Sync + 'static,
{
    pub fn new(
        piece_hashes: &ByteBuf,
        piece_length: u64,
        total_length: u64,
        store: Arc<W>,
        stats: Arc<DownloadStats>,
        progress: watch::Sender<PieceProgress>,
        info_hash: [u8; 20],
    ) -> Self {
        // Immutable once parsed, so shared rather than kept per piece.
        // `as_chunks` hands back the fixed-size arrays directly, so there is
        // nothing to fall back on: a hash list that is not a whole number of
        // pieces loses its ragged tail here and is refused by the store's
        // geometry check rather than panicking on the short chunk.
        let hashes: Arc<[[u8; 20]]> = piece_hashes.as_chunks::<20>().0.iter().copied().collect();
        let piceces: Vec<Piece> = piece_hashes
            .as_chunks::<20>()
            .0
            .iter()
            .map(|_| Piece {
                complete: false,
                requesters: Vec::new(),
            })
            .collect();

        let bitfield = Bitfield(vec![0u8; piceces.len().div_ceil(8)]);

        Self {
            hashes,
            piece_length,
            total_length,
            pieces: piceces,
            store,
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
        let stored = self.store.initialize(self.hashes.to_vec()).await.unwrap();
        self.adopt_stored(stored);
        self.publish_progress();
        info!(
            "Piece manager started: {} pieces, {} bytes/piece",
            self.total_pieces(),
            self.piece_length
        );
        // A `send` that fails means the connection that asked has gone away
        // between asking and being answered, which is ordinary. Nobody is
        // waiting for the answer, and the manager must not die over it.
        while let Some(msg) = piece_manager_channel_receiver.recv().await {
            match msg {
                PieceManagerMessage::HasPiece {
                    piece_index,
                    response_sender,
                } => {
                    let _ = response_sender.send(self.has_piece(piece_index));
                }
                PieceManagerMessage::GetBitfield { response_sender } => {
                    let _ = response_sender.send(self.bitfield_snapshot());
                }
                PieceManagerMessage::IsInteresting {
                    bitfield,
                    response_sender,
                } => {
                    let _ = response_sender.send(self.is_interesting(&bitfield));
                }
                PieceManagerMessage::ClaimPiece {
                    bitfield,
                    peer,
                    response_sender,
                } => {
                    let _ = response_sender.send(self.claim_piece(&bitfield, peer));
                }
                PieceManagerMessage::Release { piece_index, peer } => {
                    self.release(piece_index, peer);
                }
                PieceManagerMessage::PieceVerified { piece_index, peer } => {
                    self.piece_verified(piece_index, peer).await;
                }
                PieceManagerMessage::PieceFailed { piece_index, peer } => {
                    self.piece_failed(piece_index, peer);
                }
                PieceManagerMessage::TotalPieces { response_sender } => {
                    let _ = response_sender.send(self.total_pieces());
                }
                PieceManagerMessage::IsCompleted { response_sender } => {
                    let _ = response_sender.send(self.is_completed());
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

    /// Takes on whatever the store was found to hold.
    ///
    /// Resumed pieces go to `set_resumed`, not `piece_verified`: this session
    /// never downloaded them, and counting them as its work would break the
    /// received-against-verified comparison.
    fn adopt_stored(&mut self, stored: Option<Bitfield>) {
        if let Some(stored) = stored {
            for (index, piece) in self.pieces.iter_mut().enumerate() {
                piece.complete = stored.has_piece(index as u32);
            }
        }
        self.reseed_cached_views();
        self.stats
            .set_totals(self.total_pieces(), self.total_length);
        self.stats
            .set_resumed(self.completed_pieces, self.verified_bytes);
    }

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

    /// Takes a piece for this peer: an unclaimed one first, and only once
    /// nothing is unclaimed anywhere, one somebody else is already working.
    ///
    /// Chooses and registers in the same turn — split in two, a second peer
    /// can claim the same piece in the gap. Returns everything needed to
    /// finish the piece alone, because the manager hears nothing more about it
    /// until it is verified or given back.
    fn claim_piece(&mut self, bitfield: &Bitfield, peer: Peer) -> Option<channel::Claim> {
        let piece_index = match self.select_piece(bitfield) {
            Some(index) => index,
            // Everything below duplicates work, so it waits until there is no
            // untouched piece left for anyone. A peer with a poor bitfield
            // must not start racing others while whole pieces sit unclaimed.
            None if self.is_endgame() => self.select_shared_piece(bitfield, peer)?,
            None => return None,
        };
        let piece_length = self.piece_size(piece_index);
        let piece = self.pieces.get_mut(piece_index as usize)?;
        if piece.complete {
            return None;
        }
        if piece.requesters.is_empty() {
            self.stats.piece_claimed();
        }
        if !piece.requesters.contains(&peer) {
            piece.requesters.push(peer);
        }
        Some(channel::Claim {
            piece_index,
            hash: self.hashes[piece_index as usize],
            piece_length,
        })
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
                    && piece.requesters.len() < MAX_PIECE_SHARERS
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

    /// A connection finished a piece: it verified the bytes and wrote them.
    /// Everything here is what only this task can do -- the bitfield, the
    /// totals, and the announcement.
    ///
    /// Idempotent, because endgame lets two connections finish the same piece.
    async fn piece_verified(&mut self, piece_index: u32, peer: Peer) {
        let piece_length = self.piece_size(piece_index);
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return;
        };
        if piece.complete {
            // The second connection to finish it. Its bytes were identical, so
            // the store is fine; only the accounting must not happen twice.
            piece.requesters.retain(|p| *p != peer);
            self.stats.add_wasted(piece_length);
            return;
        }
        let was_held = !piece.requesters.is_empty();
        piece.requesters.clear();
        if was_held {
            self.stats.piece_released();
        }
        piece.complete = true;
        debug!(
            "{}: piece complete, hash verified by {}",
            piece_index, peer.address
        );

        self.bitfield.set_piece(piece_index, true);
        self.completed_pieces += 1;
        self.verified_bytes += piece_length;
        // The claim on disk is the connection's to make: it wrote the bytes,
        // so it is the only one that knows they landed. `set_bitfield` merges
        // rather than overwrites, so a connection claiming one bit cannot drop
        // anybody else's.
        self.stats.piece_verified(piece_length);
        self.publish_progress();
        let _ = self
            .piece_events
            .send(channel::PieceEvent::PieceComplete { piece_index });

        if self.is_completed() {
            // The publish above said every piece was verified, which is where
            // `Finalizing` begins. Extraction copies the whole store, so
            // subscribers sit in that state for as long as it takes; the
            // republish below is what ends it.
            match self.store.finalize().await {
                Ok(()) => self.extracted = true,
                // Every piece is verified and in the store; only the copy out
                // of it failed, which a full disk is the usual reason for.
                // Nothing downloaded is lost, so the client goes on serving
                // what it holds rather than being torn down over a copy that
                // can be made again. `extracted` stays false, so the status
                // keeps saying `Finalizing` instead of claiming a payload that
                // is not there.
                Err(e) => error!("{}: could not write the payload out: {}", piece_index, e),
            }
            self.publish_progress();
        }
    }

    /// A connection assembled a piece that did not match its hash. Nothing was
    /// written, so the piece simply goes back to being unclaimed.
    fn piece_failed(&mut self, piece_index: u32, peer: Peer) {
        let piece_length = self.piece_size(piece_index);
        warn!(
            "{}: piece failed its hash check at {}",
            piece_index, peer.address
        );
        if let Some(piece) = self.pieces.get_mut(piece_index as usize) {
            let was_held = !piece.requesters.is_empty();
            piece.requesters.retain(|p| *p != peer);
            if was_held && piece.requesters.is_empty() {
                self.stats.piece_released();
            }
        }
        // The whole piece has to be fetched again, so everything spent on it
        // is spent twice.
        self.stats.piece_failed_hash();
        self.stats.add_wasted(piece_length);
    }

    /// Gives the piece back. Registrations are only cleared by this, so a
    /// peer that goes away mid-piece has to say so or the piece is locked for
    /// good.
    fn release(&mut self, piece_index: u32, peer: Peer) {
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return;
        };
        let was_held = !piece.requesters.is_empty();
        piece.requesters.retain(|requester| *requester != peer);
        if was_held && piece.requesters.is_empty() {
            self.stats.piece_released();
        }
        debug!("{}: released by {}", piece_index, peer.address);
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
        blocks: std::sync::Mutex<HashMap<(u32, u64), Vec<u8>>>,
        /// Makes `finalize` fail, which is what a full disk looks like from
        /// here.
        finalize_fails: bool,
        writes: std::sync::atomic::AtomicUsize,
        reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::store::Store for MemoryWriter {
        type Error = std::io::Error;

        async fn initialize(
            &self,
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
            for ((index, offset), data) in self.blocks.lock().unwrap().iter() {
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
            &self,
            piece_index: u32,
            piece_offset: u64,
            data: Vec<u8>,
        ) -> Result<(), Self::Error> {
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.blocks
                .lock()
                .unwrap()
                .insert((piece_index, piece_offset), data);
            Ok(())
        }

        async fn record_piece(&self, _piece_index: u32) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn finalize(&self) -> Result<(), Self::Error> {
            if self.finalize_fails {
                return Err(std::io::Error::other("no space left on device"));
            }
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
            Arc::new(MemoryWriter::default()),
            Arc::new(DownloadStats::default()),
            watch::Sender::new(PieceProgress::default()),
            [0u8; 20],
        )
    }

    /// Hands piece 0 to `peer`, which is all a claim does now that the
    /// connection assembles the piece itself.
    fn claim(manager: &mut PieceManager<std::io::Error, MemoryWriter>, peer: Peer) {
        let mut bitfield = Bitfield(vec![0u8; 1]);
        bitfield.set_piece(0, true);
        assert!(
            manager.claim_piece(&bitfield, peer).is_some(),
            "the fixture's only piece should have been claimable"
        );
    }

    /// A released piece goes back to being unclaimed, so the next peer that
    /// asks can take it.
    #[tokio::test]
    async fn releasing_the_last_requester_frees_the_piece() {
        let mut manager = manager();
        let holder = peer(1);
        claim(&mut manager, holder);
        assert_eq!(manager.pieces[0].requesters, vec![holder]);

        manager.release(0, holder);

        assert!(
            manager.pieces[0].requesters.is_empty(),
            "an abandoned piece must not stay claimed"
        );
    }

    /// The counterpart: while somebody else is still working it, the piece
    /// stays spoken for.
    #[tokio::test]
    async fn releasing_one_of_two_requesters_keeps_the_piece_claimed() {
        let mut manager = manager();
        let (leaving, staying) = (peer(1), peer(2));
        claim(&mut manager, leaving);
        manager.pieces[0].requesters.push(staying);

        manager.release(0, leaving);

        assert_eq!(
            manager.pieces[0].requesters,
            vec![staying],
            "a piece somebody else is still working must stay claimed"
        );
    }

    /// A claim is exclusive until the tail of the download: a second peer must
    /// not be handed a piece somebody is already assembling.
    #[tokio::test]
    async fn a_claimed_piece_is_not_handed_to_a_second_peer() {
        let mut manager = manager();
        let mut bitfield = Bitfield(vec![0u8; 1]);
        bitfield.set_piece(0, true);
        assert!(manager.claim_piece(&bitfield, peer(1)).is_some());

        // The fixture has one piece, so there is nothing unclaimed left and
        // this is endgame by definition -- the second peer shares it.
        let shared = manager.claim_piece(&bitfield, peer(2));

        assert!(shared.is_some(), "endgame should let the tail be shared");
        assert_eq!(manager.pieces[0].requesters, vec![peer(1), peer(2)]);
    }

    /// The disk filling while the payload is copied out of the store is the
    /// likeliest I/O failure a client meets, and it used to panic the task
    /// that every connection talks to -- taking the whole swarm down over a
    /// copy that could simply be made again.
    #[tokio::test]
    async fn a_failed_extraction_does_not_take_the_download_with_it() {
        let mut manager = PieceManager::new(
            &ByteBuf::from(vec![0u8; 20]),
            BLOCK_SIZE * 2,
            BLOCK_SIZE * 2,
            Arc::new(MemoryWriter {
                finalize_fails: true,
                ..Default::default()
            }),
            Arc::new(DownloadStats::default()),
            watch::Sender::new(PieceProgress::default()),
            [0u8; 20],
        );
        manager.adopt_stored(None);
        let holder = peer(1);
        claim(&mut manager, holder);

        // Would have panicked before, on the `unwrap` inside.
        manager.piece_verified(0, holder).await;

        assert!(
            manager.pieces[0].complete,
            "the piece was verified; only the copy out of the store failed"
        );
        assert!(
            !manager.extracted,
            "extraction failed, so nothing may claim the payload was written"
        );
        assert_eq!(manager.completed_pieces, 1);
    }

    /// A resumed store is payload we already hold. Reporting it as still
    /// wanted is not only wrong on screen -- `remaining_bytes` is what a
    /// tracker is told in `left=`.
    #[tokio::test]
    async fn a_resumed_store_is_not_reported_as_still_wanted() {
        let mut manager = manager();
        let mut stored = Bitfield(vec![0u8; 1]);
        stored.set_piece(0, true);

        manager.adopt_stored(Some(stored));

        assert_eq!(
            manager.stats.remaining_bytes(),
            0,
            "a store holding the whole torrent still reported it as wanted"
        );
        assert!(manager.stats.is_complete());
    }

    /// Resumed pieces were not fetched this session, so they must not be
    /// counted as work it did: `downloaded_bytes` against `verified_bytes` is
    /// how waste is measured, and one of them never happened.
    #[tokio::test]
    async fn a_resumed_store_is_not_counted_as_this_sessions_work() {
        let mut manager = manager();
        let mut stored = Bitfield(vec![0u8; 1]);
        stored.set_piece(0, true);

        manager.adopt_stored(Some(stored));

        assert_eq!(
            manager.stats.verified_bytes(),
            0,
            "resumed bytes were counted as verified by this session"
        );
        assert_eq!(manager.stats.held_bytes(), BLOCK_SIZE * 2);
    }

    /// Nothing was held, so nothing is claimed -- the control for the two
    /// above.
    #[tokio::test]
    async fn a_fresh_store_holds_nothing() {
        let mut manager = manager();

        manager.adopt_stored(None);

        assert_eq!(manager.stats.remaining_bytes(), BLOCK_SIZE * 2);
        assert_eq!(manager.stats.held_bytes(), 0);
        assert!(!manager.stats.is_complete());
    }

    /// Endgame shares a piece so one slow peer cannot hold up the tail, but
    /// only so far: past the cap a peer is turned away rather than added to a
    /// pile that will cancel most of them.
    #[tokio::test]
    async fn endgame_stops_sharing_a_piece_past_the_cap() {
        let mut manager = manager();
        let mut bitfield = Bitfield(vec![0u8; 1]);
        bitfield.set_piece(0, true);

        // The fixture has one piece, so there is nothing unclaimed after the
        // first take and everything below is the endgame path.
        for port in 1..=MAX_PIECE_SHARERS as u16 {
            assert!(
                manager.claim_piece(&bitfield, peer(port)).is_some(),
                "peer {port} should have been allowed to share"
            );
        }

        let over_cap = manager.claim_piece(&bitfield, peer(99));

        assert!(over_cap.is_none(), "the cap did not hold");
        assert_eq!(manager.pieces[0].requesters.len(), MAX_PIECE_SHARERS);
    }

    /// The bookkeeping a verified piece triggers is the manager's alone: the
    /// bitfield, the totals, and the claim on disk.
    #[tokio::test]
    async fn a_verified_piece_is_recorded_once() {
        let mut manager = manager();
        let holder = peer(1);
        claim(&mut manager, holder);

        manager.piece_verified(0, holder).await;

        assert!(manager.pieces[0].complete);
        assert_eq!(manager.completed_pieces, 1);
        assert!(manager.bitfield.has_piece(0));
        assert_eq!(manager.stats.wasted_bytes(), 0);
    }

    /// Endgame lets two connections finish the same piece. The second report
    /// must change nothing except the wasted count.
    #[tokio::test]
    async fn a_second_report_of_the_same_piece_changes_nothing() {
        let mut manager = manager();
        claim(&mut manager, peer(1));
        manager.piece_verified(0, peer(1)).await;
        let verified = manager.verified_bytes;

        manager.piece_verified(0, peer(2)).await;

        assert_eq!(
            manager.completed_pieces, 1,
            "the duplicate was counted as a second piece"
        );
        assert_eq!(manager.verified_bytes, verified);
        assert_eq!(
            manager.stats.wasted_bytes(),
            BLOCK_SIZE * 2,
            "the duplicate piece was not counted as wasted"
        );
    }

    /// A piece that failed its hash goes back to being unclaimed, so another
    /// peer can fetch it from the start.
    #[tokio::test]
    async fn a_failed_piece_is_unclaimed_and_counted() {
        let mut manager = manager();
        let holder = peer(1);
        claim(&mut manager, holder);

        manager.piece_failed(0, holder);

        assert!(!manager.pieces[0].complete);
        assert!(manager.pieces[0].requesters.is_empty());
        assert_eq!(manager.stats.wasted_bytes(), BLOCK_SIZE * 2);
    }
}
