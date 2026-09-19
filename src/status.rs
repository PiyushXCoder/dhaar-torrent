use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

use crate::wire_protocol::Bitfield;

/// Counters written wherever the work happens and readable from anywhere.
///
/// `Relaxed` throughout: each one only counts, and nothing reads two of them
/// expecting the same instant. For a view that *is* internally consistent, see
/// [`PieceProgress`].
#[derive(Debug, Default)]
pub struct DownloadStats {
    downloaded_bytes: AtomicU64,
    uploaded_bytes: AtomicU64,
    wasted_bytes: AtomicU64,
    verified_bytes: AtomicU64,
    /// Payload the store already held at startup. Kept apart from
    /// `verified_bytes` because that one is what this session fetched, and
    /// comparing it with `downloaded_bytes` is how waste is measured.
    resumed_bytes: AtomicU64,
    total_bytes: AtomicU64,
    completed_pieces: AtomicU32,
    /// Pieces the store already held, for the same reason.
    resumed_pieces: AtomicU32,
    total_pieces: AtomicU32,
    in_flight_pieces: AtomicU32,
    hash_failures: AtomicU32,
    active_peers: AtomicUsize,
}

impl DownloadStats {
    /// Payload received off the wire, worthless copies included. Against
    /// [`DownloadStats::verified_bytes`], this is what the transfer cost.
    pub fn add_downloaded(&self, bytes: u64) {
        self.downloaded_bytes.fetch_add(bytes, Relaxed);
    }

    pub fn add_uploaded(&self, bytes: u64) {
        self.uploaded_bytes.fetch_add(bytes, Relaxed);
    }

    /// Bytes paid for and thrown away: endgame copies that lost their race,
    /// and pieces discarded for failing their hash.
    pub fn add_wasted(&self, bytes: u64) {
        self.wasted_bytes.fetch_add(bytes, Relaxed);
    }

    pub fn piece_verified(&self, bytes: u64) {
        self.verified_bytes.fetch_add(bytes, Relaxed);
        self.completed_pieces.fetch_add(1, Relaxed);
    }

    pub fn piece_failed_hash(&self) {
        self.hash_failures.fetch_add(1, Relaxed);
    }

    pub fn set_totals(&self, pieces: u32, bytes: u64) {
        self.total_pieces.store(pieces, Relaxed);
        self.total_bytes.store(bytes, Relaxed);
    }

    /// Records what a resumed store was found to hold. Called once, before any
    /// piece of this session verifies.
    pub fn set_resumed(&self, pieces: u32, bytes: u64) {
        self.resumed_pieces.store(pieces, Relaxed);
        self.resumed_bytes.store(bytes, Relaxed);
    }

    /// Called as a piece gains its first holder and loses its last, so this
    /// counts pieces being worked rather than peers working them.
    pub fn piece_claimed(&self) {
        self.in_flight_pieces.fetch_add(1, Relaxed);
    }

    pub fn piece_released(&self) {
        // Saturating, so a release that races ahead of its claim cannot wrap
        // the counter to `u32::MAX` and make the download look busy forever.
        let _ = self
            .in_flight_pieces
            .fetch_update(Relaxed, Relaxed, |count| Some(count.saturating_sub(1)));
    }

    pub fn peer_connected(&self) {
        self.active_peers.fetch_add(1, Relaxed);
    }

    pub fn peer_disconnected(&self) {
        let _ = self
            .active_peers
            .fetch_update(Relaxed, Relaxed, |count| Some(count.saturating_sub(1)));
    }

    pub fn downloaded_bytes(&self) -> u64 {
        self.downloaded_bytes.load(Relaxed)
    }

    pub fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes.load(Relaxed)
    }

    pub fn wasted_bytes(&self) -> u64 {
        self.wasted_bytes.load(Relaxed)
    }

    pub fn verified_bytes(&self) -> u64 {
        self.verified_bytes.load(Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Relaxed)
    }

    /// Everything we hold: this session's work plus what the store already
    /// had.
    pub fn held_bytes(&self) -> u64 {
        self.verified_bytes() + self.resumed_bytes.load(Relaxed)
    }

    /// Pieces we hold, from this session and from the store together.
    pub fn held_pieces(&self) -> u32 {
        self.completed_pieces() + self.resumed_pieces.load(Relaxed)
    }

    /// Payload still missing — the tracker's `left` parameter.
    pub fn remaining_bytes(&self) -> u64 {
        self.total_bytes().saturating_sub(self.held_bytes())
    }

    pub fn completed_pieces(&self) -> u32 {
        self.completed_pieces.load(Relaxed)
    }

    pub fn total_pieces(&self) -> u32 {
        self.total_pieces.load(Relaxed)
    }

    pub fn in_flight_pieces(&self) -> u32 {
        self.in_flight_pieces.load(Relaxed)
    }

    pub fn hash_failures(&self) -> u32 {
        self.hash_failures.load(Relaxed)
    }

    pub fn active_peers(&self) -> usize {
        self.active_peers.load(Relaxed)
    }

    /// Whether every piece is accounted for. False before the totals are
    /// known, so an empty torrent never reads as finished at startup.
    pub fn is_complete(&self) -> bool {
        let total = self.total_pieces();
        total > 0 && self.held_pieces() >= total
    }
}

/// What we hold, as one coherent picture: built in a single turn of the piece
/// manager's loop, so every field describes the same instant.
#[derive(Clone, Debug)]
pub struct PieceProgress {
    pub completed_pieces: u32,
    pub total_pieces: u32,
    pub verified_bytes: u64,
    pub total_bytes: u64,
    /// One bit per piece, set for the pieces we can serve.
    pub bitfield: Bitfield,
    /// Whether the payload has been written out of the store into its real
    /// shape. Carried here rather than inferred from the piece count because
    /// nothing else can know it: every piece can be verified and on disk while
    /// the extracted file does not exist yet.
    pub extracted: bool,
}

impl Default for PieceProgress {
    fn default() -> Self {
        Self {
            completed_pieces: 0,
            total_pieces: 0,
            verified_bytes: 0,
            total_bytes: 0,
            bitfield: Bitfield(Vec::new()),
            extracted: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DownloadState {
    /// No piece finished yet.
    #[default]
    Starting,
    Downloading,
    /// Every piece is verified, but the payload is still being written out of
    /// the store into the files the torrent describes. Its own state because
    /// it is not instant: the store is copied whole, so this lasts about as
    /// long as writing the download again, and a large torrent spends minutes
    /// here looking finished while the file it names does not yet exist.
    Finalizing,
    /// Everything is on disk in its real shape; the only traffic left is what
    /// we serve.
    Seeding,
}

/// A sampled view of the whole download, published on a timer.
///
/// The rates are the reason this is sampled rather than read: they cannot be
/// derived from a counter at one instant, only from two of them over a known
/// interval.
#[derive(Clone, Debug, Default)]
pub struct DownloadStatus {
    pub state: DownloadState,
    pub pieces: PieceProgress,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub wasted_bytes: u64,
    pub hash_failures: u32,
    pub in_flight_pieces: u32,
    pub active_peers: usize,
    /// Bytes per second over the last sampling interval.
    pub download_rate: u64,
    pub upload_rate: u64,
}

impl DownloadStatus {
    /// Verified payload as a fraction of the whole, 0.0 to 1.0.
    pub fn progress(&self) -> f64 {
        if self.pieces.total_bytes == 0 {
            return 0.0;
        }
        self.pieces.verified_bytes as f64 / self.pieces.total_bytes as f64
    }
}
