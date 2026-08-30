use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

use crate::wire_protocol::Bitfield;

/// Counters written wherever the work happens and readable from anywhere.
///
/// These move far too often to be worth a message — bytes arrive in 16 KiB
/// blocks from up to `MAX_PEERS` connections at once — so they are plain
/// atomics rather than state behind an actor. `Relaxed` throughout: every one
/// of them only counts, and nothing reads two of them expecting them to
/// describe the same instant.
///
/// For a view of the pieces that *is* internally consistent, see
/// [`PieceProgress`], which the piece manager builds in one turn of its loop.
#[derive(Debug, Default)]
pub struct DownloadStats {
    downloaded_bytes: AtomicU64,
    uploaded_bytes: AtomicU64,
    wasted_bytes: AtomicU64,
    verified_bytes: AtomicU64,
    total_bytes: AtomicU64,
    completed_pieces: AtomicU32,
    total_pieces: AtomicU32,
    in_flight_pieces: AtomicU32,
    hash_failures: AtomicU32,
    active_peers: AtomicUsize,
}

impl DownloadStats {
    /// Payload received off the wire, including copies that turn out to be
    /// worthless. Compare with [`DownloadStats::verified_bytes`] to see what
    /// the transfer actually cost.
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

    /// Payload still missing — the tracker's `left` parameter.
    pub fn remaining_bytes(&self) -> u64 {
        self.total_bytes().saturating_sub(self.verified_bytes())
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
        total > 0 && self.completed_pieces() >= total
    }
}

/// What we hold, as one coherent picture.
///
/// Built inside the piece manager's loop, so the count, the byte total and
/// the bitfield are all of the same instant — unlike the counters in
/// [`DownloadStats`], which are sampled independently.
#[derive(Clone, Debug)]
pub struct PieceProgress {
    pub completed_pieces: u32,
    pub total_pieces: u32,
    pub verified_bytes: u64,
    pub total_bytes: u64,
    /// One bit per piece, set for the pieces we can serve.
    pub bitfield: Bitfield,
}

impl Default for PieceProgress {
    fn default() -> Self {
        Self {
            completed_pieces: 0,
            total_pieces: 0,
            verified_bytes: 0,
            total_bytes: 0,
            bitfield: Bitfield(Vec::new()),
        }
    }
}

/// One piece's standing, for callers that draw the piece grid.
///
/// `Pending` is distinguishable from `InProgress` because a piece is not
/// divided into blocks until somebody claims it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceState {
    Pending,
    InProgress {
        blocks_done: u32,
        blocks_total: u32,
        requesters: u32,
    },
    Complete,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DownloadState {
    /// No piece finished yet.
    #[default]
    Starting,
    Downloading,
    /// Everything is on disk; the only traffic left is what we serve.
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
