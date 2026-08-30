pub mod config;
pub mod error;
pub mod helpers;
pub mod peer_connection;
pub mod peer_explorer;
pub mod peer_manager;
pub mod piece_manager;
pub mod status;
pub mod torrent_parser;
pub mod wire_protocol;

use std::{path::Path, sync::Arc, time::Duration};

use tokio::{sync::watch, task::JoinSet, time};

use crate::{
    error::Result,
    helpers::generate_random_peer_id,
    peer_explorer::{
        PeerExplorer, PeerSource, channel::new_peer_explorer_channel, tracker::TrackerManager,
    },
    peer_manager::{
        PeerManager,
        peer_selection_strategy::{PeerSelectionStrategy, RetryAfterDelayPeerSelectionStrategy},
    },
    piece_manager::{
        PieceManager,
        channel::new_piece_manager_channel,
        piece_writer::{DiskPieceWriter, PieceWriter},
    },
    status::{DownloadState, DownloadStats, DownloadStatus, PieceProgress},
    torrent_parser::{TorrentParser, metadata::Torrent, parser::TorrentFileParser},
};

/// How often the sampled [`DownloadStatus`] is republished. Rates are measured
/// across this window, so it is also how quickly they answer to a change.
const STATUS_INTERVAL: Duration = Duration::from_secs(1);

/// One torrent, and everything needed to fetch it.
///
/// The pieces of this crate are separate actors that only talk over channels,
/// which makes them easy to test in isolation and tedious to assemble by hand.
/// `Download` owns that assembly: the channels between the actors are an
/// internal detail, and callers hold a value rather than a set of tasks.
///
/// The parts a caller might reasonably want to swap are type parameters —
/// where bytes land (`W`), and which peer to try next (`S`) — while peer
/// sources are trait objects because a download usually draws on several at
/// once. [`Download::from_torrent_file`] fills all three in with the defaults.
pub struct Download<W, S>
where
    W: PieceWriter + Send + Sync + 'static,
    W::Error: std::error::Error + Send + Sync + 'static,
    S: PeerSelectionStrategy + Send + Sync + 'static,
{
    torrent: Torrent,
    peer_id: [u8; 20],
    piece_writer: W,
    peer_selection_strategy: S,
    peer_sources: Vec<Box<dyn PeerSource + Send>>,
    stats: Arc<DownloadStats>,
    progress_sender: watch::Sender<PieceProgress>,
    status_sender: watch::Sender<DownloadStatus>,
}

impl Download<DiskPieceWriter, RetryAfterDelayPeerSelectionStrategy> {
    /// Reads a `.torrent` file and takes the usual defaults: written to disk
    /// beside the current directory, peers from the trackers the file names,
    /// failed peers retried after a delay.
    pub fn from_torrent_file(path: &Path) -> Result<Self> {
        Ok(Self::from_torrent(TorrentFileParser::parse_from_file_path(
            path,
        )?))
    }

    /// As [`Download::from_torrent_file`], for metadata already in hand.
    pub fn from_torrent(torrent: Torrent) -> Self {
        let peer_id = generate_random_peer_id();
        // Built first: the tracker announces these figures, so it needs the
        // same counters the connections will be writing to.
        let stats = Arc::new(DownloadStats::default());
        let tracker_manager = TrackerManager::new(
            torrent.announce_urls(),
            &torrent.info_hash,
            &peer_id,
            stats.clone(),
        );
        let piece_writer = DiskPieceWriter::new(
            torrent.info.total_length(),
            &torrent.info.name,
            torrent.info.length,
            &torrent.info.md5sum,
            &torrent.info.files,
        );

        Self::new(
            torrent,
            peer_id,
            piece_writer,
            RetryAfterDelayPeerSelectionStrategy::new(),
            vec![Box::new(tracker_manager)],
            stats,
        )
    }
}

impl<W, S> Download<W, S>
where
    W: PieceWriter + Send + Sync + 'static,
    W::Error: std::error::Error + Send + Sync + 'static,
    S: PeerSelectionStrategy + Send + Sync + 'static,
{
    /// Every part chosen explicitly. `peer_id` identifies us to peers for the
    /// life of the download and must stay fixed once announced to a tracker.
    ///
    /// `stats` is taken rather than created because peer sources are built by
    /// the caller and may need to read it — a tracker announce is meaningless
    /// without the byte counts.
    pub fn new(
        torrent: Torrent,
        peer_id: [u8; 20],
        piece_writer: W,
        peer_selection_strategy: S,
        peer_sources: Vec<Box<dyn PeerSource + Send>>,
        stats: Arc<DownloadStats>,
    ) -> Self {
        Self {
            torrent,
            peer_id,
            piece_writer,
            peer_selection_strategy,
            peer_sources,
            stats,
            progress_sender: watch::Sender::new(PieceProgress::default()),
            status_sender: watch::Sender::new(DownloadStatus::default()),
        }
    }

    pub fn torrent(&self) -> &Torrent {
        &self.torrent
    }

    pub fn peer_id(&self) -> &[u8; 20] {
        &self.peer_id
    }

    /// The live counters. Readable at any time and from anywhere, including
    /// before the download starts.
    pub fn stats(&self) -> Arc<DownloadStats> {
        self.stats.clone()
    }

    /// A feed of sampled status. Subscribing before [`Download::spawn`] is
    /// fine — the receiver holds the empty starting value until the first
    /// sample lands.
    pub fn subscribe(&self) -> watch::Receiver<DownloadStatus> {
        self.status_sender.subscribe()
    }

    /// Starts every actor and returns at once, handing back the means to
    /// watch and to stop them.
    pub fn spawn(self) -> DownloadHandle {
        let (peer_explorer_channel_sender, peer_explorer_channel_receiver) =
            new_peer_explorer_channel();
        let (piece_manager_channel_sender, piece_manager_channel_receiver) =
            new_piece_manager_channel();

        let peer_explorer = PeerExplorer::new(self.peer_sources);
        let piece_manager = PieceManager::new(
            &self.torrent.info.pieces,
            self.torrent.info.piece_length,
            self.torrent.info.total_length(),
            self.piece_writer,
            self.stats.clone(),
            self.progress_sender.clone(),
        );
        let peer_manager = PeerManager::new(
            self.peer_selection_strategy,
            &self.torrent.info_hash,
            &self.peer_id,
            self.stats.clone(),
        );

        let status = self.status_sender.subscribe();
        let progress = self.progress_sender.subscribe();

        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(peer_explorer.start(peer_explorer_channel_sender));
        tasks.spawn(piece_manager.start(piece_manager_channel_receiver));
        tasks.spawn(
            peer_manager.start(peer_explorer_channel_receiver, piece_manager_channel_sender),
        );
        tasks.spawn(sample_status(
            self.stats.clone(),
            progress,
            self.status_sender,
        ));

        DownloadHandle {
            stats: self.stats,
            status,
            tasks,
        }
    }

    /// Runs until the first actor stops. Any one of them stopping means the
    /// download cannot continue — they are only useful in each other's
    /// company — so the rest are dropped rather than left running.
    pub async fn start(self) {
        self.spawn().wait().await;
    }
}

/// A running download.
///
/// Dropping this aborts every actor, so it has to be held for as long as the
/// download should live.
pub struct DownloadHandle {
    stats: Arc<DownloadStats>,
    status: watch::Receiver<DownloadStatus>,
    tasks: JoinSet<()>,
}

impl DownloadHandle {
    /// The live counters, updated as the work happens rather than on the
    /// sampling clock.
    pub fn stats(&self) -> &Arc<DownloadStats> {
        &self.stats
    }

    /// The most recent sample. Cheap, and never waits.
    pub fn status(&self) -> DownloadStatus {
        self.status.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<DownloadStatus> {
        self.status.clone()
    }

    /// Waits for the next sample. `false` once the download has stopped and
    /// no further sample will arrive.
    pub async fn changed(&mut self) -> bool {
        self.status.changed().await.is_ok()
    }

    /// Waits until the first actor stops, then drops the rest.
    pub async fn wait(mut self) {
        self.tasks.join_next().await;
    }

    /// Stops every actor now.
    pub fn shutdown(mut self) {
        self.tasks.abort_all();
    }
}

/// Publishes a [`DownloadStatus`] every [`STATUS_INTERVAL`].
///
/// Rates are the reason this exists: they cannot be read from a counter at one
/// instant, only measured between two of them.
async fn sample_status(
    stats: Arc<DownloadStats>,
    mut progress: watch::Receiver<PieceProgress>,
    status: watch::Sender<DownloadStatus>,
) {
    let mut ticker = time::interval(STATUS_INTERVAL);
    let mut previous_downloaded = 0;
    let mut previous_uploaded = 0;
    let seconds = STATUS_INTERVAL.as_secs().max(1);

    loop {
        ticker.tick().await;

        let downloaded_bytes = stats.downloaded_bytes();
        let uploaded_bytes = stats.uploaded_bytes();
        let pieces = progress.borrow_and_update().clone();

        let state = if stats.is_complete() {
            DownloadState::Seeding
        } else if pieces.completed_pieces == 0 {
            DownloadState::Starting
        } else {
            DownloadState::Downloading
        };

        status.send_replace(DownloadStatus {
            state,
            pieces,
            downloaded_bytes,
            uploaded_bytes,
            wasted_bytes: stats.wasted_bytes(),
            hash_failures: stats.hash_failures(),
            in_flight_pieces: stats.in_flight_pieces(),
            active_peers: stats.active_peers(),
            download_rate: downloaded_bytes.saturating_sub(previous_downloaded) / seconds,
            upload_rate: uploaded_bytes.saturating_sub(previous_uploaded) / seconds,
        });

        previous_downloaded = downloaded_bytes;
        previous_uploaded = uploaded_bytes;
    }
}
