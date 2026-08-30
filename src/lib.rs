pub mod config;
pub mod error;
pub mod helpers;
pub mod peer_connection;
pub mod peer_explorer;
pub mod peer_manager;
pub mod piece_manager;
pub mod torrent_parser;
pub mod wire_protocol;

use std::path::Path;

use tokio::task::JoinSet;

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
    torrent_parser::{TorrentParser, metadata::Torrent, parser::TorrentFileParser},
};

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
        let tracker_manager =
            TrackerManager::new(torrent.announce_urls(), &torrent.info_hash, &peer_id);
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
    pub fn new(
        torrent: Torrent,
        peer_id: [u8; 20],
        piece_writer: W,
        peer_selection_strategy: S,
        peer_sources: Vec<Box<dyn PeerSource + Send>>,
    ) -> Self {
        Self {
            torrent,
            peer_id,
            piece_writer,
            peer_selection_strategy,
            peer_sources,
        }
    }

    pub fn torrent(&self) -> &Torrent {
        &self.torrent
    }

    pub fn peer_id(&self) -> &[u8; 20] {
        &self.peer_id
    }

    /// Wires the actors together and runs them until the first one stops.
    ///
    /// Any one of them stopping means the download cannot continue — the
    /// others are only useful in each other's company — so the remaining tasks
    /// are dropped rather than left running.
    pub async fn start(self) {
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
        );
        let peer_manager = PeerManager::new(
            self.peer_selection_strategy,
            &self.torrent.info_hash,
            &self.peer_id,
        );

        let mut join_set: JoinSet<()> = JoinSet::new();
        join_set.spawn(peer_explorer.start(peer_explorer_channel_sender));
        join_set.spawn(piece_manager.start(piece_manager_channel_receiver));
        join_set.spawn(
            peer_manager.start(peer_explorer_channel_receiver, piece_manager_channel_sender),
        );
        join_set.join_next().await;
    }
}
