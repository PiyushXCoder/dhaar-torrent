use dhaar_torrent::{
    config::get_configuration,
    helpers::generate_random_peer_id,
    peer_explorer::{PeerExplorer, channel::new_peer_explorer_channel, tracker::TrackerManager},
    peer_manager::{PeerManager, peer_selection_strategy::RetryAfterDelayPeerSelectionStrategy},
    piece_manager::{
        PieceManager, channel::new_piece_manager_channel, piece_writer::DiskPieceWriter,
    },
    torrent_parser::TorrentParser,
};
use tokio::task::JoinSet;
use tracing::error;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = match get_configuration() {
        Ok(config) => config,
        Err(e) => {
            error!("{e:#}");
            return;
        }
    };

    let torrent = dhaar_torrent::torrent_parser::TorrentFileParser::parse_from_file_path(
        &config.torrent_file,
    )
    .unwrap();
    let peer_id = generate_random_peer_id();

    let (peer_explorer_channel_sender, peer_explorer_channel_receiver) =
        new_peer_explorer_channel();
    let (piece_manager_channel_sender, piece_manager_channel_receiver) =
        new_piece_manager_channel();

    let tracker_manager =
        TrackerManager::new(torrent.announce_urls(), &torrent.info_hash, &peer_id);
    let peer_explorer = PeerExplorer::new(vec![Box::new(tracker_manager)]);
    let total_length = torrent.info.total_length();
    let piece_writer = DiskPieceWriter::new(
        total_length,
        &torrent.info.name,
        torrent.info.length,
        &torrent.info.md5sum,
        &torrent.info.files,
    );
    let piece_manager = PieceManager::new(
        &torrent.info.pieces,
        torrent.info.piece_length,
        total_length,
        piece_writer,
    );
    let retry_after_delay_peer_selection_strategy = RetryAfterDelayPeerSelectionStrategy::new();
    let peer_manager = PeerManager::new(
        retry_after_delay_peer_selection_strategy,
        &torrent.info_hash,
        &peer_id,
    );

    let mut join_set: JoinSet<()> = JoinSet::new();
    join_set.spawn(peer_explorer.start(peer_explorer_channel_sender));
    join_set.spawn(piece_manager.start(piece_manager_channel_receiver));
    join_set
        .spawn(peer_manager.start(peer_explorer_channel_receiver, piece_manager_channel_sender));
    join_set.join_next().await;

    println!("{:?}", torrent);
}
