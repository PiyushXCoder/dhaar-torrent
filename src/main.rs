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

    let tracker_manager = TrackerManager::new(vec![torrent.announce], &torrent.info_hash, &peer_id);
    let mut peer_explorer = PeerExplorer::new(vec![Box::new(tracker_manager)]);
    let join_handle1 = peer_explorer.start(peer_explorer_channel_sender).await;

    let piece_writer = DiskPieceWriter::new(
        torrent.info.piece_length,
        &torrent.info.name,
        torrent.info.length,
        &torrent.info.md5sum,
        &torrent.info.files,
    );
    let piece_manager = PieceManager::new(
        &torrent.info.pieces,
        torrent.info.piece_length,
        piece_writer,
    );
    let join_handle2 = piece_manager.start(piece_manager_channel_receiver).await;

    let retry_after_delay_peer_selection_strategy = RetryAfterDelayPeerSelectionStrategy::new();
    let peer_manager = PeerManager::new(
        retry_after_delay_peer_selection_strategy,
        &torrent.info_hash,
        &peer_id,
    );
    let join_handle3 = peer_manager
        .start(peer_explorer_channel_receiver, piece_manager_channel_sender)
        .await;

    join_handle1.await.unwrap();
    join_handle2.await.unwrap();
    join_handle3.await.unwrap();
}
