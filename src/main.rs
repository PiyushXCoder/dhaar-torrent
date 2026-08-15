use dhaar_torrent::{
    config::get_configuration,
    helpers::generate_random_peer_id,
    peer_explorer::{PeerExplorer, channel::new_peer_explorer_channel, tracker::TrackerManager},
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

    println!("{:#?}", torrent.info.name);

    let (peer_explorer_channel_sender, mut peer_explorer_channel_receiver) =
        new_peer_explorer_channel();

    let peer_id = generate_random_peer_id();
    let tracker_manager = TrackerManager::new(vec![torrent.announce], &torrent.info_hash, &peer_id);
    let mut peer_explorer = PeerExplorer::new(
        peer_explorer_channel_sender,
        vec![Box::new(tracker_manager)],
    );
    peer_explorer.start().await;

    while let Some(message) = peer_explorer_channel_receiver.recv().await {
        println!("{:?}", message);
    }
}
