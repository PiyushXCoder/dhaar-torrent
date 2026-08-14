use dhaar_torrent::{
    config::get_configuration,
    helpers::generate_random_peer_id,
    torrent_parser::TorrentParser,
    tracker::{
        TcpTrackerClient, tracker_client::TrackerClient,
        tracker_client_messages::TrackerAnnounceQuery,
    },
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

    let tracker_client = TcpTrackerClient::new(&torrent.announce);
    let peer_id = generate_random_peer_id();
    let query = TrackerAnnounceQuery::new(&torrent.info_hash, &peer_id);
    let tracker_response = tracker_client.announce(&query).await.unwrap();
    println!("{:#?}", tracker_response);
}
