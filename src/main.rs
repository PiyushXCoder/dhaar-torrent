use dhaar_torrent::{config::get_configuration, torrent_parser::TorrentParser};
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

    println!("{:#?}", torrent);
}
