use dhaar_torrent::{Download, config::get_configuration};
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

    let download = match Download::from_torrent_file(&config.torrent_file) {
        Ok(download) => download,
        Err(e) => {
            error!("{e:#}");
            return;
        }
    };

    download.start().await;
}
