use dhaar_torrent::{Download, config::get_configuration};
use tracing::{error, info};

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

    // Subscribed before the download starts, so nothing is missed between
    // spawning the actors and the first sample.
    let mut status = download.subscribe();
    tokio::spawn(async move {
        while status.changed().await.is_ok() {
            let status = status.borrow_and_update();
            info!(
                "{:?} {:.1}% | {}/{} pieces | {} peers | down {} KiB/s up {} KiB/s",
                status.state,
                status.progress() * 100.0,
                status.pieces.completed_pieces,
                status.pieces.total_pieces,
                status.active_peers,
                status.download_rate / 1024,
                status.upload_rate / 1024,
            );
        }
    });

    download.start().await;
}
