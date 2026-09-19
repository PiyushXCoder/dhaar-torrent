use dhaar_torrent::{Download, config::get_configuration};
use tracing::{error, info};

/// Installs the log formatter, and the tokio-console layer alongside it when
/// the `console` feature is on.
///
/// Layered rather than swapped: the console wants the runtime's task spans and
/// the terminal wants the client's own events, and replacing one subscriber
/// with the other would mean giving up the logs exactly when they are most
/// worth having. `RUST_LOG` still governs the terminal half only, so turning
/// the console on does not quieten anything.
fn init_tracing() {
    #[cfg(feature = "console")]
    {
        use tracing_subscriber::{
            layer::{Layer, SubscriberExt},
            util::SubscriberInitExt,
        };

        tracing_subscriber::registry()
            .with(console_subscriber::spawn())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(tracing_subscriber::EnvFilter::from_default_env()),
            )
            .init();
    }
    #[cfg(not(feature = "console"))]
    tracing_subscriber::fmt::init();
}

#[tokio::main]
async fn main() {
    init_tracing();

    let config = match get_configuration() {
        Ok(config) => config,
        Err(e) => {
            error!("{e:#}");
            return;
        }
    };

    let download =
        match Download::from_torrent_file_with_port(&config.torrent_file, config.listening_port) {
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
                "{:?} {:.1}% | {}/{} pieces | {} peers | down {} KiB/s up {} KiB/s | wasted {} KiB",
                status.state,
                status.progress() * 100.0,
                status.pieces.completed_pieces,
                status.pieces.total_pieces,
                status.active_peers,
                status.download_rate / 1024,
                status.upload_rate / 1024,
                status.wasted_bytes / 1024,
            );
        }
    });

    download.start().await;
}
