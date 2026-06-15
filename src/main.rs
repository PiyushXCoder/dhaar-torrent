use tracing::error;

use crate::config::get_configuration;

pub mod bencode;
pub mod client;
mod config;
pub mod error;
pub mod helpers;
pub mod models;
pub mod torrent_file;

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

    let client = client::Client::new();
    client.download_from_file(config.torrent_file).await;
}
