use crate::config::get_configuration;

pub mod bencode;
pub mod client;
mod config;
pub mod helpers;
pub mod models;

#[tokio::main]
async fn main() {
    let config = get_configuration();
    let client = client::Client::new();
    client.download_from_file(config.torrent_file).await;
}
