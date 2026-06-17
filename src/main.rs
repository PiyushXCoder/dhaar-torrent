use tracing::{error, info};

use crate::{
    config::get_configuration,
    helpers::generate_random_peer_id,
    torrent::{Torrent, TorrentEvent},
    torrent_file::{TorrentFile, info_hash},
};
use std::fs;

pub mod bencode;
pub mod client;
mod config;
pub mod error;
pub mod helpers;
pub mod models;
pub mod peer;
pub mod piece_bag;
pub mod torrent;
pub mod torrent_file;
pub mod tracker;

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

    let mut client = client::Client::new();

    let torrent_file_data = fs::read(&config.torrent_file).unwrap();
    let torrent_file = bencode::from_bytes::<TorrentFile>(&torrent_file_data).unwrap();
    info!("torrent file: {:?}", torrent_file.announce);
    let info_hash = info_hash(&torrent_file_data);
    let peer_id = generate_random_peer_id();
    let torrent = Torrent::new(torrent_file, info_hash, peer_id);
    let mut handle = match client.add_torrent(torrent).await {
        Ok(h) => h,
        Err(e) => {
            error!("{e:#}");
            return;
        }
    };

    while let Some(event) = handle.events.recv().await {
        match event {
            TorrentEvent::PeersFound(n) => println!("Peers found: {n}"),
            TorrentEvent::TrackerWarning(msg) => eprintln!("Tracker warning: {msg}"),
            TorrentEvent::TrackerFailure(msg) => eprintln!("Tracker failure: {msg}"),
            TorrentEvent::TrackerError(msg) => eprintln!("Tracker error: {msg}"),
        }
    }
}
