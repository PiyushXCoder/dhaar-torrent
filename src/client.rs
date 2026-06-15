use crate::{
    bencode,
    torrent_file::{info_hash, url_safe_string_info_hash},
    tracker,
};
use std::path::PathBuf;
use tokio::fs;

pub struct Client {}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Client {
        Client {}
    }

    pub async fn download_from_file(&self, file: PathBuf) {
        let torrent_file_data = fs::read(file).await.unwrap();
        let parse =
            bencode::from_bytes::<crate::torrent_file::TorrentFile>(&torrent_file_data).unwrap();
        println!("Creation Date: {:?}", parse.creation_date);
        println!("File Name: {}", parse.info.name);
        println!(
            "Info Hash: {}",
            url_safe_string_info_hash(&torrent_file_data)
        );
        let tk = tracker::Tracker::new(parse.announce);
        let peer_id_bytes: [u8; 20] = *b"-DH0001-a7K9mP2xQ8rZ";
        println!("Peer ID: {}", std::str::from_utf8(&peer_id_bytes).unwrap());
        println!("Length: {}", parse.info.length.unwrap_or(0));
        let prm = tracker::TrackerAnnounceParams {
            info_hash: info_hash(&torrent_file_data),
            peer_id: peer_id_bytes,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: parse.info.length.unwrap_or(0),
            compact: false,
            no_peer_id: false,
            event: None,
            ip: None,
            num_want: None,
            key: None,
            tracker_id: None,
        };
        let res = tk.announce(&prm).await.unwrap();
        println!("{:#?}", res);
    }
}
