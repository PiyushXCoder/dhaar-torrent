use crate::{bencode, torrent_file::hex_string_info_hash};
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
        println!("Info Hash: {}", hex_string_info_hash(&torrent_file_data));
    }
}
