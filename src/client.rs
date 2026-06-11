use crate::bencode;
use std::path::PathBuf;
use tokio::fs;

use crate::models;

pub struct Client {}

impl Client {
    pub fn new() -> Client {
        Client {}
    }

    pub async fn download_from_file(&self, file: PathBuf) {
        let torrent_file_data = fs::read(file).await.unwrap();
        let parse = bencode::from_bytes::<models::file::TorrentFile>(&torrent_file_data).unwrap();
        // println!("{:?}", parse);
    }
}
