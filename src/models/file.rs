use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct TorrentFile {
    pub info: Info,
    pub announce: String,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub creation_date: Option<u64>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub encoding: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    #[serde(rename = "piece length")]
    pub piece_length: u64,
    pub pieces: ByteBuf,
    pub private: Option<u8>,
    pub name: String,
    pub length: u64,
    pub md5sum: Option<String>,
    pub files: Option<Vec<File>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct File {
    pub length: u64,
    pub md5sum: Option<String>,
    pub path: Vec<String>,
}
