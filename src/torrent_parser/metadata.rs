use sha1::Digest;
use std::collections::HashSet;
use std::path::Path;

use bencode::{
    Raw,
    chrono::{deserialize as chrono_deserialize, serialize as chrono_serialize},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

pub(crate) fn info_hash(torrent_file_data: &[u8]) -> [u8; 20] {
    let parse = bencode::from_bytes::<TorrentFileRawInfo>(torrent_file_data).unwrap();
    sha1::Sha1::digest(parse.info.bytes).into()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Info {
    #[serde(rename = "piece length")]
    pub piece_length: u64,
    pub pieces: ByteBuf,
    pub private: Option<u8>,
    pub name: String,
    pub length: Option<u64>,
    pub md5sum: Option<String>,
    pub files: Option<Vec<File>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct File {
    pub length: u64,
    pub md5sum: Option<String>,
    pub path: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TorrentFileRawInfo {
    pub info: Raw<Info>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Torrent {
    pub info: Info,
    pub announce: String,
    #[serde(rename = "announce-list")]
    pub announce_list: Option<Vec<Vec<String>>>,
    #[serde(
        rename = "creation date",
        serialize_with = "chrono_serialize",
        deserialize_with = "chrono_deserialize"
    )]
    pub creation_date: Option<DateTime<Utc>>,
    pub comment: Option<String>,
    #[serde(rename = "created by")]
    pub created_by: Option<String>,
    pub encoding: Option<String>,
    #[serde(skip)]
    pub info_hash: [u8; 20],
}

impl Torrent {
    pub(crate) fn parse_from_bytes(torrent_data: &[u8]) -> crate::error::Result<Torrent> {
        let mut torrent = bencode::from_bytes::<Torrent>(torrent_data)?;
        torrent.info_hash = info_hash(torrent_data);
        Ok(torrent)
    }

    pub(crate) fn parse_from_file_path(file_path: &Path) -> crate::error::Result<Torrent> {
        let torrent_data = std::fs::read(file_path).unwrap();
        let mut torrent = bencode::from_bytes::<Torrent>(&torrent_data).unwrap();
        torrent.info_hash = info_hash(&torrent_data);
        Ok(torrent)
    }

    /// Every tracker URL for this torrent: the primary `announce` first, then the
    /// `announce-list` tiers in order (BEP 12 lists them by preference). Duplicates
    /// are dropped — tiers normally repeat the primary URL.
    pub fn announce_urls(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        std::iter::once(&self.announce)
            .chain(self.announce_list.iter().flatten().flatten())
            .filter(|url| seen.insert(*url))
            .cloned()
            .collect()
    }
}
