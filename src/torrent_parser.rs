use crate::error::Result;
use std::path::PathBuf;

use crate::torrent_meta::Torrent;
pub struct TorrentFileParser;

impl TorrentFileParser {
    pub fn parse_from_bytes(data: &[u8]) -> Result<Torrent> {
        Torrent::parse_from_bytes(data)
    }
    pub fn parse_from_file_path(path: &PathBuf) -> Result<Torrent> {
        Torrent::parse_from_file_path(path)
    }
}
