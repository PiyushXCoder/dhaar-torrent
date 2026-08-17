use super::metadata::Torrent;
use crate::error::Result;
use std::path::Path;

pub trait TorrentParser {
    fn parse_from_bytes(data: &[u8]) -> Result<Torrent>;
    fn parse_from_file_path(path: &Path) -> Result<Torrent>;
}

pub struct TorrentFileParser;

impl TorrentParser for TorrentFileParser {
    fn parse_from_bytes(data: &[u8]) -> Result<Torrent> {
        Torrent::parse_from_bytes(data)
    }

    fn parse_from_file_path(path: &Path) -> Result<Torrent> {
        Torrent::parse_from_file_path(path)
    }
}
