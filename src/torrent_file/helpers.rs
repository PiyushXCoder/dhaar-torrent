use sha1::Digest;

use crate::bencode;
use crate::helpers::hash_helpers::{hex_string_hash, url_safe_string_hash};
use crate::torrent_file::TorrentFileRawInfo;

pub fn info_hash(torrent_file_data: &[u8]) -> [u8; 20] {
    let parse = bencode::from_bytes::<TorrentFileRawInfo>(torrent_file_data).unwrap();
    sha1::Sha1::digest(parse.info.bytes).into()
}

pub fn hex_string_info_hash(torrent_file_data: &[u8]) -> String {
    hex_string_hash(&info_hash(torrent_file_data))
}

pub fn url_safe_string_info_hash(torrent_file_data: &[u8]) -> String {
    url_safe_string_hash(&info_hash(torrent_file_data))
}
