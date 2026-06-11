use sha1::Digest;

use crate::bencode;
use crate::models;

pub fn info_hash(torrent_file_data: &[u8]) -> [u8; 20] {
    let parse = bencode::from_bytes::<models::file::TorrentFileRawInfo>(torrent_file_data).unwrap();
    sha1::Sha1::digest(parse.info.bytes).into()
}

/// Lowercase hex, e.g. "2b6694...".
pub fn hex_string_info_hash(torrent_file_data: &[u8]) -> String {
    let hash = info_hash(torrent_file_data);
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

/// Percent-encoded form used in tracker announce URLs: unreserved bytes
/// (A-Z, a-z, 0-9, '-', '_', '.', '~') stay literal, everything else "%XX".
pub fn url_safe_string_info_hash(hash: &[u8; 20]) -> String {
    hash.iter()
        .map(|&b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_string_info_hash() {
        let mut hash = [0u8; 20];
        hash[0] = 0x2b;
        hash[19] = 0xff;
        assert_eq!(
            hex_string_info_hash(&hash),
            "2b000000000000000000000000000000000000ff"
        );
    }

    #[test]
    fn test_url_safe_string_info_hash() {
        let mut hash = [0u8; 20];
        hash[0] = b'a';
        hash[1] = 0x12;
        hash[2] = b'~';
        assert_eq!(
            url_safe_string_info_hash(&hash),
            "a%12~%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00"
        );
    }
}
