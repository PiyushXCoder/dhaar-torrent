use rand::{RngExt, rng};

pub fn generate_random_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];

    let version = env!("CARGO_PKG_VERSION")
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join("");
    let prefix = format!("-DH{}-", version);
    peer_id[..prefix.len()].copy_from_slice(prefix.as_bytes());

    // Fill remaining 15 bytes with random bytes
    rng().fill(&mut peer_id[prefix.len()..]);

    peer_id
}
