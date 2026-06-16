use rand::{RngExt, rng};

pub fn generate_random_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];

    // Prefix: "dhar-"
    peer_id[..5].copy_from_slice(b"dhar-");

    // Fill remaining 15 bytes with random bytes
    rng().fill(&mut peer_id[5..]);

    peer_id
}
