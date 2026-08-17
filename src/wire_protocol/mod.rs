pub mod codec;

pub use codec::WireCodec;

#[derive(Debug)]
pub struct Handshake {
    pub pstrlen: u8,
    pub pstr: String,
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

#[derive(Debug)]
pub enum Message {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Bitfield),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
}

#[derive(Debug)]
pub enum WireItem {
    /// Yields just the `info_hash` as soon as it is available.
    /// This allows the seeder to respond with its own handshake early.
    HandshakePartial { info_hash: [u8; 20] },
    /// The full handshake – emitted once all 68 (or variable) bytes are received.
    Handshake(Handshake),
    /// A normal wire message.
    Message(Message),
}

#[derive(Debug, Clone)]
pub struct Bitfield(pub Vec<u8>);

impl Bitfield {
    pub fn has_piece(&self, index: u32) -> bool {
        match self.0.get((index / 8) as usize) {
            Some(byte) => (byte >> (7 - (index % 8))) & 1 == 1,
            None => false,
        }
    }

    pub fn set_piece(&mut self, index: u32, has: bool) {
        let byte_index = (index / 8) as usize;
        let bit_index = 7 - (index % 8);
        if has {
            self.0[byte_index] |= 1 << bit_index;
        } else {
            self.0[byte_index] &= !(1 << bit_index);
        }
    }
}
