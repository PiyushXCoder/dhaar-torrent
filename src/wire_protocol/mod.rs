pub mod codec;

pub use codec::WireCodec;

pub struct Handshake {
    pub pstrlen: u8,
    pub pstr: String,
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

pub enum Message {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
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

pub enum WireItem {
    /// Yields just the `info_hash` as soon as it is available.
    /// This allows the seeder to respond with its own handshake early.
    HandshakePartial { info_hash: [u8; 20] },
    /// The full handshake – emitted once all 68 (or variable) bytes are received.
    Handshake(Handshake),
    /// A normal wire message.
    Message(Message),
}
