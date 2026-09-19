use tokio::sync::{
    broadcast,
    mpsc::{Receiver, Sender, channel},
    oneshot::Sender as OneShotSender,
};

use crate::{peer_explorer::Peer, wire_protocol::Bitfield};

const CHANNEL_SIZE: usize = 256;
/// Overflowing costs a duplicate piece or an unannounced one, never
/// correctness, so this is sized for comfort rather than guarantees.
const EVENT_CHANNEL_SIZE: usize = 512;

pub enum PieceManagerMessage {
    HasPiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    GetBitfield {
        response_sender: OneShotSender<BitfieldSnapshot>,
    },
    IsInteresting {
        bitfield: Bitfield,
        response_sender: OneShotSender<bool>,
    },
    /// Choosing and registering happen in one turn: split across two messages,
    /// a second peer can claim the same piece in the gap and both download it.
    ClaimPiece {
        bitfield: Bitfield,
        peer: Peer,
        response_sender: OneShotSender<Option<Claim>>,
    },
    /// Requests the peer will never answer must not go on holding the piece,
    /// or nobody can ever claim it again.
    Release {
        piece_index: u32,
        peer: Peer,
    },
    /// Must arrive *after* the write has landed: the bitfield is what makes a
    /// piece servable. Idempotent, because endgame lets two connections finish
    /// the same piece.
    PieceVerified {
        piece_index: u32,
        peer: Peer,
    },
    PieceFailed {
        piece_index: u32,
        peer: Peer,
    },
    TotalPieces {
        response_sender: OneShotSender<u32>,
    },
    IsCompleted {
        response_sender: OneShotSender<bool>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum PieceEvent {
    PieceComplete { piece_index: u32 },
}

/// A connection's opening view of what we hold.
///
/// Both halves are taken in one turn of the loop because they only mean
/// anything together: fetched separately, a piece finishing in the gap lands
/// in neither the bitfield already sent nor the feed not yet joined, and the
/// protocol has no second bitfield to correct it with.
#[derive(Debug)]
pub struct BitfieldSnapshot {
    pub bitfield: Bitfield,
    pub events: PieceEventReceiver,
}

pub type PieceEventSender = broadcast::Sender<PieceEvent>;
pub type PieceEventReceiver = broadcast::Receiver<PieceEvent>;

pub fn new_piece_event_channel() -> PieceEventSender {
    broadcast::channel(EVENT_CHANNEL_SIZE).0
}

/// A piece a peer owns, and everything it needs to fetch and verify it without
/// asking again.
#[derive(Debug)]
pub struct Claim {
    pub piece_index: u32,
    pub hash: [u8; 20],
    /// The piece's own length: the last piece of a torrent is short, and block
    /// bounds are measured against this rather than the nominal length.
    pub piece_length: u64,
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
