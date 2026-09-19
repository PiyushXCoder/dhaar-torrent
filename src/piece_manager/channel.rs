use tokio::sync::{
    broadcast,
    mpsc::{Receiver, Sender, channel},
    oneshot::Sender as OneShotSender,
};

use crate::{peer_explorer::Peer, wire_protocol::Bitfield};

const CHANNEL_SIZE: usize = 256;
/// Deep enough that a connection busy with a slow write does not miss events
/// it could still act on. Overflowing only costs a duplicate block or an
/// unannounced piece, never correctness.
const EVENT_CHANNEL_SIZE: usize = 512;

pub enum PieceManagerMessage {
    HasPiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    /// What we hold, plus the feed of everything that completes from that
    /// moment on. See `BitfieldSnapshot` for why they come together.
    GetBitfield {
        response_sender: OneShotSender<BitfieldSnapshot>,
    },
    IsInteresting {
        bitfield: Bitfield,
        response_sender: OneShotSender<bool>,
    },
    /// Takes a piece for this peer. Choosing and registering happen in the
    /// same turn of the loop: split across two messages, a second peer can
    /// claim the same piece in the gap, and both download it.
    ///
    /// A whole piece, not a set of blocks. The caller assembles it, so which
    /// blocks it asks for and in what order is its own business — the manager
    /// only needs to know who owns the piece.
    ClaimPiece {
        bitfield: Bitfield,
        peer: Peer,
        response_sender: OneShotSender<Option<Claim>>,
    },
    /// Gives a piece back without having finished it. Requests the peer will
    /// never answer — it choked us, timed out, or disconnected — must not go
    /// on holding the piece, or nobody can ever claim it again.
    Release {
        piece_index: u32,
        peer: Peer,
    },
    /// A connection assembled a piece, checked it against the torrent's hash
    /// and wrote it to the store. All that is left is the bookkeeping only the
    /// manager can do: the bitfield, the totals, and telling everyone else.
    ///
    /// Must arrive *after* the write has landed. The bitfield is what makes a
    /// piece servable, so a claim that overtook its own bytes would advertise
    /// a piece that is not on disk yet.
    ///
    /// Idempotent by necessity: in endgame two connections can finish the same
    /// piece, and the second one to report must change nothing.
    PieceVerified {
        piece_index: u32,
        peer: Peer,
    },
    /// A connection assembled a piece whose hash was wrong. Nothing was
    /// written; the piece has to be fetched again from the start.
    PieceFailed {
        piece_index: u32,
        peer: Peer,
    },
    ReadBlock {
        piece_index: u32,
        block_index: u32,
        response_sender: OneShotSender<Vec<u8>>,
    },
    TotalPieces {
        response_sender: OneShotSender<u32>,
    },
    IsCompleted {
        response_sender: OneShotSender<bool>,
    },
}

/// Something finished. The piece manager is the only writer; connections
/// listen so they can react to work done by peers other than their own.
#[derive(Clone, Copy, Debug)]
pub enum PieceEvent {
    /// A piece passed its hash check, so every peer can be told we have it.
    PieceComplete { piece_index: u32 },
}

/// A connection's opening view of what we have: the bitfield it announces to
/// its peer, and the feed of everything completed after that.
///
/// Both are taken in one turn of the piece manager loop, because they only
/// mean anything together. Fetched separately, a piece finishing in the gap
/// lands in neither the bitfield already sent nor the feed not yet joined,
/// and that peer never learns we hold it — there is no second bitfield in the
/// protocol to correct it with.
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

/// The piece a peer now owns, and everything it needs to fetch and verify it
/// without asking again.
#[derive(Debug)]
pub struct Claim {
    pub piece_index: u32,
    /// What this piece must hash to. Carried with the claim because the
    /// connection, not the manager, is what verifies it now.
    pub hash: [u8; 20],
    /// The piece's own length. The last piece of a torrent is short, and
    /// block bounds are measured against this rather than the nominal piece
    /// length, so the claim carries it rather than making the caller derive
    /// it from the torrent's geometry.
    pub piece_length: u64,
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
