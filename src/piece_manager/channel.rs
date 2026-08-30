use tokio::sync::{
    broadcast,
    mpsc::{Receiver, Sender, channel},
    oneshot::Sender as OneShotSender,
};

use crate::{peer_explorer::Peer, status::PieceState, wire_protocol::Bitfield};

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
    /// Picks a piece and hands out blocks inside it in a single message.
    /// Choosing and registering have to happen in the same turn of the piece
    /// manager loop: split across two messages, a second peer can claim the
    /// same piece in the gap, and both download it.
    ///
    /// `piece_index` is the piece the caller already holds, if any — the
    /// manager tops that one up and only looks for a new piece once it is
    /// spent.
    ClaimBlocks {
        piece_index: Option<u32>,
        bitfield: Bitfield,
        peer: Peer,
        max_blocks: u32,
        response_sender: OneShotSender<ClaimReply>,
    },
    /// Gives a piece back without having finished it. Requests the peer will
    /// never answer — it choked us, timed out, or disconnected — must not go
    /// on holding the piece, or nobody can ever claim it again.
    Release {
        piece_index: u32,
        peer: Peer,
    },
    ReceiveBlock {
        piece_index: u32,
        block_index: u32,
        block_data: Vec<u8>,
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
    /// Every piece's standing, for a caller that draws them individually.
    /// Built on demand rather than published, because it is sized by the
    /// piece count and most callers only want the aggregate in `PieceProgress`.
    GetPieceStates {
        response_sender: OneShotSender<Vec<PieceState>>,
    },
}

/// Something finished. The piece manager is the only writer; connections
/// listen so they can react to work done by peers other than their own.
#[derive(Clone, Copy, Debug)]
pub enum PieceEvent {
    /// A block landed. Anyone else with it in flight is now downloading a
    /// copy nobody needs and should cancel it.
    BlockComplete { piece_index: u32, block_index: u32 },
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

/// Answer to `ClaimBlocks`.
///
/// `released` reports what the manager did, not what the caller should do:
/// taking a spent piece back has to happen in the same turn as choosing the
/// replacement, or the piece is briefly held by a peer that has moved on. The
/// caller mirrors it into its own state rather than inferring it, so neither
/// side can quietly strand a piece if the other changes.
#[derive(Debug)]
pub struct ClaimReply {
    /// Piece taken back from this peer. Its registrations are already gone.
    pub released: Option<u32>,
    /// Work to do. `None` means there was nothing left to hand out.
    pub granted: Option<Claim>,
}

/// What a peer was granted. `blocks` is empty when the piece still has
/// requests of ours in flight but nothing new to hand out — the piece is
/// still ours, there is just nothing to send this round.
#[derive(Debug)]
pub struct Claim {
    pub piece_index: u32,
    /// The piece's own length. The last piece of a torrent is short, and
    /// block bounds are measured against this rather than the nominal
    /// piece length.
    pub piece_length: u64,
    pub blocks: Vec<u32>,
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
