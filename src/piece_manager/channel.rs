use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    oneshot::Sender as OneShotSender,
};

use crate::{peer_explorer::Peer, wire_protocol::Bitfield};

const CHANNEL_SIZE: usize = 256;

pub enum PieceManagerMessage {
    HasPiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    GetBitfield {
        response_sender: OneShotSender<Bitfield>,
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
