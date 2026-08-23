use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    oneshot::Sender as OneShotSender,
};

use crate::wire_protocol::Bitfield;

const CHANNEL_SIZE: usize = 256;

pub enum PieceManagerMessage {
    HasPiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    Bitfield {
        response_sender: OneShotSender<Bitfield>,
    },
    GetAllInterestedPieces {
        bitfield: Bitfield,
        response_sender: OneShotSender<Vec<u32>>,
    },
    GetNextInterestedPiece {
        bitfield: Bitfield,
        response_sender: OneShotSender<Option<u32>>,
    },
    AmInterested {
        bitfield: Bitfield,
        response_sender: OneShotSender<bool>,
    },

    LockPiece {
        piece_index: u32,
        response_sender: OneShotSender<Option<u32>>,
    },
    UnlockPiece {
        piece_index: u32,
    },
    CompletedPiece {
        response_sender: OneShotSender<u32>,
    },
    TotalPieces {
        response_sender: OneShotSender<u32>,
    },
    PieceLength {
        response_sender: OneShotSender<u64>,
    },
    VerifyPiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    CompletePiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    /// Forgets a piece's block progress so it can be fetched again from
    /// scratch, used when the assembled piece fails its hash check.
    ResetPiece {
        piece_index: u32,
    },

    GetIncompleteBlocks {
        piece_index: u32,
        response_sender: OneShotSender<Vec<u32>>,
    },

    LockBlock {
        piece_index: u32,
        block_index: u32,
        response_sender: OneShotSender<bool>,
    },
    UnlockBlock {
        piece_index: u32,
        block_index: u32,
    },
    ReceiveBlock {
        piece_index: u32,
        block_index: u32,
        block_data: Vec<u8>,
    },
    ReadBlock {
        piece_index: u32,
        block_index: u32,
        response_sender: OneShotSender<Vec<u8>>,
    },
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
