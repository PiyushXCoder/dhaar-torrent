use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    oneshot::Sender as OneShotSender,
};

use crate::wire_protocol::Bitfield;

const CHANNEL_SIZE: usize = 256;

pub enum PieceManagerMessage {
    HasPiece {
        piece_index: u64,
        response_sender: OneShotSender<bool>,
    },
    Bitfield {
        response_sender: OneShotSender<Bitfield>,
    },
    LockNextPiece {
        bitfield: Bitfield,
        response_sender: OneShotSender<Option<u64>>,
    },

    LockNextBlock {
        piece_index: u64,
        response_sender: OneShotSender<Option<u64>>,
    },
    ReceiveBlock {
        piece_index: u64,
        block_index: u64,
        block_data: Vec<u8>,
    },
    ReadBlock {
        piece_index: u64,
        block_index: u64,
        response_sender: OneShotSender<Vec<u8>>,
    },
    VerifyPiece {
        piece_index: u64,
        response_sender: OneShotSender<bool>,
    },
    CompletePiece {
        piece_index: u64,
        response_sender: OneShotSender<bool>,
    },
    UnlockPiece {
        piece_index: u64,
    },
    CompletedPiece {
        response_sender: OneShotSender<u64>,
    },
    TotalPieces {
        response_sender: OneShotSender<u64>,
    },
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
