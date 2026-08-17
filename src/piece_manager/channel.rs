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
    AmInterested {
        bitfield: Bitfield,
        response_sender: OneShotSender<bool>,
    },
    LockNextPiece {
        bitfield: Bitfield,
        response_sender: OneShotSender<Option<u32>>,
    },

    LockNextBlock {
        piece_index: u32,
        response_sender: OneShotSender<Option<u32>>,
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
    VerifyPiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
    },
    CompletePiece {
        piece_index: u32,
        response_sender: OneShotSender<bool>,
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
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
