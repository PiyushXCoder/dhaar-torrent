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
    GetBitfield {
        response_sender: OneShotSender<Bitfield>,
    },
    GetIncompletePieces {
        bitfield: Bitfield,
        response_sender: OneShotSender<Vec<u32>>,
    },
    GetIncompleteBlocks {
        piece_index: u32,
        response_sender: OneShotSender<Vec<Block>>,
    },
    IsInteresting {
        bitfield: Bitfield,
        response_sender: OneShotSender<bool>,
    },

    TotalPieces {
        response_sender: OneShotSender<u32>,
    },
    /// Length of one specific piece. Only the last piece differs from
    /// `PieceLength`, and getting it wrong strands that piece.
    PieceLength {
        piece_index: u32,
        response_sender: OneShotSender<u64>,
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

#[derive(Debug)]
pub struct Block {
    pub index: u32,
    pub requester_len: u64,
}

pub type PieceManagerChannelSender = Sender<PieceManagerMessage>;
pub type PieceManagerChannelReceiver = Receiver<PieceManagerMessage>;

pub fn new_piece_manager_channel() -> (PieceManagerChannelSender, PieceManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
