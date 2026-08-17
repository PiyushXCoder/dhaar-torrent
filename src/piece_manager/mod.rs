use serde_bytes::ByteBuf;
use sha1::Digest;
use tokio::task::JoinHandle;
use tracing::error;

pub mod channel;
pub mod piece_writer;

use crate::wire_protocol::Bitfield;
use channel::PieceManagerMessage;

const BLOCK_SIZE: u64 = 16 * 1024;

pub struct PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
{
    piece_length: u64,
    pieces: Vec<Piece>,
    piece_writer: W,
}

pub struct Piece {
    block_length: Option<u64>,
    blocks: Option<Vec<Block>>,
    hash: [u8; 20],
    complete: bool,
    locked: bool,
}

pub struct Block {
    locked: bool,
    complete: bool,
}

impl Piece {
    fn ensure_initialized(&mut self, piece_length: u64) {
        if self.blocks.is_some() {
            return;
        }
        let num_blocks = piece_length.div_ceil(BLOCK_SIZE) as usize;
        self.block_length = Some(BLOCK_SIZE);
        self.blocks = Some(
            (0..num_blocks)
                .map(|_| Block {
                    locked: false,
                    complete: false,
                })
                .collect(),
        );
    }
}

impl<E, W> PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
{
    pub fn new(piece_hashes: &ByteBuf, piece_length: u64, piece_writer: W) -> Self {
        let piceces = piece_hashes
            .chunks(20)
            .map(|hash| Piece {
                hash: hash.to_vec().try_into().unwrap(),
                block_length: None,
                blocks: None,
                complete: false,
                locked: false,
            })
            .collect();

        Self {
            piece_length,
            pieces: piceces,
            piece_writer,
        }
    }

    pub async fn start(
        mut self,
        mut piece_manager_channel_receiver: channel::PieceManagerChannelReceiver,
    ) -> JoinHandle<()> {
        self.piece_writer.initialize().await.unwrap();
        tokio::spawn(async move {
            while let Some(msg) = piece_manager_channel_receiver.recv().await {
                match msg {
                    PieceManagerMessage::HasPiece {
                        piece_index,
                        response_sender,
                    } => {
                        response_sender.send(self.has_piece(piece_index)).unwrap();
                    }
                    PieceManagerMessage::Bitfield { response_sender } => {
                        response_sender.send(self.bitfield()).unwrap();
                    }
                    PieceManagerMessage::AmInterested {
                        bitfield,
                        response_sender,
                    } => {
                        response_sender.send(self.am_interested(&bitfield)).unwrap();
                    }
                    PieceManagerMessage::LockNextPiece {
                        bitfield,
                        response_sender,
                    } => {
                        response_sender
                            .send(self.lock_next_piece(&bitfield))
                            .unwrap();
                    }
                    PieceManagerMessage::LockNextBlock {
                        piece_index,
                        response_sender,
                    } => {
                        response_sender
                            .send(self.lock_next_block(piece_index))
                            .unwrap();
                    }
                    PieceManagerMessage::ReceiveBlock {
                        piece_index,
                        block_index,
                        block_data,
                    } => {
                        self.receive_block(piece_index, block_index, block_data)
                            .await;
                    }
                    PieceManagerMessage::ReadBlock {
                        piece_index,
                        block_index,
                        response_sender,
                    } => {
                        response_sender
                            .send(self.read_block(piece_index, block_index).await)
                            .unwrap();
                    }
                    PieceManagerMessage::VerifyPiece {
                        piece_index,
                        response_sender,
                    } => {
                        response_sender
                            .send(self.verify_piece(piece_index).await)
                            .unwrap();
                    }
                    PieceManagerMessage::CompletePiece {
                        piece_index,
                        response_sender,
                    } => {
                        response_sender
                            .send(self.complete_piece(piece_index).await)
                            .unwrap();
                    }
                    PieceManagerMessage::UnlockPiece { piece_index } => {
                        self.unlock_piece(piece_index);
                    }
                    PieceManagerMessage::CompletedPiece { response_sender } => {
                        response_sender.send(self.completed_pieces()).unwrap();
                    }
                    PieceManagerMessage::TotalPieces { response_sender } => {
                        response_sender.send(self.total_pieces()).unwrap();
                    }
                }
            }
        })
    }

    fn has_piece(&self, piece_index: u32) -> bool {
        match self.pieces.get(piece_index as usize) {
            Some(piece) => piece.complete,
            None => true,
        }
    }

    fn bitfield(&self) -> Bitfield {
        let mut bytes = vec![0u8; self.pieces.len().div_ceil(8)];
        for (index, piece) in self.pieces.iter().enumerate() {
            if piece.complete {
                bytes[index / 8] |= 1 << (7 - (index % 8));
            }
        }
        Bitfield(bytes)
    }

    fn am_interested(&self, bitfield: &Bitfield) -> bool {
        self.pieces
            .iter()
            .enumerate()
            .any(|(index, piece)| !piece.complete && bitfield.has_piece(index as u32))
    }

    fn lock_next_piece(&mut self, bitfield: &Bitfield) -> Option<u32> {
        let piece_length = self.piece_length;
        let (index, piece) = self.pieces.iter_mut().enumerate().find(|(index, piece)| {
            !piece.complete && !piece.locked && bitfield.has_piece(*index as u32)
        })?;
        piece.locked = true;
        piece.ensure_initialized(piece_length);
        Some(index as u32)
    }

    fn lock_next_block(&mut self, piece_index: u32) -> Option<u32> {
        let piece_length = self.piece_length;
        let piece = self.pieces.get_mut(piece_index as usize)?;
        piece.ensure_initialized(piece_length);
        let blocks = piece.blocks.as_mut()?;
        let (index, block) = blocks
            .iter_mut()
            .enumerate()
            .find(|(_, block)| !block.locked && !block.complete)?;
        block.locked = true;
        Some(index as u32)
    }

    async fn receive_block(&mut self, piece_index: u32, block_index: u32, block_data: Vec<u8>) {
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return;
        };
        let Some(block_length) = piece.block_length else {
            return;
        };
        let offset = block_index as u64 * block_length;
        self.piece_writer
            .write(piece_index, offset, block_data)
            .await
            .unwrap();
        if let Some(blocks) = piece.blocks.as_mut() {
            if let Some(block) = blocks.get_mut(block_index as usize) {
                block.complete = true;
            }
        }
    }

    async fn read_block(&self, piece_index: u32, block_index: u32) -> Vec<u8> {
        let Ok(data) = self.piece_writer.read(piece_index, 0).await else {
            return Vec::new();
        };
        let offset = (block_index as u64 * BLOCK_SIZE) as usize;
        if offset >= data.len() {
            return Vec::new();
        }
        let end = (offset + BLOCK_SIZE as usize).min(data.len());
        data[offset..end].to_vec()
    }

    async fn verify_piece(&self, piece_index: u32) -> bool {
        let Some(piece) = self.pieces.get(piece_index as usize) else {
            return false;
        };
        let Ok(data) = self.piece_writer.read(piece_index, 0).await else {
            return false;
        };
        let digest: [u8; 20] = sha1::Sha1::digest(&data).into();
        digest == piece.hash
    }

    async fn complete_piece(&mut self, piece_index: u32) -> bool {
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return false;
        };
        piece.complete = true;
        piece.blocks = None;
        piece.block_length = None;

        if self.completed_pieces() == self.total_pieces() {
            if let Err(e) = self.piece_writer.finalize().await {
                error!("Failed to finalize download: {}", e);
            }
        }

        true
    }

    fn unlock_piece(&mut self, piece_index: u32) {
        if let Some(piece) = self.pieces.get_mut(piece_index as usize) {
            piece.locked = false;
            if let Some(blocks) = piece.blocks.as_mut() {
                for block in blocks.iter_mut() {
                    block.locked = false;
                }
            }
        }
    }

    fn completed_pieces(&self) -> u32 {
        self.pieces.iter().filter(|piece| piece.complete).count() as u32
    }

    fn total_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }
}
