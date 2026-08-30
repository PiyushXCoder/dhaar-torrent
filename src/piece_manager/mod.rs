use serde_bytes::ByteBuf;
use sha1::Digest;
use tracing::{info, warn};

pub mod channel;
pub mod piece_writer;

use crate::{peer_explorer::Peer, wire_protocol::Bitfield};
use channel::PieceManagerMessage;

pub const BLOCK_SIZE: u64 = 16 * 1024;

pub struct PieceManager<E, W>
where
    E: std::error::Error + Send + Sync + 'static,
    W: piece_writer::PieceWriter<Error = E> + Send + Sync + 'static,
{
    pub piece_length: u64,
    pub total_length: u64,
    pub pieces: Vec<Piece>,
    // TODO: expose data from piece writer
    pub piece_writer: W,
}

pub struct Piece {
    block_length: Option<u64>,
    pub blocks: Option<Vec<Block>>,
    hash: [u8; 20],
    pub complete: bool,
}

pub struct Block {
    pub complete: bool,
    pub requesters: Vec<Peer>,
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
                    requesters: Vec::new(),
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
    pub fn new(
        piece_hashes: &ByteBuf,
        piece_length: u64,
        total_length: u64,
        piece_writer: W,
    ) -> Self {
        let piceces = piece_hashes
            .chunks(20)
            .map(|hash| Piece {
                hash: hash.to_vec().try_into().unwrap(),
                block_length: None,
                blocks: None,
                complete: false,
            })
            .collect();

        Self {
            piece_length,
            total_length,
            pieces: piceces,
            piece_writer,
        }
    }

    pub async fn start(
        &mut self,
        mut piece_manager_channel_receiver: channel::PieceManagerChannelReceiver,
    ) {
        self.piece_writer.initialize().await.unwrap();
        info!(
            "Piece manager started: {} pieces, {} bytes/piece",
            self.total_pieces(),
            self.piece_length
        );
        while let Some(msg) = piece_manager_channel_receiver.recv().await {
            // TODO: error handling
            match msg {
                PieceManagerMessage::HasPiece {
                    piece_index,
                    response_sender,
                } => {
                    response_sender.send(self.has_piece(piece_index)).unwrap();
                }
                PieceManagerMessage::GetIncompleteBlocks {
                    piece_index,
                    response_sender,
                } => {
                    response_sender
                        .send(self.get_incomplete_blocks(piece_index))
                        .unwrap();
                }
                PieceManagerMessage::GetBitfield { response_sender } => {
                    response_sender.send(self.bitfield()).unwrap();
                }
                PieceManagerMessage::IsInteresting {
                    bitfield,
                    response_sender,
                } => {
                    response_sender
                        .send(self.is_interesting(&bitfield))
                        .unwrap();
                }
                PieceManagerMessage::GetIncompletePieces {
                    bitfield,
                    response_sender,
                } => {
                    response_sender
                        .send(self.get_incomplete_pieces(&bitfield))
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
                PieceManagerMessage::TotalPieces { response_sender } => {
                    response_sender.send(self.total_pieces()).unwrap();
                }
                PieceManagerMessage::PieceLength {
                    piece_index,
                    response_sender,
                } => {
                    response_sender.send(self.piece_size(piece_index)).unwrap();
                }
            }
        }
    }

    /// Bytes in `piece_index`. Every piece is `piece_length` except the last,
    /// which is whatever is left over — treating it as full length makes its
    /// hash, its block count and its block requests all wrong.
    fn piece_size(&self, piece_index: u32) -> u64 {
        let offset = piece_index as u64 * self.piece_length;
        self.piece_length
            .min(self.total_length.saturating_sub(offset))
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

    fn is_interesting(&self, bitfield: &Bitfield) -> bool {
        self.pieces
            .iter()
            .enumerate()
            .any(|(index, piece)| !piece.complete && bitfield.has_piece(index as u32))
    }

    /// Every piece we still need that `bitfield` can serve. Locked pieces are
    /// included: `lock_piece` is the arbiter, so callers race there instead of
    /// acting on a list that may already be stale.
    fn get_incomplete_pieces(&self, bitfield: &Bitfield) -> Vec<u32> {
        self.pieces
            .iter()
            .enumerate()
            .filter(|(index, piece)| !piece.complete && bitfield.has_piece(*index as u32))
            .map(|(index, _)| index as u32)
            .collect()
    }

    async fn receive_block(&mut self, piece_index: u32, block_index: u32, block_data: Vec<u8>) {
        let piece_length = self.piece_size(piece_index);
        let Some(piece) = self.pieces.get_mut(piece_index as usize) else {
            return;
        };
        piece.ensure_initialized(piece_length);
        let Some(block_length) = piece.block_length else {
            return;
        };
        let offset = block_index as u64 * block_length;
        self.piece_writer
            .write(piece_index, offset, self.piece_length, block_data)
            .await
            .unwrap(); // TODO: handle errors
        if let Some(blocks) = piece.blocks.as_mut()
            && let Some(block) = blocks.get_mut(block_index as usize)
        {
            block.complete = true;
        }
        if piece
            .blocks
            .as_ref()
            .map_or(true, |blocks| blocks.iter().all(|block| block.complete))
        {
            let data = self
                .piece_writer
                .read(piece_index, 0, self.piece_length, piece_length)
                .await
                .unwrap();
            let hash: [u8; 20] = sha1::Sha1::digest(&data).into();
            if hash != piece.hash {
                warn!("{}: piece failed its hash check", piece_index);
                piece.blocks = None;
                piece.complete = false;
                return;
            }
            piece.complete = true;
        }
    }

    async fn read_block(&self, piece_index: u32, block_index: u32) -> Vec<u8> {
        let piece_size = self.piece_size(piece_index);
        let Ok(data) = self
            .piece_writer
            .read(piece_index, 0, self.piece_length, piece_size)
            .await
        else {
            return Vec::new();
        };
        let offset = (block_index as u64 * BLOCK_SIZE) as usize;
        if offset >= data.len() {
            return Vec::new();
        }
        let end = (offset + BLOCK_SIZE as usize).min(data.len());
        data[offset..end].to_vec()
    }

    fn completed_pieces(&self) -> u32 {
        self.pieces.iter().filter(|piece| piece.complete).count() as u32
    }

    fn total_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }

    fn get_incomplete_blocks(&self, piece_index: u32) -> Vec<channel::Block> {
        let Some(piece) = self.pieces.get(piece_index as usize) else {
            return Vec::new();
        };
        let Some(blocks) = piece.blocks.as_ref() else {
            return Vec::new();
        };
        blocks
            .iter()
            .enumerate()
            .filter(|(_, block)| !block.complete)
            .map(|(index, block)| channel::Block {
                index: index as u32,
                requester_len: block.requesters.len() as u64,
            })
            .collect()
    }
}
