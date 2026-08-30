use super::channels::{IncomingChannelReceiver, OutgoingChannelSender};
use super::{close, peer_addr, piece_manager_request};
use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    peer_manager::channels::PeerManagerChannelSender,
    piece_manager::{
        BLOCK_SIZE,
        channel::{PieceManagerChannelSender, PieceManagerMessage},
    },
    wire_protocol::{Bitfield, Message, WireItem},
};

use tokio::{select, sync::oneshot, time};
use tracing::{debug, warn};

/// Any traffic at all resets this. Purely a liveness check.
const IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(60);
/// Only block data resets this, so a peer that chats but never delivers
/// stops holding our blocks hostage.
const REQUEST_TIMEOUT: time::Duration = time::Duration::from_secs(30);
/// Pieces we failed to lock can be freed by other peers at any time and no
/// event tells us about it, so we re-check on our own.
const AVAILABILITY_TICK: time::Duration = time::Duration::from_secs(5);
const MAX_REQUESTS: u32 = 8;

pub struct RequestManager {
    pub peer: Option<Peer>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub peer_bitfield: Bitfield,
    pub active_piece: Option<u32>,
    pub active_piece_length: u64,
    pub active_blocks: Vec<u32>,
    pub peer_manager_channel_sender: Option<PeerManagerChannelSender>,
    pub piece_manager_channel_sender: PieceManagerChannelSender,
    pub incoming_channel_receiver: IncomingChannelReceiver,
    pub outgoing_channel_sender: OutgoingChannelSender,
}

impl RequestManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        peer: Option<Peer>,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        peer_bitfield: Bitfield,
        peer_manager_channel_sender: Option<PeerManagerChannelSender>,
        piece_manager_channel_sender: PieceManagerChannelSender,
        incoming_channel_receiver: IncomingChannelReceiver,
        outgoing_channel_sender: OutgoingChannelSender,
    ) -> Self {
        Self {
            peer,
            info_hash,
            peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield,
            active_piece: None,
            active_piece_length: 0,
            active_blocks: Vec::new(),
            peer_manager_channel_sender,
            piece_manager_channel_sender,
            incoming_channel_receiver,
            outgoing_channel_sender,
        }
    }

    pub async fn start(mut self) {
        match self.run().await {
            Ok(()) | Err(PeerConnectionError::PeerDisconnected) => {
                debug!("{}: connection ended", peer_addr(&self.peer));
            }
            Err(e) => warn!("{}: connection ended: {}", peer_addr(&self.peer), e),
        }
        close(&mut self.peer_manager_channel_sender, &mut self.peer).await;
    }

    async fn run(&mut self) -> PeerConnectionResult<()> {
        let mut idle_deadline = time::Instant::now() + IDLE_TIMEOUT;
        let mut request_deadline: Option<time::Instant> = None;
        let mut availability_tick = time::interval(AVAILABILITY_TICK);

        self.update_interest().await?;

        loop {
            select! {
                _ = time::sleep_until(idle_deadline) => {
                    debug!("{}: silent for {:?}", peer_addr(&self.peer), IDLE_TIMEOUT);
                    return Err(PeerConnectionError::PeerDisconnected);
                },
                // Armed only while we are actually waiting on blocks.
                _ = async {
                    match request_deadline {
                        Some(deadline) => time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    warn!("{}: requests timed out", peer_addr(&self.peer));
                    request_deadline = None;
                },
                _ = availability_tick.tick() => {
                    self.availability_tick().await?;
                },
                item = self.incoming_channel_receiver.recv() => {
                    let Some(item) = item else {
                        return Err(PeerConnectionError::PeerDisconnected);
                    };

                    let carried_block =
                        matches!(item, WireItem::Message(Message::Piece { .. }));
                    self.handle_incoming_message(item).await?;

                    idle_deadline = time::Instant::now() + IDLE_TIMEOUT;
                    if self.active_blocks.is_empty() {
                        request_deadline = None;
                    } else if carried_block || request_deadline.is_none() {
                        request_deadline = Some(time::Instant::now() + REQUEST_TIMEOUT);
                    }
                },
            }
        }
    }

    async fn handle_incoming_message(&mut self, item: WireItem) -> PeerConnectionResult<()> {
        match item {
            WireItem::Message(Message::Choke) => {
                self.peer_choking = true;
                // The peer throws away every request it has not answered, so
                // holding those block locks would strand them.
            }
            WireItem::Message(Message::Unchoke) => {
                self.peer_choking = false;
                self.fill_pipeline().await?;
            }
            WireItem::Message(Message::Interested) => {
                self.peer_interested = true;
                // There is no upload policy yet, so whoever asks gets served.
                if self.am_choking {
                    self.am_choking = false;
                    self.send_message(Message::Unchoke).await?;
                }
            }
            WireItem::Message(Message::NotInterested) => {
                self.peer_interested = false;
            }
            WireItem::Message(Message::Have(index)) => {
                if !self.mark_peer_has(index) {
                    warn!(
                        "{}: have {} is outside the bitfield",
                        peer_addr(&self.peer),
                        index
                    );
                    return Ok(());
                }
                self.update_interest().await?;
                self.fill_pipeline().await?;
            }
            // The peer's bitfield is consumed before this loop starts, so a
            // second one carries no new information and only signals that the
            // peer is not following the framing rules.
            WireItem::Message(Message::Bitfield(_)) => {
                warn!("{}: sent a second bitfield", peer_addr(&self.peer));
                return Err(PeerConnectionError::UnexpectedMessage);
            }
            WireItem::Message(Message::Request {
                index,
                begin,
                length,
            }) => {
                self.serve_block(index, begin, length).await?;
            }
            WireItem::Message(Message::Piece {
                index,
                begin,
                block,
            }) => {
                self.receive_block(index, begin, block).await?;
            }
            // Requests are answered inline as they arrive, so by the time a
            // cancel lands there is no queued upload left to drop.
            WireItem::Message(Message::Cancel { .. }) => {}
            // DHT is not implemented, so the peer's DHT port is of no use.
            WireItem::Message(Message::Port(_port)) => {}
            _ => {}
        }
        Ok(())
    }

    /// Records a piece the peer announced. `false` means the index falls
    /// outside the bitfield length both sides agreed on.
    fn mark_peer_has(&mut self, index: u32) -> bool {
        if (index / 8) as usize >= self.peer_bitfield.0.len() {
            return false;
        }
        self.peer_bitfield.set_piece(index, true);
        true
    }

    /// Tells the peer whether it holds anything we still need, but only when
    /// the answer changed — this runs on every `Have`, so it is hot.
    async fn update_interest(&mut self) -> PeerConnectionResult<()> {
        let bitfield = self.peer_bitfield.clone();
        let interested = self
            .ask(|response_sender| PieceManagerMessage::IsInteresting {
                bitfield,
                response_sender,
            })
            .await?;
        if interested == self.am_interested {
            return Ok(());
        }
        self.am_interested = interested;
        self.send_message(if interested {
            Message::Interested
        } else {
            Message::NotInterested
        })
        .await
    }

    /// Keeps up to `MAX_REQUESTS` blocks in flight, all inside one piece.
    /// Working a single piece at a time means a dead connection strands at
    /// most one partial piece.
    async fn fill_pipeline(&mut self) -> PeerConnectionResult<()> {
        if self.peer_choking || !self.am_interested {
            return Ok(());
        }
        if self.active_piece.is_none() {
            self.acquire_piece().await?;
        }
        let Some(piece_index) = self.active_piece else {
            return Ok(());
        };

        let candidates = self
            .ask(|response_sender| PieceManagerMessage::GetIncompleteBlocks {
                piece_index,
                response_sender,
            })
            .await?;

        for block_index in candidates {
            if self.active_blocks.len() >= MAX_REQUESTS as usize {
                break;
            }
            if self.active_blocks.contains(&block_index) {
                continue;
            }
            let locked = self
                .ask(|response_sender| PieceManagerMessage::LockBlock {
                    piece_index,
                    block_index,
                    response_sender,
                })
                .await?;
            if !locked {
                continue;
            }
            let (begin, length) = self.block_bounds(block_index);
            self.send_message(Message::Request {
                index: piece_index,
                begin,
                length,
            })
            .await?;
            self.active_blocks.push(block_index);
        }
        Ok(())
    }

    /// Claims the first piece this peer can serve that nobody else holds.
    /// The interested list can be stale, so `LockPiece` is what decides.
    async fn acquire_piece(&mut self) -> PeerConnectionResult<()> {
        let bitfield = self.peer_bitfield.clone();
        let candidates = self
            .ask(|response_sender| PieceManagerMessage::GetIncompletePieces {
                bitfield,
                response_sender,
            })
            .await?;
        let Some(piece_index) = candidates.get(0).copied() else {
            return Err(PeerConnectionError::PeerDisconnected);
        };
        self.active_piece = Some(piece_index);
        self.active_piece_length = self
            .ask(|response_sender| PieceManagerMessage::PieceLength {
                piece_index,
                response_sender,
            })
            .await?;
        Ok(())
    }

    /// Byte range of one block inside the active piece. The final block of a
    /// piece is short, and every block count is measured against the active
    /// piece's own length — the last piece of a torrent is itself short.
    fn block_bounds(&self, block_index: u32) -> (u32, u32) {
        let begin = block_index as u64 * BLOCK_SIZE;
        let length = BLOCK_SIZE.min(self.active_piece_length.saturating_sub(begin));
        (begin as u32, length as u32)
    }

    /// Files a block the peer sent us. Data we never asked for is dropped:
    /// accepting it would clear a lock we do not hold.
    async fn receive_block(
        &mut self,
        index: u32,
        begin: u32,
        block: Vec<u8>,
    ) -> PeerConnectionResult<()> {
        let Some(piece_index) = self.active_piece else {
            debug!(
                "{}: block for piece {} while working on nothing",
                peer_addr(&self.peer),
                index
            );
            return Ok(());
        };
        if index != piece_index || !(begin as u64).is_multiple_of(BLOCK_SIZE) {
            debug!(
                "{}: unexpected block {}+{}, working on piece {}",
                peer_addr(&self.peer),
                index,
                begin,
                piece_index
            );
            return Ok(());
        }
        let block_index = (begin as u64 / BLOCK_SIZE) as u32;
        let Some(position) = self
            .active_blocks
            .iter()
            .position(|active| *active == block_index)
        else {
            debug!(
                "{}: block {} of piece {} was not requested",
                peer_addr(&self.peer),
                block_index,
                piece_index
            );
            return Ok(());
        };
        self.active_blocks.swap_remove(position);

        self.send_to_piece_manager(PieceManagerMessage::ReceiveBlock {
            piece_index,
            block_index,
            block_data: block,
        })
        .await;

        // The piece manager handles messages in order, so this already
        // accounts for the block above.
        let remaining = self
            .ask(|response_sender| PieceManagerMessage::GetIncompleteBlocks {
                piece_index,
                response_sender,
            })
            .await?;
        if remaining.is_empty() {
            self.finish_piece(piece_index).await?;
        }
        self.fill_pipeline().await
    }

    /// Hashes an assembled piece and either commits it or throws the progress
    /// away. A bad hash is unattributable — any peer that touched the piece
    /// could have poisoned it — so this only resets it for a redownload
    /// instead of blaming the peer that happened to finish it.
    async fn finish_piece(&mut self, piece_index: u32) -> PeerConnectionResult<()> {
        self.active_blocks.clear();
        self.active_piece = None;
        self.active_piece_length = self.piece_length;

        let valid = self
            .ask(|response_sender| PieceManagerMessage::VerifyPiece {
                piece_index,
                response_sender,
            })
            .await?;
        if valid {
            self.ask(|response_sender| PieceManagerMessage::CompletePiece {
                piece_index,
                response_sender,
            })
            .await?;
        } else {
            warn!(
                "{}: piece {} failed its hash check",
                peer_addr(&self.peer),
                piece_index
            );
            self.send_to_piece_manager(PieceManagerMessage::ResetPiece { piece_index })
                .await;
        }
        self.send_to_piece_manager(PieceManagerMessage::UnlockPiece { piece_index })
            .await;
        self.update_interest().await
    }

    /// Answers a peer's request out of our own storage.
    async fn serve_block(
        &mut self,
        index: u32,
        begin: u32,
        length: u32,
    ) -> PeerConnectionResult<()> {
        if self.am_choking {
            return Ok(());
        }
        // Storage is addressed per block, so a request straddling block
        // boundaries cannot be served.
        if !(begin as u64).is_multiple_of(BLOCK_SIZE) || length as u64 > BLOCK_SIZE {
            debug!(
                "{}: unserviceable request for piece {} at {} ({} bytes)",
                peer_addr(&self.peer),
                index,
                begin,
                length
            );
            return Ok(());
        }
        let has_piece = self
            .ask(|response_sender| PieceManagerMessage::HasPiece {
                piece_index: index,
                response_sender,
            })
            .await?;
        if !has_piece {
            return Ok(());
        }
        let block_index = (begin as u64 / BLOCK_SIZE) as u32;
        let mut block = self
            .ask(|response_sender| PieceManagerMessage::ReadBlock {
                piece_index: index,
                block_index,
                response_sender,
            })
            .await?;
        if block.is_empty() {
            return Ok(());
        }
        block.truncate(length as usize);
        self.send_message(Message::Piece {
            index,
            begin,
            block,
        })
        .await
    }

    /// Unlocking happens on teardown, which races with piece manager shutdown,
    /// so a closed channel is expected rather than fatal.
    async fn send_to_piece_manager(&self, message: PieceManagerMessage) {
        if let Err(e) = self.piece_manager_channel_sender.send(message).await {
            debug!(
                "{}: piece manager unreachable: {}",
                peer_addr(&self.peer),
                e
            );
        }
    }

    /// Nothing notifies us when another peer releases a piece, or when a
    /// timed-out request frees its blocks, so an idle pipeline is retried on a
    /// timer.
    async fn availability_tick(&mut self) -> PeerConnectionResult<()> {
        if self.peer_choking || self.active_blocks.len() >= MAX_REQUESTS as usize {
            return Ok(());
        }
        self.update_interest().await?;
        self.fill_pipeline().await
    }

    async fn send_message(&self, message: Message) -> PeerConnectionResult<()> {
        self.outgoing_channel_sender
            .send(WireItem::Message(message))
            .await
            .map_err(|_| PeerConnectionError::PeerDisconnected)
    }

    async fn ask<T>(
        &mut self,
        build: impl FnOnce(oneshot::Sender<T>) -> PieceManagerMessage,
    ) -> PeerConnectionResult<T> {
        piece_manager_request(&mut self.piece_manager_channel_sender, build).await
    }
}
