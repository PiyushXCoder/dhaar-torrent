use super::channels::{IncomingChannelReceiver, OutgoingChannelSender};
use super::{close, peer_addr, piece_manager_request};
use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    peer_manager::channels::PeerManagerChannelSender,
    piece_manager::{
        BLOCK_SIZE,
        channel::{PieceEvent, PieceEventReceiver, PieceManagerChannelSender, PieceManagerMessage},
    },
    wire_protocol::{Bitfield, Message, WireItem},
};

use tokio::{
    select,
    sync::{broadcast, oneshot},
    time,
};
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

/// A claimed piece, held for as long as this connection is working it.
///
/// Releasing is not something a connection can be trusted to do on its way
/// out: a panic unwinds past every line of teardown, and a task dropped at an
/// await point never reaches them at all. Either way the piece manager would
/// go on believing the piece is spoken for, and since only an unheld piece can
/// be claimed, nobody could ever pick it up again. Tying the release to the
/// value's lifetime covers the paths that `start` cannot.
struct PieceHold {
    piece_index: u32,
    peer: Peer,
    /// Set once the piece is known to be back with the manager, so `drop`
    /// stays quiet.
    released: bool,
    piece_manager_channel_sender: PieceManagerChannelSender,
}

impl PieceHold {
    fn new(
        piece_index: u32,
        peer: Peer,
        piece_manager_channel_sender: PieceManagerChannelSender,
    ) -> Self {
        Self {
            piece_index,
            peer,
            released: false,
            piece_manager_channel_sender,
        }
    }

    /// Hands the piece back and disarms the guard. This is the ordinary path:
    /// it can wait for room in the queue, which `drop` cannot.
    async fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        if let Err(e) = self
            .piece_manager_channel_sender
            .send(PieceManagerMessage::Release {
                piece_index: self.piece_index,
                peer: self.peer,
            })
            .await
        {
            debug!("piece manager unreachable while releasing: {}", e);
        }
    }

    /// Marks the piece as already back with the manager — it took it back
    /// itself as part of handing out the next one.
    fn disarm(&mut self) {
        self.released = true;
    }
}

impl Drop for PieceHold {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        // `drop` cannot await, so this is the one send that has to be
        // non-blocking. The queue is deep and this message is small, so a
        // refusal means the manager is gone or badly backed up; nothing
        // further can be done from here, but the piece must not go quietly.
        if let Err(e) = self
            .piece_manager_channel_sender
            .try_send(PieceManagerMessage::Release {
                piece_index: self.piece_index,
                peer: self.peer,
            })
        {
            warn!(
                "{}: piece {} stranded, release could not be sent: {}",
                self.peer.address, self.piece_index, e
            );
            return;
        }
        warn!(
            "{}: piece {} released without teardown",
            self.peer.address, self.piece_index
        );
    }
}

pub struct RequestManager {
    pub peer: Option<Peer>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub peer_bitfield: Bitfield,
    active_piece: Option<PieceHold>,
    pub active_piece_length: u64,
    pub active_blocks: Vec<u32>,
    pub peer_manager_channel_sender: Option<PeerManagerChannelSender>,
    pub piece_manager_channel_sender: PieceManagerChannelSender,
    pub incoming_channel_receiver: IncomingChannelReceiver,
    pub outgoing_channel_sender: OutgoingChannelSender,
    /// Taken with the bitfield this connection announced, so the two cannot
    /// disagree about what we hold.
    piece_events: PieceEventReceiver,
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
        piece_events: PieceEventReceiver,
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
            piece_events,
        }
    }

    /// Index of the piece this connection currently holds.
    fn held_piece(&self) -> Option<u32> {
        self.active_piece.as_ref().map(|hold| hold.piece_index)
    }

    pub async fn start(mut self) {
        match self.run().await {
            Ok(()) | Err(PeerConnectionError::PeerDisconnected) => {
                debug!("{}: connection ended", peer_addr(&self.peer));
            }
            Err(e) => warn!("{}: connection ended: {}", peer_addr(&self.peer), e),
        }
        self.release_active_piece().await;
        close(&mut self.peer_manager_channel_sender, &mut self.peer).await;
    }

    async fn run(&mut self) -> PeerConnectionResult<()> {
        let mut idle_deadline = time::Instant::now() + IDLE_TIMEOUT;
        let mut request_deadline: Option<time::Instant> = None;
        let mut availability_tick = time::interval(AVAILABILITY_TICK);

        debug!("{}: request loop started", peer_addr(&self.peer));
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
                    self.release_active_piece().await;
                    request_deadline = None;
                },
                _ = availability_tick.tick() => {
                    self.availability_tick().await?;
                },
                event = self.piece_events.recv() => {
                    match event {
                        Ok(event) => self.handle_piece_event(event).await?,
                        // Falling behind costs a duplicate block or an
                        // unannounced piece, never correctness, so carry on.
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            warn!(
                                "{}: missed {} piece event(s)",
                                peer_addr(&self.peer),
                                missed
                            );
                        }
                        // The piece manager is gone, so there is nothing left
                        // to download and nothing to serve from.
                        Err(broadcast::error::RecvError::Closed) => {
                            return Err(PeerConnectionError::PeerDisconnected);
                        }
                    }
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
                debug!(
                    "{}: choked us, {} request(s) dropped",
                    peer_addr(&self.peer),
                    self.active_blocks.len()
                );
                self.peer_choking = true;
                // The peer throws away every request it has not answered, so
                // holding those block locks would strand them.
                self.release_active_piece().await;
            }
            WireItem::Message(Message::Unchoke) => {
                debug!("{}: unchoked us", peer_addr(&self.peer));
                self.peer_choking = false;
                self.fill_pipeline().await?;
            }
            WireItem::Message(Message::Interested) => {
                debug!("{}: interested in us", peer_addr(&self.peer));
                self.peer_interested = true;
                // There is no upload policy yet, so whoever asks gets served.
                if self.am_choking {
                    debug!("{}: unchoking", peer_addr(&self.peer));
                    self.am_choking = false;
                    self.send_message(Message::Unchoke).await?;
                }
            }
            WireItem::Message(Message::NotInterested) => {
                debug!("{}: no longer interested in us", peer_addr(&self.peer));
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
            // Requests are answered inline as they arrive: `serve_block` runs
            // to completion before the next message is read, so by the time a
            // cancel is seen its block has already gone out. There is no
            // upload queue to drop it from, only the record that the peer
            // stopped wanting it.
            WireItem::Message(Message::Cancel {
                index,
                begin,
                length,
            }) => {
                debug!(
                    "{}: cancelled its request for piece {} at {} ({} bytes), already served",
                    peer_addr(&self.peer),
                    index,
                    begin,
                    length
                );
            }
            // DHT is not implemented, so the peer's DHT port is of no use.
            WireItem::Message(Message::Port(_port)) => {}
            _ => {}
        }
        Ok(())
    }

    /// Reacts to work finished elsewhere. Our own deliveries come back
    /// through here too, but they have already left `active_blocks` by then,
    /// so they fall through as no-ops.
    async fn handle_piece_event(&mut self, event: PieceEvent) -> PeerConnectionResult<()> {
        match event {
            PieceEvent::BlockComplete {
                piece_index,
                block_index,
            } => {
                if self.held_piece() != Some(piece_index) {
                    return Ok(());
                }
                let Some(position) = self
                    .active_blocks
                    .iter()
                    .position(|active| *active == block_index)
                else {
                    return Ok(());
                };
                // Endgame had us racing another peer for this block and we
                // lost. Stop the transfer rather than pay for a copy of data
                // that is already on disk.
                self.active_blocks.swap_remove(position);
                let (begin, length) = self.block_bounds(block_index);
                self.send_message(Message::Cancel {
                    index: piece_index,
                    begin,
                    length,
                })
                .await?;
                debug!(
                    "{}: cancelled block {} of piece {}, another peer delivered it",
                    peer_addr(&self.peer),
                    block_index,
                    piece_index
                );
                // A slot just came free.
                self.fill_pipeline().await?;
            }
            PieceEvent::PieceComplete { piece_index } => {
                self.send_message(Message::Have(piece_index)).await?;
                // Finishing a piece can be what makes this peer uninteresting.
                self.update_interest().await?;
            }
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
        debug!(
            "{}: we are now {}",
            peer_addr(&self.peer),
            if interested {
                "interested"
            } else {
                "not interested"
            }
        );
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
    ///
    /// The piece manager both chooses the piece and registers us against its
    /// blocks in one message: asking what is free and then claiming it would
    /// let a second peer take the same piece in between.
    async fn fill_pipeline(&mut self) -> PeerConnectionResult<()> {
        if self.peer_choking || !self.am_interested {
            return Ok(());
        }
        let Some(capacity) = MAX_REQUESTS.checked_sub(self.active_blocks.len() as u32) else {
            return Ok(());
        };
        if capacity == 0 {
            return Ok(());
        }

        let bitfield = self.peer_bitfield.clone();
        let peer = self.peer.unwrap();
        let piece_index = self.held_piece();
        let reply = self
            .ask(|response_sender| PieceManagerMessage::ClaimBlocks {
                piece_index,
                bitfield,
                peer,
                max_blocks: capacity,
                response_sender,
            })
            .await?;

        // A spent piece is taken back by the manager in the same turn it picks
        // the replacement. Mirror what it reports instead of inferring it: if
        // it ever stops taking pieces back, we keep holding this one and hand
        // it over at teardown, rather than stranding it forever.
        if let Some(released) = reply.released {
            debug!(
                "{}: piece {} is spent, taken back",
                peer_addr(&self.peer),
                released
            );
            if self.held_piece() == Some(released) {
                // Already back with the manager, so there is nothing left for
                // the guard to hand over.
                if let Some(hold) = self.active_piece.as_mut() {
                    hold.disarm();
                }
                self.active_piece = None;
            }
        }

        let Some(claim) = reply.granted else {
            return Ok(());
        };

        if self.held_piece() != Some(claim.piece_index) {
            debug!(
                "{}: claimed piece {} ({} bytes)",
                peer_addr(&self.peer),
                claim.piece_index,
                claim.piece_length
            );
            self.active_piece = Some(PieceHold::new(
                claim.piece_index,
                peer,
                self.piece_manager_channel_sender.clone(),
            ));
        }
        self.active_piece_length = claim.piece_length;

        for block_index in claim.blocks.iter().copied() {
            let (begin, length) = self.block_bounds(block_index);
            self.send_message(Message::Request {
                index: claim.piece_index,
                begin,
                length,
            })
            .await?;
            self.active_blocks.push(block_index);
        }
        if !claim.blocks.is_empty() {
            debug!(
                "{}: requested {} block(s) of piece {}, {} in flight",
                peer_addr(&self.peer),
                claim.blocks.len(),
                claim.piece_index,
                self.active_blocks.len()
            );
        }
        Ok(())
    }

    /// Hands the active piece back to the piece manager. Every path that
    /// abandons requests has to go through here: registrations are otherwise
    /// only cleared by data arriving, and requests we walk away from would
    /// keep the piece locked for the rest of the session.
    async fn release_active_piece(&mut self) {
        let Some(mut hold) = self.active_piece.take() else {
            return;
        };
        debug!(
            "{}: releasing piece {} with {} request(s) outstanding",
            peer_addr(&self.peer),
            hold.piece_index,
            self.active_blocks.len()
        );
        self.active_blocks.clear();
        hold.release().await;
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
        let Some(piece_index) = self.held_piece() else {
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

        let peer = self.peer.unwrap();
        self.send_to_piece_manager(PieceManagerMessage::ReceiveBlock {
            piece_index,
            block_index,
            block_data: block,
            peer,
        })
        .await;

        // The piece stays ours until the manager says it is spent; topping it
        // up is `fill_pipeline`'s job.
        self.fill_pipeline().await
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
            debug!(
                "{}: asked for piece {}, which we do not have",
                peer_addr(&self.peer),
                index
            );
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
            debug!(
                "{}: storage returned nothing for piece {} block {}",
                peer_addr(&self.peer),
                index,
                block_index
            );
            return Ok(());
        }
        block.truncate(length as usize);
        debug!(
            "{}: serving block {} of piece {} ({} bytes)",
            peer_addr(&self.peer),
            block_index,
            index,
            length
        );
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
