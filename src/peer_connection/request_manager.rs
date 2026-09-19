use super::channels::{IncomingChannelReceiver, OutgoingChannelSender};
use super::{close, peer_addr, piece_manager_request};
use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    piece_manager::{
        BLOCK_SIZE,
        channel::{PieceEvent, PieceEventReceiver, PieceManagerChannelSender, PieceManagerMessage},
    },
    status::DownloadStats,
    wire_protocol::{Bitfield, Message, WireItem},
};

use std::sync::Arc;

use tokio::{
    select,
    sync::{broadcast, oneshot},
    time,
};
use tracing::{debug, trace, warn};

/// Off the module path so this series can be switched on alone:
/// `RUST_LOG=dhaar_torrent=info,request_window=trace`.
const WINDOW_TARGET: &str = "request_window";
/// Target for the serving side, separate from `WINDOW_TARGET` so a seeding run
/// and a leeching run can be measured without either burying the other.
const SERVE_TARGET: &str = "serve_latency";

/// A block asked for and not yet given. The timestamp is the round trip the
/// request window is sized against, and nothing else in the client records it.
struct ActiveBlock {
    piece_index: u32,
    index: u32,
    requested_at: time::Instant,
}

/// Any traffic at all resets this. Purely a liveness check.
///
/// Longer than the two minutes peers conventionally leave between keep-alives,
/// or a peer with nothing to say would be dropped while behaving correctly.
const IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(150);
/// How long this connection may stay silent before saying something. Under the
/// two minutes peers conventionally wait, so we speak first.
const KEEP_ALIVE_INTERVAL: time::Duration = time::Duration::from_secs(100);
/// Only block data resets this, so a peer that chats but never delivers
/// stops holding our blocks hostage.
const REQUEST_TIMEOUT: time::Duration = time::Duration::from_secs(30);
/// Pieces we failed to lock can be freed by other peers at any time and no
/// event tells us about it, so we re-check on our own.
const AVAILABILITY_TICK: time::Duration = time::Duration::from_secs(5);
const MAX_REQUESTS: u32 = 50;
/// How many pieces one connection assembles at once.
///
/// One was enough on loopback and crippling anywhere else: a piece is sixteen
/// blocks at the usual length, so a single piece caps the window at 256 KiB
/// however high `MAX_REQUESTS` goes, and the connection falls silent for a
/// whole round trip at every piece boundary while it hashes, writes and claims
/// the next one. Against a real swarm that measured 150 KB/s per peer where
/// Transmission got 1396 from the same tracker and fewer peers.
///
/// The cost is what the single piece bought: a connection that dies strands
/// this many pieces rather than one.
const MAX_PIECES_IN_FLIGHT: usize = 4;

/// A claimed piece, held while this connection works it.
///
/// A panic unwinds past teardown and a task dropped at an await never reaches
/// it, and either way the manager would go on believing the piece is spoken
/// for — permanently, since only an unheld piece can be claimed. Tying the
/// release to the value's lifetime covers what `start` cannot.
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

    /// The ordinary path: it can wait for room in the queue, which `drop`
    /// cannot.
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
        // `drop` cannot await, so this is the one send that must not block. A
        // refusal means the manager is gone; nothing more can be done here,
        // but the piece must not go quietly.
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

pub struct RequestManager<W>
where
    W: crate::store::Store + Send + Sync + 'static,
    W::Error: std::error::Error + Send + Sync + 'static,
{
    pub peer: Option<Peer>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub peer_bitfield: Bitfield,
    /// Pieces this connection is assembling, oldest first. More than one so
    /// the window can stay full across a piece boundary.
    in_flight: Vec<PieceInFlight>,
    active_blocks: Vec<ActiveBlock>,
    pub piece_manager_channel_sender: PieceManagerChannelSender,
    pub incoming_channel_receiver: IncomingChannelReceiver,
    pub outgoing_channel_sender: OutgoingChannelSender,
    /// Taken with the bitfield this connection announced, so the two cannot
    /// disagree about what we hold.
    piece_events: PieceEventReceiver,
    stats: Arc<DownloadStats>,
    /// When this connection last sent anything at all. Keep-alives are only
    /// worth sending into silence, so this is what the timer measures from.
    last_sent: time::Instant,
    /// Shared with every other connection. Safe because access is positional
    /// and pieces occupy disjoint ranges.
    store: Arc<W>,
}

/// A piece being assembled by one connection.
struct PieceBuffer {
    piece_index: u32,
    /// What the torrent says this piece must hash to.
    hash: [u8; 20],
    bytes: Vec<u8>,
    /// Fed in block order; `None` once a block has arrived out of order and
    /// the running hash can no longer be trusted.
    hasher: Option<sha1::Sha1>,
    next_hashed_block: u32,
    /// Which blocks have landed, so the piece knows when it is whole.
    have: Vec<bool>,
}

impl PieceBuffer {
    fn new(piece_index: u32, hash: [u8; 20], piece_length: u64) -> Self {
        Self {
            piece_index,
            hash,
            bytes: vec![0u8; piece_length as usize],
            hasher: Some(<sha1::Sha1 as sha1::Digest>::new()),
            next_hashed_block: 0,
            have: vec![false; piece_length.div_ceil(BLOCK_SIZE) as usize],
        }
    }

    fn is_complete(&self) -> bool {
        self.have.iter().all(|had| *had)
    }

    /// Takes the block in, in every sense: hashes it while it is still its own
    /// value, then copies it into place. Returns false if the block was
    /// already held, which is the endgame loser's copy and must not be counted
    /// or hashed twice.
    fn accept(&mut self, block_index: u32, data: &[u8]) -> bool {
        let Some(had) = self.have.get_mut(block_index as usize) else {
            return false;
        };
        if *had {
            return false;
        }
        *had = true;
        if self.next_hashed_block == block_index {
            if let Some(hasher) = self.hasher.as_mut() {
                sha1::Digest::update(hasher, data);
            }
            self.next_hashed_block += 1;
        } else {
            self.hasher = None;
        }
        let offset = (block_index as u64 * BLOCK_SIZE) as usize;
        if offset < self.bytes.len() {
            let end = (offset + data.len()).min(self.bytes.len());
            self.bytes[offset..end].copy_from_slice(&data[..end - offset]);
        }
        true
    }

    /// The piece's hash, from the running digest when it saw every block in
    /// order, and from the assembled bytes otherwise.
    fn digest(&mut self) -> [u8; 20] {
        let blocks_total = self.have.len() as u32;
        match self.hasher.take() {
            Some(hasher) if self.next_hashed_block == blocks_total => {
                sha1::Digest::finalize(hasher).into()
            }
            _ => <sha1::Sha1 as sha1::Digest>::digest(&self.bytes).into(),
        }
    }
}

/// One piece being assembled, with the blocks of it still to ask for.
struct PieceInFlight {
    hold: PieceHold,
    buffer: PieceBuffer,
    piece_length: u64,
    /// Not yet requested. The manager hands over a whole piece, so pacing the
    /// window across it happens here.
    pending: std::collections::VecDeque<u32>,
}

impl PieceInFlight {
    fn piece_index(&self) -> u32 {
        self.buffer.piece_index
    }

    /// Byte range of one block. The final block of a piece is short, and the
    /// final piece of a torrent is itself short, so this is measured against
    /// the piece's own length.
    fn block_bounds(&self, block_index: u32) -> (u32, u32) {
        let begin = block_index as u64 * BLOCK_SIZE;
        let length = BLOCK_SIZE.min(self.piece_length.saturating_sub(begin));
        (begin as u32, length as u32)
    }
}

impl<W> RequestManager<W>
where
    W: crate::store::Store + Send + Sync + 'static,
    W::Error: std::error::Error + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        peer: Option<Peer>,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        peer_bitfield: Bitfield,
        piece_manager_channel_sender: PieceManagerChannelSender,
        incoming_channel_receiver: IncomingChannelReceiver,
        outgoing_channel_sender: OutgoingChannelSender,
        piece_events: PieceEventReceiver,
        stats: Arc<DownloadStats>,
        store: Arc<W>,
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
            in_flight: Vec::new(),
            active_blocks: Vec::new(),
            piece_manager_channel_sender,
            incoming_channel_receiver,
            outgoing_channel_sender,
            piece_events,
            stats,
            last_sent: time::Instant::now(),
            store,
        }
    }

    /// Index of the piece this connection currently holds.
    /// Which of our in-flight pieces this index is, if any.
    fn slot_of(&self, piece_index: u32) -> Option<usize> {
        self.in_flight
            .iter()
            .position(|piece| piece.piece_index() == piece_index)
    }

    pub async fn start(mut self) {
        match self.run().await {
            Ok(()) | Err(PeerConnectionError::PeerDisconnected) => {
                debug!("{}: connection ended", peer_addr(&self.peer));
            }
            Err(e) => warn!("{}: connection ended: {}", peer_addr(&self.peer), e),
        }
        self.release_all_pieces().await;
        close(&self.peer);
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
                    self.release_all_pieces().await;
                    request_deadline = None;
                },
                _ = availability_tick.tick() => {
                    self.availability_tick().await?;
                },
                // Re-armed from `last_sent` on every pass, so real traffic
                // keeps pushing it back and it only fires into silence.
                _ = time::sleep_until(self.last_sent + KEEP_ALIVE_INTERVAL) => {
                    debug!("{}: keep-alive", peer_addr(&self.peer));
                    self.send_message(Message::KeepAlive).await?;
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
                self.release_all_pieces().await;
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
            // Nothing to do beyond having arrived: reaching here has already
            // pushed back the idle deadline, which is the whole point of it.
            WireItem::Message(Message::KeepAlive) => {}
            // DHT is not implemented, so the peer's DHT port is of no use.
            WireItem::Message(Message::Port(_port)) => {}
            _ => {}
        }
        Ok(())
    }

    /// Reacts to work finished elsewhere. A piece we were working ourselves
    /// comes back through here too, and falls through as a no-op.
    async fn handle_piece_event(&mut self, event: PieceEvent) -> PeerConnectionResult<()> {
        match event {
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
    /// most one piece.
    ///
    /// The manager is asked once per piece, not once per refill: a claim hands
    /// over the whole piece, and which of its blocks to ask for next is this
    /// connection's own business. That is the difference between two messages
    /// per piece and one per window slot.
    async fn fill_pipeline(&mut self) -> PeerConnectionResult<()> {
        if self.peer_choking || !self.am_interested {
            return Ok(());
        }

        let mut sent = 0;
        while self.active_blocks.len() < MAX_REQUESTS as usize {
            // Oldest piece first, so pieces finish rather than all advancing
            // together — a finished piece is one that can be written and
            // announced, a half-finished one is only memory.
            let next = self
                .in_flight
                .iter_mut()
                .enumerate()
                .find_map(|(slot, piece)| piece.pending.pop_front().map(|block| (slot, block)));

            let (slot, block_index) = match next {
                Some(found) => found,
                // Nothing left to ask for in anything we hold: take on another
                // piece rather than let the window drain.
                None => {
                    if self.in_flight.len() >= MAX_PIECES_IN_FLIGHT
                        || !self.claim_another_piece().await?
                    {
                        break;
                    }
                    continue;
                }
            };

            let piece_index = self.in_flight[slot].piece_index();
            let (begin, length) = self.in_flight[slot].block_bounds(block_index);
            self.send_message(Message::Request {
                index: piece_index,
                begin,
                length,
            })
            .await?;
            self.active_blocks.push(ActiveBlock {
                piece_index,
                index: block_index,
                requested_at: time::Instant::now(),
            });
            sent += 1;
        }

        if sent > 0 {
            debug!(
                "{}: requested {} block(s) across {} piece(s), {} in flight",
                peer_addr(&self.peer),
                sent,
                self.in_flight.len(),
                self.active_blocks.len()
            );
            trace!(
                target: WINDOW_TARGET,
                peer = %peer_addr(&self.peer),
                depth = self.active_blocks.len(),
                pieces = self.in_flight.len(),
                granted = sent,
            );
        }
        Ok(())
    }

    /// Asks the manager for one more piece. `false` means it had nothing for
    /// us, which is ordinary near the end of a download.
    async fn claim_another_piece(&mut self) -> PeerConnectionResult<bool> {
        let bitfield = self.peer_bitfield.clone();
        let peer = self.peer.unwrap();
        let Some(claim) = self
            .ask(|response_sender| PieceManagerMessage::ClaimPiece {
                bitfield,
                peer,
                response_sender,
            })
            .await?
        else {
            return Ok(false);
        };
        debug!(
            "{}: claimed piece {} ({} bytes)",
            peer_addr(&self.peer),
            claim.piece_index,
            claim.piece_length
        );
        self.in_flight.push(PieceInFlight {
            hold: PieceHold::new(
                claim.piece_index,
                peer,
                self.piece_manager_channel_sender.clone(),
            ),
            buffer: PieceBuffer::new(claim.piece_index, claim.hash, claim.piece_length),
            piece_length: claim.piece_length,
            pending: (0..claim.piece_length.div_ceil(BLOCK_SIZE) as u32).collect(),
        });
        Ok(true)
    }

    /// Hands the active piece back to the piece manager. Every path that
    /// abandons requests has to go through here: registrations are otherwise
    /// only cleared by data arriving, and requests we walk away from would
    /// Hands every piece back. Nothing of them was written, so the bytes are
    /// only useful to a connection that goes on to finish them.
    async fn release_all_pieces(&mut self) {
        let held: Vec<u32> = self.in_flight.iter().map(|p| p.piece_index()).collect();
        if !held.is_empty() {
            debug!(
                "{}: releasing piece(s) {:?} with {} request(s) outstanding",
                peer_addr(&self.peer),
                held,
                self.active_blocks.len()
            );
        }
        self.active_blocks.clear();
        for mut piece in std::mem::take(&mut self.in_flight) {
            piece.hold.release().await;
        }
    }

    /// Files a block the peer sent us. Data we never asked for is dropped:
    /// accepting it would clear a lock we do not hold.
    async fn receive_block(
        &mut self,
        index: u32,
        begin: u32,
        block: Vec<u8>,
    ) -> PeerConnectionResult<()> {
        let Some(slot) = self.slot_of(index) else {
            debug!(
                "{}: block for piece {}, which we are not working",
                peer_addr(&self.peer),
                index
            );
            self.stats.add_wasted(block.len() as u64);
            return Ok(());
        };
        if !(begin as u64).is_multiple_of(BLOCK_SIZE) {
            debug!(
                "{}: block {}+{} is not on a block boundary",
                peer_addr(&self.peer),
                index,
                begin
            );
            self.stats.add_wasted(block.len() as u64);
            return Ok(());
        }
        let piece_index = index;
        let block_index = (begin as u64 / BLOCK_SIZE) as u32;
        // Blocks of several pieces are in flight at once now, so a block is
        // only ours if both halves match.
        let Some(position) = self
            .active_blocks
            .iter()
            .position(|active| active.piece_index == piece_index && active.index == block_index)
        else {
            debug!(
                "{}: block {} of piece {} was not requested",
                peer_addr(&self.peer),
                block_index,
                piece_index
            );
            // Bytes that crossed the network and cannot be used -- most often
            // an endgame copy still in flight when the cancel was sent. The
            // wasted figure is how the endgame threshold would be tuned, so
            // dropping these silently understates exactly the thing it exists
            // to measure.
            self.stats.add_wasted(block.len() as u64);
            return Ok(());
        };
        let active = self.active_blocks.swap_remove(position);
        // The decrement half of the window series, and the only place the
        // round trip can be closed. Emitted as bare samples rather than folded
        // into running averages: what to do with the numbers -- mean depth
        // over time, how fast a piece drains, the latency distribution -- is a
        // question for whatever reads the log, and answering it there costs
        // nothing here and can be changed without another run.
        //
        // `mailbox` is how many messages are already queued for the piece
        // manager. It is one task serving every connection, and it writes to
        // disk and hashes inline, so when it falls behind every peer stalls at
        // once and each one looks individually slow. Sampling it here, on the
        // hot path, is what separates "waiting on the peer" from "waiting on
        // ourselves".
        trace!(
            target: WINDOW_TARGET,
            peer = %peer_addr(&self.peer),
            depth = self.active_blocks.len(),
            piece = piece_index,
            block = block_index,
            latency_us = active.requested_at.elapsed().as_micros() as u64,
            mailbox = self.piece_manager_channel_sender.max_capacity()
                - self.piece_manager_channel_sender.capacity(),
        );
        self.stats.add_downloaded(block.len() as u64);

        // The block goes no further than this task. Buffering, hashing and --
        // once the piece is whole -- writing all happen here, so a download
        // with eight peers verifies eight pieces on eight cores instead of
        // queueing them all behind one.
        let accepted = self.in_flight[slot].buffer.accept(block_index, &block);
        if !accepted {
            // Already held. An endgame copy that lost its race, and hashing it
            // twice would corrupt the digest.
            self.stats.add_wasted(block.len() as u64);
            return self.fill_pipeline().await;
        }
        drop(block);

        if self.in_flight[slot].buffer.is_complete() {
            self.complete_piece(slot).await?;
        }

        // The piece stays ours until the manager says it is spent; topping it
        // up is `fill_pipeline`'s job.
        self.fill_pipeline().await
    }

    /// Verifies the assembled piece and, if it holds up, writes it before
    /// telling the manager. The order matters: the manager sets the bitfield
    /// bit on that message, and the bitfield is what makes a piece servable to
    /// other peers.
    async fn complete_piece(&mut self, slot: usize) -> PeerConnectionResult<()> {
        let mut piece = self.in_flight.swap_remove(slot);
        let peer = self.peer.unwrap();
        let piece_index = piece.piece_index();
        let expected = piece.buffer.hash;
        let digest = piece.buffer.digest();
        // Nothing outstanding for this piece is wanted any more, however this
        // turns out; leaving them would keep the window full of requests whose
        // answers have nowhere to go.
        self.active_blocks
            .retain(|active| active.piece_index != piece_index);
        // The guard has nothing to hand back: either the manager takes the
        // piece on `PieceVerified`, or `PieceFailed` unclaims it below.
        piece.hold.disarm();

        if digest != expected {
            self.send_to_piece_manager(PieceManagerMessage::PieceFailed { piece_index, peer })
                .await;
            return Ok(());
        }

        if let Err(e) = self.store.write(piece_index, 0, piece.buffer.bytes).await {
            // Nothing was claimed, so the piece is simply still missing. Give
            // it back rather than reporting a completion the store cannot
            // back up.
            warn!(
                "{}: piece {} could not be written: {}",
                peer_addr(&self.peer),
                piece_index,
                e
            );
            self.send_to_piece_manager(PieceManagerMessage::PieceFailed { piece_index, peer })
                .await;
            return Ok(());
        }

        // The claim on disk, before the claim in memory. The bitfield is what
        // makes a piece servable, so recording it after `PieceVerified` would
        // advertise a piece the store does not yet admit to holding. A failure
        // here costs a re-download after an unclean exit and nothing else, so
        // it is not worth abandoning a piece that verified.
        if let Err(e) = self.store.record_piece(piece_index).await {
            warn!(
                "{}: piece {} written but not claimed on disk: {}",
                peer_addr(&self.peer),
                piece_index,
                e
            );
        }
        self.send_to_piece_manager(PieceManagerMessage::PieceVerified { piece_index, peer })
            .await;
        Ok(())
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
        // Straight to the store, which this connection holds anyway. It used
        // to go through the piece manager, and that cost two things: a turn of
        // the one task every peer shares, and a whole piece read off disk to
        // answer for one block of it -- sixteen times the bytes at the usual
        // piece length.
        //
        // Reads hit the page cache on a small torrent and the device on a
        // large one, and the gap between those is three orders of magnitude,
        // so the distribution matters far more than any average of it.
        let read_started = time::Instant::now();
        let block = self
            .store
            .read(index, begin as u64, length as u64)
            .await
            .unwrap_or_default();
        trace!(
            target: SERVE_TARGET,
            peer = %peer_addr(&self.peer),
            piece = index,
            block = block_index,
            read_us = read_started.elapsed().as_micros() as u64,
            empty = block.is_empty(),
        );
        if block.is_empty() {
            debug!(
                "{}: storage returned nothing for piece {} block {}",
                peer_addr(&self.peer),
                index,
                block_index
            );
            return Ok(());
        }
        self.stats.add_uploaded(block.len() as u64);
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

    async fn send_message(&mut self, message: Message) -> PeerConnectionResult<()> {
        self.outgoing_channel_sender
            .send(WireItem::Message(message))
            .await
            .map_err(|_| PeerConnectionError::PeerDisconnected)?;
        // Every send pushes the keep-alive out, so one is only ever written
        // when this connection would otherwise have gone quiet.
        self.last_sent = time::Instant::now();
        Ok(())
    }

    async fn ask<T>(
        &mut self,
        build: impl FnOnce(oneshot::Sender<T>) -> PieceManagerMessage,
    ) -> PeerConnectionResult<T> {
        piece_manager_request(&mut self.piece_manager_channel_sender, build).await
    }
}
