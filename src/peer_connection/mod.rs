use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    peer_manager::channels::{PeerManagerChannelMessage, PeerManagerChannelSender},
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
    wire_protocol::{Bitfield, Handshake, Message, WireCodec, WireItem},
};

use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

pub mod error;

pub struct PeerConnection {
    pub peer: Option<Peer>,
    pub peer_manager_channel_sender: Option<PeerManagerChannelSender>,
    pub piece_manager_channel_sender: PieceManagerChannelSender,
    pub stream: Option<TcpStream>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub peer_bitfield: Option<Bitfield>,
    pub requested_pieces: Vec<u32>,
}

const BLOCK_SIZE: u32 = 16384;

impl PeerConnection {
    /// Connects to `peer`. On failure the peer is handed back so the caller
    /// can requeue it, instead of panicking the whole peer manager loop over
    /// a single dead/unreachable peer.
    pub async fn connect(
        peer: Peer,
        peer_manager_channel_sender: PeerManagerChannelSender,
        piece_manager_channel_sender: PieceManagerChannelSender,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
    ) -> PeerConnectionResult<Self> {
        let stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .await
            .map_err(|source| PeerConnectionError::ConnectFailed {
                peer: Box::new(peer.clone()),
                source,
            })?;

        Ok(PeerConnection {
            peer: Some(peer),
            peer_manager_channel_sender: Some(peer_manager_channel_sender),
            piece_manager_channel_sender,
            stream: Some(stream),
            info_hash: *info_hash,
            peer_id: *peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield: None,
            requested_pieces: Vec::new(),
        })
    }

    pub async fn from_stream(
        stream: TcpStream,
        peer_manager_channel_sender: PeerManagerChannelSender,
        piece_manager_channel_sender: PieceManagerChannelSender,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
    ) -> Self {
        PeerConnection {
            peer: None,
            peer_manager_channel_sender: Some(peer_manager_channel_sender),
            piece_manager_channel_sender,
            stream: Some(stream),
            info_hash: *info_hash,
            peer_id: *peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield: None,
            requested_pieces: Vec::new(),
        }
    }
    /// Drives the connection to completion. `run` never closes the connection
    /// itself, it just reports why it stopped; teardown happens here so every
    /// error path funnels through exactly one `close`.
    pub async fn start(mut self) {
        tokio::spawn(async move {
            match self.run().await {
                Ok(()) | Err(PeerConnectionError::PeerDisconnected) => {
                    debug!("{}: connection ended", self.peer_addr());
                }
                Err(e) => warn!("{}: connection ended: {}", self.peer_addr(), e),
            }
            self.close().await;
        });
    }

    async fn run(&mut self) -> PeerConnectionResult<()> {
        let mut framed = Framed::new(self.stream.take().unwrap(), WireCodec::new());
        self.handshake(&mut framed).await?;
        let our_bitfield = self
            .piece_manager_request(|tx| PieceManagerMessage::Bitfield {
                response_sender: tx,
            })
            .await?;

        framed
            .send(WireItem::Message(Message::Bitfield(our_bitfield)))
            .await?;

        let mut peer_bitfield: Option<Bitfield> = None;
        let mut current_piece: Option<u32> = None;

        loop {
            match framed.next().await {
                Some(Ok(WireItem::Message(Message::Choke))) => {
                    debug!("{}: peer choked us", self.peer_addr());
                    self.peer_choking = true;
                }
                Some(Ok(WireItem::Message(Message::Unchoke))) => {
                    debug!("{}: peer unchoked us", self.peer_addr());
                    self.peer_choking = false;
                    self.request_next_block(
                        &mut framed,
                        &mut current_piece,
                        peer_bitfield.as_ref(),
                    )
                    .await?;
                }
                Some(Ok(WireItem::Message(Message::Interested))) => {
                    debug!("{}: peer is interested", self.peer_addr());
                    self.peer_interested = true;
                }
                Some(Ok(WireItem::Message(Message::NotInterested))) => {
                    debug!("{}: peer is not interested", self.peer_addr());
                    self.peer_interested = false;
                }
                Some(Ok(WireItem::Message(Message::Have(piece_index)))) => {
                    debug!("{}: peer has piece {}", self.peer_addr(), piece_index);
                    if let Some(peer_bitfield) = peer_bitfield.as_mut() {
                        peer_bitfield.set_piece(piece_index, true);
                    }
                }
                Some(Ok(WireItem::Message(Message::Bitfield(bitfield)))) => {
                    peer_bitfield = Some(bitfield);
                    let am_interested = self
                        .piece_manager_request(|tx| PieceManagerMessage::AmInterested {
                            bitfield: peer_bitfield.as_ref().unwrap().to_owned(),
                            response_sender: tx,
                        })
                        .await?;
                    self.am_interested = am_interested;
                    debug!(
                        "{}: received peer bitfield, am_interested={}",
                        self.peer_addr(),
                        am_interested
                    );
                    framed
                        .send(WireItem::Message(if am_interested {
                            Message::Interested
                        } else {
                            Message::NotInterested
                        }))
                        .await?;
                }
                Some(Ok(WireItem::Message(Message::Request {
                    index,
                    begin,
                    length: _length,
                }))) => {
                    if self.am_choking {
                        debug!(
                            "{}: ignoring request for piece {} (choking peer)",
                            self.peer_addr(),
                            index
                        );
                    } else {
                        let block_index = begin / BLOCK_SIZE;
                        debug!(
                            "{}: peer requested piece {} block {}",
                            self.peer_addr(),
                            index,
                            block_index
                        );
                        let block = self
                            .piece_manager_request(|tx| PieceManagerMessage::ReadBlock {
                                piece_index: index,
                                block_index,
                                response_sender: tx,
                            })
                            .await?;
                        framed
                            .send(WireItem::Message(Message::Piece {
                                index,
                                begin,
                                block,
                            }))
                            .await?;
                    }
                }
                Some(Ok(WireItem::Message(Message::Piece {
                    index,
                    begin,
                    block,
                }))) => {
                    let block_index = begin / BLOCK_SIZE;
                    debug!(
                        "{}: received piece {} block {} ({} bytes)",
                        self.peer_addr(),
                        index,
                        block_index,
                        block.len()
                    );
                    self.piece_manager_notify(PieceManagerMessage::ReceiveBlock {
                        piece_index: index,
                        block_index,
                        block_data: block,
                    })
                    .await?;
                    self.request_next_block(
                        &mut framed,
                        &mut current_piece,
                        peer_bitfield.as_ref(),
                    )
                    .await?;
                }
                Some(Ok(WireItem::Message(Message::Cancel {
                    index: _index,
                    begin: _begin,
                    length: _length,
                }))) => {
                    debug!(
                        "{}: ignoring cancel, requests are served synchronously, nothing queued to cancel",
                        self.peer_addr()
                    );
                }
                Some(Ok(WireItem::Message(Message::Port(_port)))) => {
                    debug!(
                        "{}: ignoring port message, DHT not implemented",
                        self.peer_addr()
                    );
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Err(PeerConnectionError::PeerDisconnected),
                _ => {}
            }
        }
    }

    pub async fn close(&mut self) {
        debug!("{}: closing connection", self.peer_addr());
        if let Some((peer, peer_manager_channel_sender)) = self
            .peer
            .take()
            .zip(self.peer_manager_channel_sender.take())
            && let Err(e) = peer_manager_channel_sender
                .send(PeerManagerChannelMessage::Closing(peer))
                .await
        {
            error!("Failed to close peer connection: {}", e);
        }
    }

    fn peer_addr(&self) -> String {
        match self.peer.as_ref() {
            Some(peer) => format!("{}:{}", peer.ip, peer.port),
            None => "unknown".to_string(),
        }
    }

    /// Requests the next available block, one at a time (no pipelining).
    /// Locks a piece from `peer_bitfield` if we're not already working on
    /// one, then locks and requests its next block; once a piece runs out
    /// of unlocked blocks it's verified, completed, and unlocked before
    /// moving on to the next.
    async fn request_next_block(
        &mut self,
        framed: &mut Framed<TcpStream, WireCodec>,
        current_piece: &mut Option<u32>,
        peer_bitfield: Option<&Bitfield>,
    ) -> PeerConnectionResult<()> {
        if !self.am_interested || self.peer_choking {
            return Ok(());
        }

        loop {
            let piece_index = match *current_piece {
                Some(piece_index) => piece_index,
                None => {
                    let Some(bitfield) = peer_bitfield else {
                        return Ok(());
                    };
                    let Some(piece_index) = self
                        .piece_manager_request(|tx| PieceManagerMessage::LockNextPiece {
                            bitfield: bitfield.to_owned(),
                            response_sender: tx,
                        })
                        .await?
                    else {
                        return Ok(());
                    };
                    *current_piece = Some(piece_index);
                    piece_index
                }
            };

            let Some(block_index) = self
                .piece_manager_request(|tx| PieceManagerMessage::LockNextBlock {
                    piece_index,
                    response_sender: tx,
                })
                .await?
            else {
                let verified = self
                    .piece_manager_request(|tx| PieceManagerMessage::VerifyPiece {
                        piece_index,
                        response_sender: tx,
                    })
                    .await?;
                if verified {
                    self.piece_manager_request(|tx| PieceManagerMessage::CompletePiece {
                        piece_index,
                        response_sender: tx,
                    })
                    .await?;
                }
                debug!(
                    "{}: piece {} complete, verified={}",
                    self.peer_addr(),
                    piece_index,
                    verified
                );
                self.piece_manager_notify(PieceManagerMessage::UnlockPiece { piece_index })
                    .await?;
                *current_piece = None;
                continue;
            };

            let begin = block_index * BLOCK_SIZE;
            debug!(
                "{}: requesting piece {} block {}",
                self.peer_addr(),
                piece_index,
                block_index
            );
            framed
                .send(WireItem::Message(Message::Request {
                    index: piece_index,
                    begin,
                    length: BLOCK_SIZE,
                }))
                .await?;
            return Ok(());
        }
    }

    async fn piece_manager_notify(
        &mut self,
        message: PieceManagerMessage,
    ) -> PeerConnectionResult<()> {
        self.piece_manager_channel_sender.send(message).await?;
        Ok(())
    }

    async fn piece_manager_request<T>(
        &mut self,
        build: impl FnOnce(oneshot::Sender<T>) -> PieceManagerMessage,
    ) -> error::PeerConnectionResult<T> {
        let (tx, rx) = oneshot::channel();
        self.piece_manager_channel_sender.send(build(tx)).await?;
        Ok(rx.await?)
    }

    fn our_handshake(&self) -> Handshake {
        Handshake {
            pstrlen: 19,
            pstr: "BitTorrent protocol".to_string(),
            reserved: [0; 8],
            info_hash: self.info_hash,
            peer_id: self.peer_id,
        }
    }

    /// Runs the handshake for this connection and reports success. On
    /// outbound connections we already know `info_hash`/`peer_id` (we chose
    /// this peer for this torrent), so we send first and just verify the
    /// peer agrees. On inbound connections we don't know who this is until
    /// they tell us, so we must wait for their handshake before responding.
    pub async fn handshake(
        &mut self,
        framed: &mut Framed<TcpStream, WireCodec>,
    ) -> error::PeerConnectionResult<()> {
        if self.peer.is_some() {
            self.handshake_outbound(framed).await?;
        } else {
            self.handshake_inbound(framed).await?;
        }
        Ok(())
    }

    async fn handshake_outbound(
        &mut self,
        framed: &mut Framed<TcpStream, WireCodec>,
    ) -> error::PeerConnectionResult<()> {
        framed
            .send(WireItem::Handshake(self.our_handshake()))
            .await?;
        loop {
            match framed.next().await {
                Some(Ok(WireItem::HandshakePartial { info_hash })) => {
                    if info_hash != self.info_hash {
                        warn!("{}: handshake failed, info_hash mismatch", self.peer_addr());
                        return Err(error::PeerConnectionError::InfoHashMismatch);
                    }
                }
                Some(Ok(WireItem::Handshake(handshake))) => {
                    let Some(peer) = self.peer.as_mut() else {
                        error!("Handshake failed: outbound connection missing peer");
                        return Err(error::PeerConnectionError::PeerNotFound);
                    };
                    peer.peer_id = Some(handshake.peer_id);
                    debug!("Outbound handshake complete with {}:{}", peer.ip, peer.port);
                    return Ok(());
                }
                _ => {
                    warn!(
                        "{}: handshake failed, unexpected message or connection closed",
                        self.peer_addr()
                    );
                    return Err(error::PeerConnectionError::UnexpectedMessage);
                }
            }
        }
    }

    async fn handshake_inbound(
        &mut self,
        framed: &mut Framed<TcpStream, WireCodec>,
    ) -> error::PeerConnectionResult<()> {
        loop {
            match framed.next().await {
                Some(Ok(WireItem::HandshakePartial { info_hash })) => {
                    if info_hash != self.info_hash {
                        warn!("{}: handshake failed, unknown info_hash", self.peer_addr());
                        return Err(error::PeerConnectionError::InfoHashMismatch);
                    }
                    framed
                        .send(WireItem::Handshake(self.our_handshake()))
                        .await?;
                    debug!("Inbound handshake info_hash verified, replied with our handshake");
                }
                Some(Ok(WireItem::Handshake(handshake))) => {
                    let addr = framed.get_ref().peer_addr()?;
                    self.peer = Some(Peer {
                        peer_id: Some(handshake.peer_id),
                        ip: addr.ip().to_string(),
                        port: addr.port(),
                    });
                    debug!("Inbound handshake complete with {}", addr);
                    return Ok(());
                }
                _ => {
                    warn!(
                        "{}: handshake failed, unexpected message or connection closed",
                        self.peer_addr()
                    );
                    return Err(error::PeerConnectionError::UnexpectedMessage);
                }
            }
        }
    }
}
