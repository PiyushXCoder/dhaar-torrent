use crate::{
    peer_explorer::Peer,
    peer_manager::channels::{PeerManagerChannelMessage, PeerManagerChannelSender},
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
    wire_protocol::{Bitfield, Handshake, Message, WireCodec, WireItem},
};

use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, error};

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
}

const BLOCK_SIZE: u64 = 16384;

impl PeerConnection {
    pub async fn connect(
        peer: Peer,
        peer_manager_channel_sender: PeerManagerChannelSender,
        piece_manager_channel_sender: PieceManagerChannelSender,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
    ) -> Self {
        let stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .await
            .unwrap();

        PeerConnection {
            peer: Some(peer),
            peer_manager_channel_sender: Some(peer_manager_channel_sender),
            piece_manager_channel_sender: piece_manager_channel_sender,
            stream: Some(stream),
            info_hash: *info_hash,
            peer_id: *peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
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
            piece_manager_channel_sender: piece_manager_channel_sender,
            stream: Some(stream),
            info_hash: *info_hash,
            peer_id: *peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }
    pub async fn start(mut self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(&mut self) -> Option<()> {
        let mut framed = Framed::new(self.stream.take().unwrap(), WireCodec::new());
        if !self.handshake(&mut framed).await {
            return None;
        }

        let our_bitfield = self
            .piece_manager_request(|tx| PieceManagerMessage::Bitfield {
                response_sender: tx,
            })
            .await?;
        framed
            .send(WireItem::Message(Message::Bitfield(our_bitfield)))
            .await
            .unwrap();

        let mut peer_bitfield: Option<Bitfield> = None;
        let mut current_piece: Option<u64> = None;

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
                        .await
                        .unwrap();
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
                        let block_index = begin as u64 / BLOCK_SIZE;
                        debug!(
                            "{}: peer requested piece {} block {}",
                            self.peer_addr(),
                            index,
                            block_index
                        );
                        let block = self
                            .piece_manager_request(|tx| PieceManagerMessage::ReadBlock {
                                piece_index: index as u64,
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
                            .await
                            .unwrap();
                    }
                }
                Some(Ok(WireItem::Message(Message::Piece {
                    index,
                    begin,
                    block,
                }))) => {
                    let block_index = begin as u64 / BLOCK_SIZE;
                    debug!(
                        "{}: received piece {} block {} ({} bytes)",
                        self.peer_addr(),
                        index,
                        block_index,
                        block.len()
                    );
                    self.piece_manager_notify(PieceManagerMessage::ReceiveBlock {
                        piece_index: index as u64,
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
                Some(Err(e)) => {
                    error!("Failed to receive message: {}", e);
                    self.close().await;
                    return None;
                }
                _ => {}
            }
        }
        #[allow(unreachable_code)]
        self.close().await;
        Some(())
    }

    pub async fn close(&mut self) {
        debug!("{}: closing connection", self.peer_addr());
        if let Some((peer, peer_manager_channel_sender)) = self
            .peer
            .take()
            .zip(self.peer_manager_channel_sender.take())
        {
            if let Err(e) = peer_manager_channel_sender
                .send(PeerManagerChannelMessage::Closing(peer))
                .await
            {
                error!("Failed to close peer connection: {}", e);
            }
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
        current_piece: &mut Option<u64>,
        peer_bitfield: Option<&Bitfield>,
    ) -> Option<()> {
        if !self.am_interested || self.peer_choking {
            return Some(());
        }

        loop {
            let piece_index = match *current_piece {
                Some(piece_index) => piece_index,
                None => {
                    let Some(bitfield) = peer_bitfield else {
                        return Some(());
                    };
                    let Some(piece_index) = self
                        .piece_manager_request(|tx| PieceManagerMessage::LockNextPiece {
                            bitfield: bitfield.to_owned(),
                            response_sender: tx,
                        })
                        .await?
                    else {
                        return Some(());
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

            let begin = block_index as u32 * BLOCK_SIZE as u32;
            debug!(
                "{}: requesting piece {} block {}",
                self.peer_addr(),
                piece_index,
                block_index
            );
            framed
                .send(WireItem::Message(Message::Request {
                    index: piece_index as u32,
                    begin,
                    length: BLOCK_SIZE as u32,
                }))
                .await
                .unwrap();
            return Some(());
        }
    }

    async fn piece_manager_notify(&mut self, message: PieceManagerMessage) -> Option<()> {
        if let Err(e) = self.piece_manager_channel_sender.send(message).await {
            error!("Failed to send message to piece manager: {}", e);
            self.close().await;
            return None;
        }
        Some(())
    }

    async fn piece_manager_request<T>(
        &mut self,
        build: impl FnOnce(oneshot::Sender<T>) -> PieceManagerMessage,
    ) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.piece_manager_channel_sender.send(build(tx)).await {
            error!("Failed to send request to piece manager: {}", e);
            self.close().await;
            return None;
        }
        match rx.await {
            Ok(value) => Some(value),
            Err(e) => {
                error!("Piece manager dropped response channel: {}", e);
                self.close().await;
                None
            }
        }
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
    pub async fn handshake(&mut self, framed: &mut Framed<TcpStream, WireCodec>) -> bool {
        if self.peer.is_some() {
            self.handshake_outbound(framed).await
        } else {
            self.handshake_inbound(framed).await
        }
    }

    async fn handshake_outbound(&mut self, framed: &mut Framed<TcpStream, WireCodec>) -> bool {
        if let Err(e) = framed.send(WireItem::Handshake(self.our_handshake())).await {
            error!("Failed to send handshake: {}", e);
            self.close().await;
            return false;
        }

        loop {
            match framed.next().await {
                Some(Ok(WireItem::HandshakePartial { info_hash })) => {
                    if info_hash != self.info_hash {
                        error!("Handshake failed: info_hash mismatch");
                        self.close().await;
                        return false;
                    }
                }
                Some(Ok(WireItem::Handshake(handshake))) => {
                    let Some(peer) = self.peer.as_mut() else {
                        error!("Handshake failed: outbound connection missing peer");
                        self.close().await;
                        return false;
                    };
                    peer.peer_id = Some(handshake.peer_id);
                    debug!("Outbound handshake complete with {}:{}", peer.ip, peer.port);
                    return true;
                }
                _ => {
                    error!("Handshake failed: unexpected message or connection closed");
                    self.close().await;
                    return false;
                }
            }
        }
    }

    async fn handshake_inbound(&mut self, framed: &mut Framed<TcpStream, WireCodec>) -> bool {
        loop {
            match framed.next().await {
                Some(Ok(WireItem::HandshakePartial { info_hash })) => {
                    if info_hash != self.info_hash {
                        error!("Handshake failed: unknown info_hash");
                        self.close().await;
                        return false;
                    }
                    if let Err(e) = framed.send(WireItem::Handshake(self.our_handshake())).await {
                        error!("Failed to send handshake: {}", e);
                        self.close().await;
                        return false;
                    }
                    debug!("Inbound handshake info_hash verified, replied with our handshake");
                }
                Some(Ok(WireItem::Handshake(handshake))) => {
                    let addr = match framed.get_ref().peer_addr() {
                        Ok(addr) => addr,
                        Err(e) => {
                            error!("Failed to get peer address: {}", e);
                            self.close().await;
                            return false;
                        }
                    };
                    self.peer = Some(Peer {
                        peer_id: Some(handshake.peer_id),
                        ip: addr.ip().to_string(),
                        port: addr.port(),
                    });
                    debug!("Inbound handshake complete with {}", addr);
                    return true;
                }
                _ => {
                    error!("Handshake failed: unexpected message or connection closed");
                    self.close().await;
                    return false;
                }
            }
        }
    }
}
