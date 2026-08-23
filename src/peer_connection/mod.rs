use std::net::SocketAddr;

use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    peer_manager::channels::{PeerManagerChannelMessage, PeerManagerChannelSender},
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
    wire_protocol::{Bitfield, Handshake, Message, WireCodec, WireItem},
};

use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, select, sync::oneshot, task::JoinSet, time};
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

pub mod channels;
pub mod error;
pub mod request_manager;

pub struct PeerConnection {
    pub peer: Option<Peer>,
    pub peer_manager_channel_sender: Option<PeerManagerChannelSender>,
    pub piece_manager_channel_sender: PieceManagerChannelSender,
    pub stream: Option<TcpStream>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

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
        let stream = TcpStream::connect(peer.address).await.map_err(|source| {
            PeerConnectionError::ConnectFailed {
                peer: Box::new(peer.clone()),
                source,
            }
        })?;

        Ok(PeerConnection {
            peer: Some(peer),
            peer_manager_channel_sender: Some(peer_manager_channel_sender),
            piece_manager_channel_sender,
            stream: Some(stream),
            info_hash: *info_hash,
            peer_id: *peer_id,
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
        }
    }
    /// Drives the connection to completion. `run` never closes the connection
    /// itself, it just reports why it stopped; teardown happens here so every
    /// error path funnels through exactly one `close`.
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
        let mut framed = Framed::new(self.stream.take().unwrap(), WireCodec::new());
        self.handshake(&mut framed).await?;
        let our_bitfield = piece_manager_request(&mut self.piece_manager_channel_sender, |tx| {
            PieceManagerMessage::Bitfield {
                response_sender: tx,
            }
        })
        .await?;

        framed
            .send(WireItem::Message(Message::Bitfield(our_bitfield)))
            .await?;

        let piece_length = piece_manager_request(&mut self.piece_manager_channel_sender, |tx| {
            PieceManagerMessage::PieceLength {
                response_sender: tx,
            }
        })
        .await?;

        let peer_bitfield: Bitfield;
        select! {
            _ = time::sleep(time::Duration::from_secs(30)) => {
                return Err(PeerConnectionError::Timeout);
            },
            item = framed.next() => {
                match item {
                    Some(Ok(WireItem::Message(Message::Bitfield(bitfield)))) => {
                        peer_bitfield = bitfield;
                    }
                    _ => {
                        return Err(PeerConnectionError::PeerDisconnected);
                    }
                }
            }
        }

        let (incoming_sender, incoming_receiver) = channels::new_incoming_channel();
        let (outgoing_sender, mut outgoing_receiver) = channels::new_outgoing_channel();

        let (mut sink, mut stream) = framed.split();

        let mut joinset: JoinSet<()> = JoinSet::new();

        joinset.spawn(async move {
            while let Some(item) = outgoing_receiver.recv().await {
                if let Err(e) = sink.send(item).await {
                    warn!("wire write failed: {e}");
                    break;
                }
            }
        });

        joinset.spawn(async move {
            loop {
                match stream.next().await {
                    Some(Ok(item)) => {
                        // Receiver gone means the request manager already quit,
                        // so there is nobody left to read what we decode.
                        if incoming_sender.send(item).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        warn!("wire decode failed: {e}");
                        break;
                    }
                    None => break,
                }
            }
        });

        let request_manager = request_manager::RequestManager::new(
            self.peer.take(),
            self.info_hash,
            self.peer_id,
            peer_bitfield,
            piece_length,
            self.peer_manager_channel_sender.clone(),
            self.piece_manager_channel_sender.clone(),
            incoming_receiver,
            outgoing_sender,
        );

        joinset.spawn(async move {
            request_manager.start().await;
        });

        joinset.join_all().await;

        Ok(())
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
            select! {
                _ = time::sleep(time::Duration::from_secs(30)) => {
                    return Err(PeerConnectionError::Timeout);
                },
                item = framed.next() => {
                    match item {
                        Some(Ok(WireItem::HandshakePartial { info_hash })) => {
                            if info_hash != self.info_hash {
                                warn!(
                                    "{}: handshake failed, info_hash mismatch",
                                    peer_addr(&self.peer)
                                );
                                return Err(error::PeerConnectionError::InfoHashMismatch);
                            }
                        }
                        Some(Ok(WireItem::Handshake(handshake))) => {
                            let Some(peer) = self.peer.as_mut() else {
                                error!("Handshake failed: outbound connection missing peer");
                                return Err(error::PeerConnectionError::PeerNotFound);
                            };
                            peer.peer_id = Some(handshake.peer_id);
                            debug!("Outbound handshake complete with {}", peer.address);
                            return Ok(());
                        }
                        _ => {
                            warn!(
                                "{}: handshake failed, unexpected message or connection closed",
                                peer_addr(&self.peer)
                            );
                            return Err(error::PeerConnectionError::UnexpectedMessage);
                        }
                    }
                },
            }
        }
    }

    async fn handshake_inbound(
        &mut self,
        framed: &mut Framed<TcpStream, WireCodec>,
    ) -> error::PeerConnectionResult<()> {
        loop {
            select! {
                _ = time::sleep(time::Duration::from_secs(30)) => {
                    return Err(PeerConnectionError::Timeout);
                },
                item = framed.next() => {
                    match item {
                        Some(Ok(WireItem::HandshakePartial { info_hash })) => {
                            if info_hash != self.info_hash {
                                warn!(
                                    "{}: handshake failed, unknown info_hash",
                                    peer_addr(&self.peer)
                                );
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
                                address: SocketAddr::new(addr.ip(), addr.port()),
                            });
                            debug!("Inbound handshake complete with {}", addr);
                            return Ok(());
                        }
                        _ => {
                            warn!(
                                "{}: handshake failed, unexpected message or connection closed",
                                peer_addr(&self.peer)
                            );
                            return Err(error::PeerConnectionError::UnexpectedMessage);
                        }
                    }
                }
            }
        }
    }
}

pub async fn close(
    peer_manager_channel_sender: &mut Option<PeerManagerChannelSender>,
    peer: &mut Option<Peer>,
) {
    debug!("{}: closing connection", peer_addr(peer));
    if let Some((peer, peer_manager_channel_sender)) =
        peer.take().zip(peer_manager_channel_sender.take())
        && let Err(e) = peer_manager_channel_sender
            .send(PeerManagerChannelMessage::Closing(peer))
            .await
    {
        error!("Failed to close peer connection: {}", e);
    }
}

fn peer_addr(peer: &Option<Peer>) -> String {
    match peer.as_ref() {
        Some(peer) => format!("{}", peer.address),
        None => "unknown".to_string(),
    }
}

async fn piece_manager_request<T>(
    piece_manager_channel_sender: &mut PieceManagerChannelSender,
    build: impl FnOnce(oneshot::Sender<T>) -> PieceManagerMessage,
) -> error::PeerConnectionResult<T> {
    let (tx, rx) = oneshot::channel();
    piece_manager_channel_sender.send(build(tx)).await?;
    Ok(rx.await?)
}
