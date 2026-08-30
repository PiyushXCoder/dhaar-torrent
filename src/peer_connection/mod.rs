use std::net::SocketAddr;

use std::sync::Arc;

use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
    status::DownloadStats,
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
    pub stats: Arc<DownloadStats>,
    pub peer: Option<Peer>,
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
        piece_manager_channel_sender: PieceManagerChannelSender,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
        stats: Arc<DownloadStats>,
    ) -> PeerConnectionResult<Self> {
        let stream = TcpStream::connect(peer.address).await.map_err(|source| {
            PeerConnectionError::ConnectFailed {
                peer: Box::new(peer),
                source,
            }
        })?;

        Ok(PeerConnection {
            stats,
            peer: Some(peer),
            piece_manager_channel_sender,
            stream: Some(stream),
            info_hash: *info_hash,
            peer_id: *peer_id,
        })
    }

    pub async fn from_stream(
        stream: TcpStream,
        piece_manager_channel_sender: PieceManagerChannelSender,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
        stats: Arc<DownloadStats>,
    ) -> Self {
        PeerConnection {
            stats,
            peer: None,
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
        close(&self.peer);
    }

    async fn run(&mut self) -> PeerConnectionResult<()> {
        let mut framed = Framed::new(self.stream.take().unwrap(), WireCodec::new());
        self.handshake(&mut framed).await?;
        // The subscription comes back with the bitfield rather than being
        // taken later: anything completing in between would be missing from
        // both, and the peer would never hear about it.
        let snapshot = piece_manager_request(&mut self.piece_manager_channel_sender, |tx| {
            PieceManagerMessage::GetBitfield {
                response_sender: tx,
            }
        })
        .await?;

        framed
            .send(WireItem::Message(Message::Bitfield(snapshot.bitfield)))
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
            self.piece_manager_channel_sender.clone(),
            incoming_receiver,
            outgoing_sender,
            snapshot.events,
            self.stats.clone(),
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

/// Marks the end of a connection.
///
/// Nothing is reported anywhere: the peer manager watches the connection's
/// task instead of waiting to be told, because a task that panics or is
/// dropped would never get as far as telling it.
pub fn close(peer: &Option<Peer>) {
    debug!("{}: closing connection", peer_addr(peer));
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
