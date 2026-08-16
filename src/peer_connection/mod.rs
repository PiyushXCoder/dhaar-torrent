use crate::{
    peer_explorer::Peer,
    peer_manager::channels::{PeerManagerChannelMessage, PeerManagerChannelSender},
    piece_manager::channel::PieceManagerChannelSender,
    wire_protocol::{Handshake, WireCodec, WireItem},
};

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::error;

pub struct PeerConnection {
    pub peer: Option<Peer>,
    pub peer_manager_channel_sender: Option<PeerManagerChannelSender>,
    pub piece_manager_channel_sender: Option<PieceManagerChannelSender>,
    pub stream: Option<TcpStream>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
}

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
            piece_manager_channel_sender: Some(piece_manager_channel_sender),
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
            piece_manager_channel_sender: Some(piece_manager_channel_sender),
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
        let mut framed = Framed::new(self.stream.take().unwrap(), WireCodec::new());
        self.handshake(&mut framed).await;

        tokio::spawn(async move {
            loop {
                todo!()
            }
            #[allow(unreachable_code)]
            self.close().await;
        });
    }

    pub async fn close(&mut self) {
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

        match framed.next().await {
            Some(Ok(WireItem::HandshakePartial { info_hash })) if info_hash == self.info_hash => {}
            _ => {
                error!("Handshake failed: info_hash mismatch or connection closed");
                self.close().await;
                return false;
            }
        }

        match framed.next().await {
            Some(Ok(WireItem::Handshake(handshake))) => {
                if let Some(peer) = self.peer.as_mut() {
                    peer.peer_id = Some(handshake.peer_id);
                }
                true
            }
            _ => {
                error!("Handshake failed: did not receive full handshake");
                self.close().await;
                false
            }
        }
    }

    async fn handshake_inbound(&mut self, framed: &mut Framed<TcpStream, WireCodec>) -> bool {
        match framed.next().await {
            Some(Ok(WireItem::HandshakePartial { info_hash })) if info_hash == self.info_hash => {}
            _ => {
                error!("Handshake failed: unknown info_hash or connection closed");
                self.close().await;
                return false;
            }
        }

        if let Err(e) = framed.send(WireItem::Handshake(self.our_handshake())).await {
            error!("Failed to send handshake: {}", e);
            self.close().await;
            return false;
        }

        match framed.next().await {
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
                true
            }
            _ => {
                error!("Handshake failed: did not receive full handshake");
                self.close().await;
                false
            }
        }
    }
}
