use super::channels::{IncomingChannelReceiver, OutgoingChannelSender};
use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    peer_manager::channels::{PeerManagerChannelMessage, PeerManagerChannelSender},
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
    wire_protocol::{Bitfield, Handshake, Message, WireCodec, WireItem},
};

use tracing::{debug, error, warn};

pub struct RequestManager {
    pub peer: Option<Peer>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub peer_bitfield: Bitfield,
    pub requested_pieces: Vec<u32>,
    pub peer_manager_channel_sender: Option<PeerManagerChannelSender>,
    pub piece_manager_channel_sender: PieceManagerChannelSender,
    pub incoming_channel_receiver: IncomingChannelReceiver,
    pub outgoing_channel_sender: OutgoingChannelSender,
}

impl RequestManager {
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
            requested_pieces: Vec::new(),
            peer_manager_channel_sender,
            piece_manager_channel_sender,
            incoming_channel_receiver,
            outgoing_channel_sender,
        }
    }

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
        Ok(())
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
}
