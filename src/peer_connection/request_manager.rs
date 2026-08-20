use std::pin::Pin;

use super::channels::{IncomingChannelReceiver, OutgoingChannelSender};
use super::{close, peer_addr, piece_manager_request};
use crate::{
    peer_connection::error::{PeerConnectionError, PeerConnectionResult},
    peer_explorer::Peer,
    peer_manager::channels::{PeerManagerChannelMessage, PeerManagerChannelSender},
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
    wire_protocol::{Bitfield, Handshake, Message, WireCodec, WireItem},
};

use tokio::{net::TcpStream, select, sync::oneshot, task::JoinSet, time};
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
                    debug!("{}: connection ended", peer_addr(&self.peer));
                }
                Err(e) => warn!("{}: connection ended: {}", peer_addr(&self.peer), e),
            }
            close(&mut self.peer_manager_channel_sender, &mut self.peer).await;
        });
    }

    async fn run(&mut self) -> PeerConnectionResult<()> {
        let mut timeout: Option<Pin<Box<tokio::time::Sleep>>> = None;

        loop {
            select! {
                _ = async {
                    match &mut timeout {
                        Some(timeout) => timeout.await,
                        None => std::future::pending().await,
                    }
                } => {
                    return Err(PeerConnectionError::PeerDisconnected);
                },
                item = self.incoming_channel_receiver.recv() => {
                    let Some(item) = item else {
                        return Err(PeerConnectionError::PeerDisconnected);
                    };
                    timeout = self.handle_incoming_message(item).await?;
                },
            }
        }
    }

    async fn handle_incoming_message(
        &mut self,
        item: WireItem,
    ) -> PeerConnectionResult<Option<Pin<Box<tokio::time::Sleep>>>> {
        match item {
            WireItem::Message(Message::Choke) => {}
            WireItem::Message(Message::Unchoke) => {}
            WireItem::Message(Message::Interested) => {}
            WireItem::Message(Message::NotInterested) => {}
            WireItem::Message(Message::Have(_index)) => {}
            WireItem::Message(Message::Request {
                index: _,
                begin: _,
                length: _,
            }) => {}
            WireItem::Message(Message::Piece {
                index: _,
                begin: _,
                block: _,
            }) => {}
            WireItem::Message(Message::Cancel {
                index: _,
                begin: _,
                length: _,
            }) => {}
            WireItem::Message(Message::Port(_port)) => {}
            _ => {}
        }
        Ok(None)
    }
}
