use crate::{
    peer_connection::{PeerConnection, error::PeerConnectionError},
    peer_explorer::channel::{PeerExplorerChannelMessage, PeerExplorerChannelReceiver},
    piece_manager::channel::PieceManagerChannelSender,
};
use channels::PeerManagerChannelMessage;
use tokio::task::JoinHandle;
use tracing::warn;

pub mod channels;
pub mod peer_selection_strategy;

const MAX_PEERS: usize = 50;

pub struct PeerManager<S>
where
    S: peer_selection_strategy::PeerSelectionStrategy + Sync + Send + 'static,
{
    peer_slection_strategy: S,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
}

impl<S> PeerManager<S>
where
    S: peer_selection_strategy::PeerSelectionStrategy + Sync + Send + 'static,
{
    pub fn new(peer_slection_strategy: S, info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Self {
        Self {
            peer_slection_strategy,
            info_hash: *info_hash,
            peer_id: *peer_id,
        }
    }

    pub async fn start(
        mut self,
        mut peer_explorer_channel_receiver: PeerExplorerChannelReceiver,
        piece_manager_channel_sender: PieceManagerChannelSender,
    ) -> JoinHandle<()> {
        let (peer_manager_channel_sender, mut peer_manager_channel_receiver) =
            channels::new_peer_manager_channel();

        tokio::spawn(async move {
            let mut active: usize = 0;

            loop {
                tokio::select! {
                    msg = peer_manager_channel_receiver.recv() => {
                        match msg {
                            Some(PeerManagerChannelMessage::Closing(peer)) => {
                                active -= 1;
                                self.peer_slection_strategy.push(peer, true);
                            }
                            None => break,
                        }
                    }
                    Some(msg) = peer_explorer_channel_receiver.recv() => {
                        match msg {
                            PeerExplorerChannelMessage::PeerFound(peer) => {
                                self.peer_slection_strategy.push(peer, false);
                            }
                        }
                    }
                    Some(attempt) = self.peer_slection_strategy.pop(),
                        if active < MAX_PEERS && self.peer_slection_strategy.peek().is_some() =>
                    {
                        active += 1;

                        let peer_manager_channel_sender = peer_manager_channel_sender.clone();
                        let piece_manager_channel_sender = piece_manager_channel_sender.clone();
                        let info_hash = self.info_hash;
                        let peer_id = self.peer_id;

                        tokio::spawn(async move {
                            let connection_sender = peer_manager_channel_sender.clone();
                            match PeerConnection::connect(
                                attempt.peer,
                                connection_sender,
                                piece_manager_channel_sender,
                                &info_hash,
                                &peer_id,
                            )
                            .await
                            {
                                Ok(peer_connection) => peer_connection.start().await,
                                Err(e) => {
                                    warn!("{}", e);
                                    // Only `ConnectFailed` hands the peer back; without it
                                    // there is nothing to requeue.
                                    if let PeerConnectionError::ConnectFailed { peer, .. } = e {
                                        let _ = peer_manager_channel_sender
                                            .send(PeerManagerChannelMessage::Closing(*peer))
                                            .await;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        })
    }
}
