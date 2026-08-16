use crate::{
    peer_explorer::{
        Peer,
        channel::{PeerExplorerChannelMessage, PeerExplorerChannelReceiver},
    },
    piece_manager::channel::PieceManagerChannelSender,
};
use channels::PeerManagerChannelMessage;

pub mod channels;
pub mod peer_selection_strategy;

const MAX_PEERS: usize = 10;

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
    ) {
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
                        tokio::spawn(peer_conncection(
                            attempt.peer,
                            peer_manager_channel_sender.clone(),
                        ));
                    }
                }
            }
        });
    }
}

async fn peer_conncection(
    peer: Peer,
    peer_manager_channel_sender: channels::PeerManagerChannelSender,
) {
    println!("Connecting to peer {} {}", peer.ip, peer.port);
    peer_manager_channel_sender
        .send(PeerManagerChannelMessage::Closing(peer))
        .await
        .unwrap();
}
