use std::{collections::HashSet, net::SocketAddr};

use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::error::Result;

pub mod channel;
pub mod tracker;

pub struct PeerExplorer {
    pub peer_sources: Vec<Box<dyn PeerSource + Send>>,
}

impl PeerExplorer {
    pub fn new(peer_sources: Vec<Box<dyn PeerSource + Send>>) -> PeerExplorer {
        PeerExplorer { peer_sources }
    }

    pub async fn start(mut self, peer_explorer_channel_sender: channel::PeerExplorerChannelSender) {
        let (peer_source_channel_sender, mut peer_source_channel_receiver) =
            channel::new_peer_source_channel();

        for peer_source in self.peer_sources.iter_mut() {
            if let Err(e) = peer_source.start(peer_source_channel_sender.clone()).await {
                error!("Failed to start peer source: {}", e);
            }
        }

        let mut dedup = HashSet::new();
        while let Some(message) = peer_source_channel_receiver.recv().await {
            match message {
                channel::PeerSourceChannelMessage::PeerFound(peer) => {
                    if dedup.insert(peer.clone()) {
                        debug!("New unique peer discovered, {} total so far", dedup.len());
                        peer_explorer_channel_sender
                            .send(channel::PeerExplorerChannelMessage::PeerFound(peer))
                            .await
                            .expect("Failed to send peer from Peer Explorer");
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
pub trait PeerSource {
    async fn start(
        &self,
        peer_source_channel_sender: channel::PeerSourceChannelSender,
    ) -> Result<JoinHandle<()>>;
}

#[derive(Debug, Clone, Copy, Eq)]
pub struct Peer {
    pub peer_id: Option<[u8; 20]>,
    pub address: SocketAddr,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl std::hash::Hash for Peer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}
