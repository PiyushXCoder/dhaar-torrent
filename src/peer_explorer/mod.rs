use std::collections::HashSet;

use tokio::task::JoinHandle;
use tracing::error;

use crate::error::Result;

pub mod channel;
pub mod tracker;

pub struct PeerExplorer {
    pub peer_explorer_channel_sender: channel::PeerExplorerChannelSender,
    pub peer_sources: Vec<Box<dyn PeerSource>>,
}

impl PeerExplorer {
    pub fn new(
        peer_explorer_channel_sender: channel::PeerExplorerChannelSender,
        peer_sources: Vec<Box<dyn PeerSource>>,
    ) -> PeerExplorer {
        PeerExplorer {
            peer_explorer_channel_sender,
            peer_sources,
        }
    }

    pub async fn start(&mut self) {
        let (peer_source_channel_sender, mut peer_source_channel_receiver) =
            channel::new_peer_source_channel();

        for peer_source in self.peer_sources.iter_mut() {
            if let Err(e) = peer_source.start(peer_source_channel_sender.clone()).await {
                error!("Failed to start peer source: {}", e);
            }
        }

        let peer_explorer_channel_sender = self.peer_explorer_channel_sender.clone();
        let mut dedup = HashSet::new();
        tokio::spawn(async move {
            while let Some(message) = peer_source_channel_receiver.recv().await {
                match message {
                    channel::PeerSourceChannelMessage::PeerFound(peer) => {
                        if dedup.insert(peer.clone()) {
                            peer_explorer_channel_sender
                                .send(channel::PeerExplorerChannelMessage::PeerFound(peer))
                                .await
                                .expect("Failed to send peer from Peer Explorer");
                        }
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait]
pub trait PeerSource {
    async fn start(
        &self,
        peer_source_channel_sender: channel::PeerSourceChannelSender,
    ) -> Result<JoinHandle<()>>;
}

#[derive(Debug, Clone, Eq)]
pub struct Peer {
    pub peer_id: Option<[u8; 20]>,
    pub ip: String,
    pub port: u16,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}

impl std::hash::Hash for Peer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.port.hash(state);
    }
}
