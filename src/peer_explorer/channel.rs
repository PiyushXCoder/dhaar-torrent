use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::peer_explorer::tracker::tracker_client_messages::Peer;

const PEER_EXPLORER_CHANNEL_SIZE: usize = 256;

#[derive(Debug)]
pub enum PeerExplorerChannelMessage {
    PeerFound(Peer),
}

pub type PeerExplorerChannelSender = Sender<PeerExplorerChannelMessage>;
pub type PeerExplorerChannelReceiver = Receiver<PeerExplorerChannelMessage>;
pub fn new_peer_explorer_channel() -> (
    Sender<PeerExplorerChannelMessage>,
    Receiver<PeerExplorerChannelMessage>,
) {
    channel(PEER_EXPLORER_CHANNEL_SIZE)
}
