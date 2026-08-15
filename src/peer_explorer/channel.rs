use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::peer_explorer::Peer;

const CHANNEL_SIZE: usize = 256;

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
    channel(CHANNEL_SIZE)
}

#[derive(Debug)]
pub enum PeerSourceChannelMessage {
    PeerFound(Peer),
}

pub type PeerSourceChannelSender = Sender<PeerSourceChannelMessage>;
pub type PeerSourceChannelReceiver = Receiver<PeerSourceChannelMessage>;

pub fn new_peer_source_channel() -> (
    Sender<PeerSourceChannelMessage>,
    Receiver<PeerSourceChannelMessage>,
) {
    channel(CHANNEL_SIZE)
}
