use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::peer_explorer::Peer;

const CHANNEL_SIZE: usize = 256;

#[derive(Debug)]
pub enum PeerManagerChannelMessage {
    Closing(Peer),
}

pub type PeerManagerChannelSender = Sender<PeerManagerChannelMessage>;
pub type PeerManagerChannelReceiver = Receiver<PeerManagerChannelMessage>;

pub fn new_peer_manager_channel() -> (PeerManagerChannelSender, PeerManagerChannelReceiver) {
    channel(CHANNEL_SIZE)
}
