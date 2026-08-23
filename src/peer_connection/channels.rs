use crate::wire_protocol::WireItem;
use tokio::sync::mpsc::{Receiver, Sender, channel};

pub const CHANNEL_SIZE: usize = 256;

pub type IncomingChannelSender = Sender<WireItem>;
pub type IncomingChannelReceiver = Receiver<WireItem>;

pub fn new_incoming_channel() -> (Sender<WireItem>, Receiver<WireItem>) {
    channel(CHANNEL_SIZE)
}

pub type OutgoingChannelSender = Sender<WireItem>;
pub type OutgoingChannelReceiver = Receiver<WireItem>;

pub fn new_outgoing_channel() -> (Sender<WireItem>, Receiver<WireItem>) {
    channel(CHANNEL_SIZE)
}
