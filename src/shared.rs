use crate::tracker::tracker_client_messages::Peer;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Default)]
pub struct PeerPool {
    pub peers: Arc<Mutex<HashSet<Peer>>>,
}
