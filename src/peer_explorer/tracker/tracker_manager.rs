use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::error::Result;
use crate::peer_explorer::PeerSource;
use crate::peer_explorer::channel::{PeerSourceChannelMessage, PeerSourceChannelSender};

use super::tcp_tracker_client::TcpTrackerClient;
use super::tracker_client::TrackerClient;
use super::tracker_client_messages::TrackerAnnounceQuery;

pub struct TrackerManager {
    announce_urls: Vec<String>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
}

impl TrackerManager {
    pub fn new(announce_urls: Vec<String>, info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Self {
        Self {
            announce_urls,
            info_hash: *info_hash,
            peer_id: *peer_id,
        }
    }
}

#[async_trait::async_trait]
impl PeerSource for TrackerManager {
    async fn start(
        &self,
        peer_source_channel_sender: PeerSourceChannelSender,
    ) -> Result<JoinHandle<()>> {
        let mut scheduler_heap = BinaryHeap::new();

        let trackers_clients: Vec<Tracker> = self
            .announce_urls
            .iter()
            .map(|announce_url| Tracker {
                // TODO: support other trackers
                tracker_client: Arc::new(TcpTrackerClient::new(announce_url)),
                announce_url: announce_url.clone(),
                next_instance: Instant::now(),
                failure_count: 0,
            })
            .collect();

        for tracker in trackers_clients {
            scheduler_heap.push(Reverse(tracker));
        }

        let query = TrackerAnnounceQuery::new(&self.info_hash, &self.peer_id);
        let join_handle = tokio::spawn(async move {
            announce_tracker(&mut scheduler_heap, &query, &peer_source_channel_sender).await;
        });
        Ok(join_handle)
    }
}

async fn announce_tracker(
    scheduler_heap: &mut BinaryHeap<Reverse<Tracker>>,
    query: &TrackerAnnounceQuery,
    peer_explorer_channel_sender: &PeerSourceChannelSender,
) {
    info!("Starting to announce");
    loop {
        let mut next = match scheduler_heap.pop() {
            Some(tracker) => tracker,
            None => break,
        }
        .0;

        debug!(
            "Next tracker to announce: {} (failure_count={}, heap_len={})",
            next.announce_url,
            next.failure_count,
            scheduler_heap.len()
        );

        tokio::time::sleep_until(next.next_instance).await;
        debug!("Announcing to tracker: {}", next.announce_url);
        let response = match next.tracker_client.announce(query).await {
            Ok(response) => response,
            Err(e) => {
                next.failure_count += 1;
                next.next_instance = Instant::now() + Duration::from_secs(5);
                warn!(
                    "Error announcing to tracker {}: {} (failure_count={})",
                    next.announce_url, e, next.failure_count
                );
                scheduler_heap.push(Reverse(next));
                continue;
            }
        };
        let min_interval = response.base.min_interval.unwrap_or(60);
        let peers = response.peers.unwrap_or(vec![]);
        info!(
            "Tracker {} responded: {} peers discovered, min_interval={}s",
            next.announce_url,
            peers.len(),
            min_interval
        );
        for peer in peers {
            peer_explorer_channel_sender
                .send(PeerSourceChannelMessage::PeerFound(peer.into()))
                .await
                .unwrap();
        }
        next.failure_count = 0;
        next.next_instance = Instant::now() + Duration::from_secs(min_interval as u64);
        debug!(
            "Tracker {} next announce at {:?}",
            next.announce_url, next.next_instance
        );
        scheduler_heap.push(Reverse(next));
    }
}

#[derive(Clone)]
struct Tracker {
    tracker_client: Arc<dyn TrackerClient + Sync + Send>,
    announce_url: String,
    next_instance: Instant,
    failure_count: u32,
}

impl Ord for Tracker {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.next_instance.cmp(&other.next_instance)
    }
}
impl PartialOrd for Tracker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for Tracker {}
impl PartialEq for Tracker {
    fn eq(&self, other: &Self) -> bool {
        self.next_instance == other.next_instance
    }
}
