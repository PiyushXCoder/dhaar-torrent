use crate::error::Result;
use crate::peer::Peer;
use crate::piece_bag::PieceBag;
use crate::torrent::TorrentEvent;
use crate::tracker::Peer as TrackerPeer;
use rand::seq::IndexedRandom;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::warn;

const MAX_PEERS: usize = 20;

pub enum OrchestratePeersMessage {
    PeerDying,
}

pub struct SwarmManager {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
    pub swarm: Arc<RwLock<Vec<Arc<Mutex<Peer>>>>>,
    pub piece_bag: Arc<PieceBag>,
    pub torrent_event_tx: mpsc::Sender<TorrentEvent>,
    pub seen: HashSet<TrackerPeer>,
    pub semaphore: Arc<Semaphore>,
    pub orchestrate_rx: mpsc::Receiver<(TrackerPeer, OrchestratePeersMessage)>,
    pub orchestrate_tx: mpsc::Sender<(TrackerPeer, OrchestratePeersMessage)>,
}

impl SwarmManager {
    pub fn new(
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
        swarm: Arc<RwLock<Vec<Arc<Mutex<Peer>>>>>,
        piece_bag: Arc<PieceBag>,
        torrent_event_tx: mpsc::Sender<TorrentEvent>,
    ) -> Self {
        let (orchestrate_tx, orchestrate_rx) = mpsc::channel(MAX_PEERS);
        Self {
            info_hash,
            peer_id,
            available_peers,
            swarm,
            piece_bag,
            torrent_event_tx,
            seen: HashSet::new(),
            semaphore: Arc::new(Semaphore::new(MAX_PEERS)),
            orchestrate_rx,
            orchestrate_tx,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        loop {
            let tracker_peer = match self.get_random_peer().await {
                Some(v) => v,
                None => {
                    // No unseen peers — wait for a peer to die (immediate retry) or
                    // timeout (tracker may have added new peers)
                    tokio::select! {
                        Some((dead_peer, _)) = self.orchestrate_rx.recv() => {
                            self.seen.remove(&dead_peer);
                        }
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                    }
                    continue;
                }
            };

            // Acquire permit BEFORE committing to this peer — blocks here when at MAX_PEERS
            let permit = self.semaphore.clone().acquire_owned().await?;
            self.seen.insert(tracker_peer.clone());

            let peer = Arc::new(Mutex::new(Peer::new(
                self.piece_bag.clone(),
                tracker_peer.clone(),
                self.info_hash,
                self.peer_id,
            )));
            self.swarm.write().await.push(peer.clone());

            let peer_for_task = peer.clone();
            let torrent_event_tx_cloned = self.torrent_event_tx.clone();
            let orchestrate_tx_cloned = self.orchestrate_tx.clone();
            let handle = tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = Peer::run(peer_for_task, torrent_event_tx_cloned).await {
                    warn!("peer error: {e}");
                }
                // Notify scheduler this slot is now available for reconnect
                let _ = orchestrate_tx_cloned
                    .send((tracker_peer, OrchestratePeersMessage::PeerDying))
                    .await;
            });
            peer.lock().await.join_handler = Some(handle);
        }
    }

    pub async fn get_random_peer(&mut self) -> Option<TrackerPeer> {
        let peers: Vec<TrackerPeer> = self
            .available_peers
            .read()
            .await
            .iter()
            .filter(|p| !self.seen.contains(p))
            .cloned()
            .collect();
        peers.choose(&mut rand::rng()).cloned()
    }
}
