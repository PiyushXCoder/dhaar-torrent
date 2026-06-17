use std::{
    collections::{BinaryHeap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::RwLock,
    time::{Instant, sleep_until},
};
use tracing::{info, warn};

use crate::{
    error::Result,
    helpers::hex_string_hash,
    peer::Peer,
    piece_bag::PieceBag,
    torrent_file::TorrentFile,
    tracker::{Peer as TrackerPeer, Tracker, TrackerAnnounceParams},
};

pub struct Torrent {
    pub torrent_file: Arc<TorrentFile>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub trackers: Arc<RwLock<Vec<Tracker>>>,
    pub available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
    pub swarm: Vec<Peer>,
    pub piece_bag: Arc<RwLock<PieceBag>>,
}

impl Torrent {
    pub fn new(torrent_file: TorrentFile, info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self {
            torrent_file: Arc::new(torrent_file),
            info_hash,
            peer_id,
            trackers: Arc::new(RwLock::new(Vec::new())),
            available_peers: Arc::new(RwLock::new(HashSet::new())),
            swarm: Vec::new(),
            piece_bag: Arc::new(RwLock::new(PieceBag::new(hex_string_hash(&info_hash)))),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting torrent {}", hex_string_hash(&self.info_hash));
        self.prepare_announce_list().await;

        let default_params = TrackerAnnounceParams::new(&self.info_hash, &self.peer_id);
        Self::fetch_peers_in_background(
            self.trackers.clone(),
            self.piece_bag.clone(),
            self.torrent_file.clone(),
            self.available_peers.clone(),
            default_params,
        )
        .await?;
        Ok(())
    }

    pub async fn prepare_announce_list(&mut self) {
        if let Some(tracker_announce_list) = &self.torrent_file.announce_list
            && !tracker_announce_list.is_empty()
        {
            for tracker_announce in tracker_announce_list {
                let first = &tracker_announce[0];
                let rest = &tracker_announce[1..];
                let mut trackers = self.trackers.write().await;
                trackers.push(Tracker::new(first, rest.to_vec()));
            }
        }
    }

    fn fetch_peers_in_background(
        trackers: Arc<RwLock<Vec<Tracker>>>,
        piece_bag: Arc<RwLock<PieceBag>>,
        torrent_file: Arc<TorrentFile>,
        available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
        default_params: TrackerAnnounceParams,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut heap: BinaryHeap<Tracker> = BinaryHeap::new();
            for tracker in trackers.read().await.iter() {
                heap.push(tracker.to_owned());
            }

            let total_torrent_length = torrent_file.info.length.unwrap_or(
                torrent_file
                    .info
                    .files
                    .as_ref()
                    .map(|files| files.iter().map(|f| f.length).sum())
                    .unwrap_or(0),
            );
            loop {
                let mut tracker = heap.pop().unwrap();
                sleep_until(tracker.next_run).await;

                let params = TrackerAnnounceParams {
                    downloaded: piece_bag.read().await.downloaded,
                    uploaded: piece_bag.read().await.uploaded,
                    left: total_torrent_length - piece_bag.read().await.downloaded,
                    ..default_params.clone()
                };

                let mut success = false;
                info!("Announcing to tracker {}", tracker.announce_url);

                let attempt_count = tracker.backup_urls.len() + 1;
                for _ in 0..attempt_count {
                    match tracker.announce(&params).await {
                        Ok(response) => {
                            tracker.tracker_id = response.base.tracker_id;

                            if let Some(warning_message) = response.base.warning_message {
                                warn!("Tracker warning: {}", warning_message);
                            }

                            if let Some(peers) = response.peers {
                                info!("Got {} peers from tracker", peers.len());
                                available_peers.write().await.extend(peers);

                                tracker.next_run = Instant::now()
                                    + Duration::from_secs(
                                        response.base.interval.unwrap_or(10) as u64
                                    );
                                tracker.failed_count = 0;
                                success = true;
                                break;
                            }
                            warn!("Got no peers from tracker");
                            if let Some(failure_message) = response.base.failure_reason {
                                warn!("Failure Message: {}", failure_message);
                            }
                        }
                        Err(err) => {
                            warn!("Got error from tracker: {}", err);
                        }
                    }
                    tracker.failed_count += 1;
                    tracker.next_run =
                        Instant::now() + Duration::from_secs(10 * tracker.failed_count as u64);
                    tracker.rotate_url();
                }

                if !success {
                    warn!("Failed to get peers from all trackers");
                }
                let sleep_dur = tracker
                    .next_run
                    .saturating_duration_since(tokio::time::Instant::now());
                info!(
                    "Sleeping tracker till {}",
                    (chrono::Local::now() + chrono::Duration::from_std(sleep_dur).unwrap())
                        .format("%Y-%m-%d %H:%M:%S %Z")
                );
                heap.push(tracker);
            }
        })
    }
}
