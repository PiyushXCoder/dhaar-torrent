use crate::error::Error;
use crate::error::Result;
use crate::piece_bag::PieceBag;
use crate::torrent::TorrentEvent;
use crate::torrent_file::TorrentFile;
use crate::tracker::Peer as TrackerPeer;
use crate::tracker::Tracker;
use crate::tracker::TrackerAnnounceParams;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, sleep_until};
use tracing::info;
use tracing::warn;

pub struct AvailablePeerManager {
    torrent_event_tx: mpsc::Sender<TorrentEvent>,
    trackers: Arc<RwLock<Vec<Tracker>>>,
    piece_bag: Arc<PieceBag>,
    torrent_file: Arc<TorrentFile>,
    available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
    default_params: TrackerAnnounceParams,
    heap: BinaryHeap<Tracker>,
}

impl AvailablePeerManager {
    pub fn new(
        torrent_event_tx: mpsc::Sender<TorrentEvent>,
        trackers: Arc<RwLock<Vec<Tracker>>>,
        piece_bag: Arc<PieceBag>,
        torrent_file: Arc<TorrentFile>,
        available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
        default_params: TrackerAnnounceParams,
    ) -> Self {
        Self {
            torrent_event_tx,
            trackers,
            piece_bag,
            torrent_file,
            available_peers,
            default_params,
            heap: BinaryHeap::new(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        for tracker in self.trackers.read().await.iter() {
            self.heap.push(tracker.to_owned());
        }

        let total_torrent_length = self.torrent_file.info.length.unwrap_or(
            self.torrent_file
                .info
                .files
                .as_ref()
                .map(|files| files.iter().map(|f| f.length).sum())
                .unwrap_or(0),
        );
        loop {
            let mut tracker = self.heap.pop().ok_or(Error::TrackerError(
                "No trackers left to announce".to_string(),
            ))?;
            sleep_until(tracker.next_run).await;

            let downloaded = self
                .piece_bag
                .downloaded
                .load(std::sync::atomic::Ordering::Relaxed);
            let params = TrackerAnnounceParams {
                downloaded,
                uploaded: self
                    .piece_bag
                    .uploaded
                    .load(std::sync::atomic::Ordering::Relaxed),
                left: total_torrent_length - downloaded,
                ..self.default_params.clone()
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
                            let _ = self
                                .torrent_event_tx
                                .send(TorrentEvent::TrackerWarning(warning_message))
                                .await;
                        }

                        if let Some(peers) = response.peers {
                            info!("Got {} peers from tracker", peers.len());
                            let _ = self
                                .torrent_event_tx
                                .send(TorrentEvent::PeersFound(peers.len()))
                                .await;
                            self.available_peers.write().await.extend(peers);

                            tracker.next_run = Instant::now()
                                + Duration::from_secs(response.base.interval.unwrap_or(10) as u64);
                            tracker.failed_count = 0;
                            success = true;
                            break;
                        }
                        warn!("Got no peers from tracker");
                        if let Some(failure_message) = response.base.failure_reason {
                            warn!("Failure Message: {}", failure_message);
                            let _ = self
                                .torrent_event_tx
                                .send(TorrentEvent::TrackerFailure(failure_message))
                                .await;
                        }
                    }
                    Err(err) => {
                        warn!("Got error from tracker: {}", err);
                        let _ = self
                            .torrent_event_tx
                            .send(TorrentEvent::TrackerError(err.to_string()))
                            .await;
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
                (chrono::Local::now() + chrono::Duration::from_std(sleep_dur)?)
                    .format("%Y-%m-%d %H:%M:%S %Z")
            );
            self.heap.push(tracker);
        }
    }
}
