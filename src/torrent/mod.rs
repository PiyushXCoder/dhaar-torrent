use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;
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

        let default_tracker_announce_params =
            TrackerAnnounceParams::new(&self.info_hash, &self.peer_id);
        let trackers = self.trackers.clone();
        let piece_bag = self.piece_bag.clone();
        let torrent_file = self.torrent_file.clone();
        let available_peers = self.available_peers.clone();
        tokio::spawn(async move {
            let mut trackers = trackers.write().await;
            let piece_bag = piece_bag.read().await;
            let params = TrackerAnnounceParams {
                downloaded: piece_bag.downloaded,
                uploaded: piece_bag.uploaded,
                left: torrent_file.info.length.unwrap_or(u64::MAX) - piece_bag.downloaded,
                ..default_tracker_announce_params // downloaded: piece_bag.downloaded,
            };
            for tracker in trackers.iter_mut() {
                let mut success = false;

                info!("Announcing to tracker {}", tracker.announce_url);
                for _ in 0..(tracker.backup_urls.len() + 1) {
                    match tracker.announce(&params).await {
                        Ok(response) => {
                            let Some(peers) = response.peers else {
                                warn!("Got no peers from tracker");
                                tracker.rotate_url();
                                continue;
                            };

                            info!("Got {} peers from tracker", peers.len());

                            let mut available_peers = available_peers.write().await;
                            for peer in peers {
                                available_peers.insert(peer);
                            }

                            success = true;
                            break;
                        }
                        Err(err) => {
                            warn!("Got error from tracker: {}", err);
                            tracker.rotate_url();
                        }
                    }
                }

                if !success {
                    warn!("Failed to get peers from all trackers");
                }
            }
            info!("{:#?}", available_peers.read().await);
        })
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
}
