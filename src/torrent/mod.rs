use std::{collections::HashSet, sync::Arc};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::info;
mod available_peer_manager;
mod swarm_manager;
use crate::peer::Peer;
use crate::{
    error::Result,
    helpers::hex_string_hash,
    piece_bag::PieceBag,
    torrent::{available_peer_manager::AvailablePeerManager, swarm_manager::SwarmManager},
    torrent_file::TorrentFile,
    tracker::{Peer as TrackerPeer, Tracker, TrackerAnnounceParams},
};

#[derive(Debug)]
pub enum TorrentEvent {
    PeersFound(usize),
    TrackerWarning(String),
    TrackerFailure(String),
    TrackerError(String),
    PeerConnected,
    PeerDisconnected,
    Downloaded(u64),
    PieceComplete(u32),
}

pub struct TorrentHandle {
    pub events: mpsc::Receiver<TorrentEvent>,
    pub task: JoinHandle<(Result<()>, Result<()>)>,
}

pub struct Torrent {
    pub torrent_file: Arc<TorrentFile>,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub trackers: Arc<RwLock<Vec<Tracker>>>,
    pub available_peers: Arc<RwLock<HashSet<TrackerPeer>>>,
    pub swarm: Arc<RwLock<Vec<Arc<Mutex<Peer>>>>>,
    pub piece_bag: Arc<PieceBag>,
}

impl Torrent {
    pub fn new(torrent_file: TorrentFile, info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self {
            torrent_file: Arc::new(torrent_file),
            info_hash,
            peer_id,
            trackers: Arc::new(RwLock::new(Vec::new())),
            available_peers: Arc::new(RwLock::new(HashSet::new())),
            swarm: Arc::new(RwLock::new(Vec::new())),
            piece_bag: Arc::new(PieceBag::new(hex_string_hash(&info_hash))),
        }
    }

    pub async fn start(&mut self) -> Result<TorrentHandle> {
        info!("Starting torrent {}", hex_string_hash(&self.info_hash));
        self.prepare_announce_list().await;

        let default_params = TrackerAnnounceParams::new(&self.info_hash, &self.peer_id);
        let (torrent_event_tx, torrent_event_rx) = mpsc::channel(64);
        let (trackers, piece_bag, torrent_file, available_peers, swarm) = (
            self.trackers.clone(),
            self.piece_bag.clone(),
            self.torrent_file.clone(),
            self.available_peers.clone(),
            self.swarm.clone(),
        );
        let (info_hash, peer_id) = (self.info_hash, self.peer_id);
        let mut available_peer_manager = AvailablePeerManager::new(
            torrent_event_tx.clone(),
            trackers,
            piece_bag.clone(),
            torrent_file,
            available_peers.clone(),
            default_params,
        );

        let mut swarm_manager = SwarmManager::new(
            info_hash,
            peer_id,
            available_peers,
            swarm,
            piece_bag,
            torrent_event_tx,
        );

        let join_handle = tokio::spawn(async move {
            tokio::join!(available_peer_manager.start(), swarm_manager.start())
        });

        Ok(TorrentHandle {
            events: torrent_event_rx,
            task: join_handle,
        })
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
