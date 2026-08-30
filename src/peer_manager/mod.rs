use crate::status::DownloadStats;
use crate::{
    peer_connection::PeerConnection,
    peer_explorer::{
        Peer,
        channel::{PeerExplorerChannelMessage, PeerExplorerChannelReceiver},
    },
    piece_manager::channel::{PieceManagerChannelSender, PieceManagerMessage},
};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::oneshot, task, task::JoinSet};
use tracing::{error, warn};

pub mod peer_selection_strategy;

const MAX_PEERS: usize = 50;

pub struct PeerManager<S>
where
    S: peer_selection_strategy::PeerSelectionStrategy + Sync + Send + 'static,
{
    peer_slection_strategy: S,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    /// Peer connections currently in flight, capped at `MAX_PEERS`. Kept in
    /// the shared counters rather than a field of its own, so a caller asking
    /// for status sees the same number this loop makes decisions on.
    stats: Arc<DownloadStats>,
    download_completed: bool,
}

impl<S> PeerManager<S>
where
    S: peer_selection_strategy::PeerSelectionStrategy + Sync + Send + 'static,
{
    pub fn new(
        peer_slection_strategy: S,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
        stats: Arc<DownloadStats>,
    ) -> Self {
        Self {
            peer_slection_strategy,
            info_hash: *info_hash,
            peer_id: *peer_id,
            stats,
            download_completed: false,
        }
    }

    /// Whether there is anything left to download, cached once true. The
    /// answer only ever goes from false to true, so the piece manager is asked
    /// until it says yes and never again.
    async fn is_download_completed(&mut self, sender: &PieceManagerChannelSender) -> bool {
        if self.download_completed {
            return true;
        }
        let (response_sender, response) = oneshot::channel();
        // A piece manager that has gone away leaves nothing to download for.
        if sender
            .send(PieceManagerMessage::IsCompleted { response_sender })
            .await
            .is_err()
        {
            self.download_completed = true;
            return true;
        }
        self.download_completed = response.await.unwrap_or(true);
        self.download_completed
    }

    pub async fn start(
        mut self,
        mut peer_explorer_channel_receiver: PeerExplorerChannelReceiver,
        piece_manager_channel_sender: PieceManagerChannelSender,
    ) {
        // Connections are supervised rather than trusted to announce their own
        // end. A task that panics or is dropped runs none of its teardown, so
        // anything it was asked to report on the way out — the slot it holds,
        // the peer to requeue — would be lost. Watching the tasks themselves
        // catches every ending, including the ones that skip all our code.
        let mut connections: JoinSet<()> = JoinSet::new();
        // A panicking task returns nothing, so the peer it was dialling has to
        // be recoverable from the task id alone.
        let mut dialled: HashMap<task::Id, Peer> = HashMap::new();

        loop {
            tokio::select! {
                Some(outcome) = connections.join_next_with_id() => {
                    let (id, panicked) = match outcome {
                        Ok((id, ())) => (id, false),
                        Err(e) => (e.id(), e.is_panic()),
                    };
                    let Some(peer) = dialled.remove(&id) else {
                        continue;
                    };
                    if panicked {
                        error!("{}: connection task panicked", peer.address);
                    }
                    self.stats.peer_disconnected();
                    self.peer_slection_strategy.push(peer, true);
                }
                Some(msg) = peer_explorer_channel_receiver.recv() => {
                    match msg {
                        PeerExplorerChannelMessage::PeerFound(peer) => {
                            self.peer_slection_strategy.push(peer, false);
                        }
                    }
                }
                Some(attempt) = self.peer_slection_strategy.pop(),
                    if !self.download_completed
                        && self.stats.active_peers() < MAX_PEERS
                        && self.peer_slection_strategy.peek().is_some() =>
                {
                    // The flag can be stale by a few pieces, so confirm before
                    // dialling: a peer connected now would hand back its piece
                    // and close without transferring anything.
                    if self.is_download_completed(&piece_manager_channel_sender).await {
                        continue;
                    }

                    self.stats.peer_connected();

                    let peer = attempt.peer;
                    let stats = self.stats.clone();
                    let piece_manager_channel_sender = piece_manager_channel_sender.clone();
                    let info_hash = self.info_hash;
                    let peer_id = self.peer_id;

                    // One task for the whole connection, not one to dial and
                    // another to run it: only then does the task ending mean
                    // the connection is over.
                    let handle = connections.spawn(async move {
                        match PeerConnection::connect(
                            peer,
                            piece_manager_channel_sender,
                            &info_hash,
                            &peer_id,
                            stats,
                        )
                        .await
                        {
                            Ok(peer_connection) => peer_connection.start().await,
                            Err(e) => warn!("{}", e),
                        }
                    });
                    dialled.insert(handle.id(), peer);
                }
            }
        }
    }
}
