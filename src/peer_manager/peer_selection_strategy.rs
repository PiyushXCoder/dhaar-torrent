use crate::peer_explorer::Peer;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// A peer handed out by [`PeerSelectionStrategy::pop`], tagged with whether
/// it's back after a failed attempt so the caller can tell it apart from a
/// fresh peer.
pub struct PeerAttempt {
    pub peer: Peer,
    pub is_retry: bool,
}

#[async_trait::async_trait]
pub trait PeerSelectionStrategy {
    /// Enqueue a peer. `failed` marks it as a retry after a failed attempt,
    /// delaying it by a fixed [`RETRY_DELAY`] instead of making it
    /// immediately available.
    fn push(&mut self, peer: Peer, failed: bool);
    /// Look at the next peer due, without waiting or removing it.
    fn peek(&self) -> Option<&Peer>;
    /// Wait until the next peer is due, then remove and return it.
    async fn pop(&mut self) -> Option<PeerAttempt>;
}

pub struct RetryAfterDelayPeerSelectionStrategy {
    heap: BinaryHeap<Reverse<PeerCandiate>>,
}

impl RetryAfterDelayPeerSelectionStrategy {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }
}

impl Default for RetryAfterDelayPeerSelectionStrategy {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl PeerSelectionStrategy for RetryAfterDelayPeerSelectionStrategy {
    fn push(&mut self, peer: Peer, failed: bool) {
        self.heap
            .retain(|Reverse(candidate)| candidate.peer != peer);

        let next_instance = if failed {
            Instant::now() + RETRY_DELAY
        } else {
            Instant::now()
        };

        self.heap.push(Reverse(PeerCandiate {
            peer,
            next_instance,
            is_retry: failed,
        }));
    }

    fn peek(&self) -> Option<&Peer> {
        self.heap.peek().map(|Reverse(candidate)| &candidate.peer)
    }

    async fn pop(&mut self) -> Option<PeerAttempt> {
        let Reverse(candidate) = self.heap.peek()?;
        sleep_until(candidate.next_instance).await;
        self.heap.pop().map(|Reverse(candidate)| PeerAttempt {
            peer: candidate.peer,
            is_retry: candidate.is_retry,
        })
    }
}

pub struct PeerCandiate {
    pub peer: Peer,
    pub next_instance: Instant,
    pub is_retry: bool,
}

impl PartialEq for PeerCandiate {
    fn eq(&self, other: &Self) -> bool {
        self.next_instance == other.next_instance
    }
}

impl Eq for PeerCandiate {}

impl PartialOrd for PeerCandiate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PeerCandiate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.next_instance.cmp(&other.next_instance)
    }
}
