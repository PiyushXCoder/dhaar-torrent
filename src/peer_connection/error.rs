use thiserror::Error;
use tokio::io::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

use crate::{peer_explorer::Peer, piece_manager::channel::PieceManagerMessage};

#[derive(Debug, Error)]
pub enum PeerConnectionError {
    #[error("error: {0}")]
    Error(String),
    #[error("handshake failed")]
    HandshakeFailed,
    #[error("io error: {0}")]
    Io(#[from] Error),
    #[error("info hash mismatch")]
    InfoHashMismatch,
    #[error("peer not found")]
    PeerNotFound,
    #[error("unexpected message")]
    UnexpectedMessage,
    #[error("peer disconnected")]
    PeerDisconnected,
    /// Carries the peer so the failure can name it. Requeuing no longer
    /// depends on this: the peer manager knows which peer each task was
    /// dialling and reclaims it whichever way the task ends.
    #[error("failed to connect to peer {}: {source}", peer.address)]
    ConnectFailed {
        peer: Box<Peer>,
        source: std::io::Error,
    },
    #[error("piece manager channel send error: {0}")]
    PieceManagerSend(#[from] SendError<PieceManagerMessage>),
    #[error("piece manager dropped response channel: {0}")]
    ResponseChannelDropped(#[from] RecvError),
    #[error("timeout")]
    Timeout,
}

pub type PeerConnectionResult<T> = std::result::Result<T, PeerConnectionError>;
