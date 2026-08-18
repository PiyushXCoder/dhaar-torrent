use thiserror::Error;
use tokio::io::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

use crate::{
    peer_explorer::Peer, peer_manager::channels::PeerManagerChannelMessage,
    piece_manager::channel::PieceManagerMessage,
};

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
    /// Carries the peer back so the caller can requeue it instead of losing it.
    #[error("failed to connect to peer {}:{}: {source}", peer.ip, peer.port)]
    ConnectFailed {
        peer: Box<Peer>,
        source: std::io::Error,
    },
    #[error("piece manager channel send error: {0}")]
    PieceManagerSend(#[from] SendError<PieceManagerMessage>),
    #[error("peer manager channel send error: {0}")]
    PeerManagerSend(#[from] SendError<PeerManagerChannelMessage>),
    #[error("piece manager dropped response channel: {0}")]
    ResponseChannelDropped(#[from] RecvError),
}

pub type PeerConnectionResult<T> = std::result::Result<T, PeerConnectionError>;
