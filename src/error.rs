use crate::{bencode, torrent::TorrentEvent};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum Error {
    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("toml de error: {0}")]
    TomlError(#[from] toml::de::Error),
    #[error("toml ser error: {0}")]
    TomlSerError(#[from] toml::ser::Error),
    #[error("bencode error: {0}")]
    BencodeError(#[from] bencode::error::Error),
    #[error("join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("unsupported tracker protocol")]
    UnsupportedTrackerProtocol,
    #[error("sender error: {0}")]
    TorrentEventSenderError(#[from] mpsc::error::SendError<TorrentEvent>),
    #[error("tracker error: {0}")]
    TrackerError(String),
    #[error("error: {0}")]
    Error(String),
}

pub type Result<T> = std::result::Result<T, Error>;
