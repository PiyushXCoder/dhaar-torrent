use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Debug, Clone)]
pub struct TrackerAnnounceQuery {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub compact: bool,
    pub no_peer_id: bool,
    pub event: Option<TrackerEvent>,
    pub ip: Option<String>,
    pub num_want: Option<u32>,
    pub key: Option<String>,
    pub tracker_id: Option<String>,
}

impl TrackerAnnounceQuery {
    pub fn new(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Self {
        TrackerAnnounceQuery {
            info_hash: *info_hash,
            peer_id: *peer_id,
            port: 6889,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            compact: true,
            no_peer_id: false,
            event: None,
            ip: None,
            num_want: None,
            key: None,
            tracker_id: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TrackerEvent {
    Started,
    Completed,
    Stopped,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackerResponseBase {
    #[serde(rename = "failure reason")]
    pub failure_reason: Option<String>,
    #[serde(rename = "warning message")]
    pub warning_message: Option<String>,
    pub interval: Option<u32>,
    #[serde(rename = "min interval")]
    pub min_interval: Option<u32>,
    #[serde(rename = "tracker id")]
    pub tracker_id: Option<String>,
    pub complete: Option<u32>,
    pub incomplete: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackerResponse {
    #[serde(flatten)]
    pub base: TrackerResponseBase,
    pub peers: Option<Vec<Peer>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackerResponseRawPeer {
    #[serde(flatten)]
    pub base: TrackerResponseBase,
    pub peers: ByteBuf,
}

impl TryFrom<TrackerResponseRawPeer> for TrackerResponse {
    type Error = Error;
    fn try_from(value: TrackerResponseRawPeer) -> Result<TrackerResponse> {
        let peers: Vec<Peer> = value
            .peers
            .chunks_exact(6)
            .map(|chunk| Peer {
                peer_id: None,
                ip: format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]),
                port: u16::from_be_bytes([chunk[4], chunk[5]]),
            })
            .collect();

        Ok(TrackerResponse {
            base: value.base,
            peers: Some(peers),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq)]
pub struct Peer {
    #[serde(rename = "peer id")]
    pub peer_id: Option<[u8; 20]>,
    pub ip: String,
    pub port: u16,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}

impl std::hash::Hash for Peer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.port.hash(state);
    }
}

impl From<Peer> for crate::peer_explorer::Peer {
    fn from(val: Peer) -> Self {
        crate::peer_explorer::Peer {
            peer_id: val.peer_id,
            ip: val.ip,
            port: val.port,
        }
    }
}
