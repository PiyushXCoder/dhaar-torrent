use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::time::Instant;
use tracing::info;

use crate::bencode;
use crate::error::{Error, Result};

fn url_encode_bytes(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            result.push(b as char);
        } else {
            result.push_str(&format!("%{:02X}", b));
        }
    }
    result
}

#[derive(Debug, Clone)]
pub struct TrackerAnnounceParams {
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

impl TrackerAnnounceParams {
    pub fn new(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Self {
        TrackerAnnounceParams {
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

#[derive(Debug, Eq, Clone)]
pub struct Tracker {
    pub announce_url: String,
    pub backup_urls: VecDeque<String>,
    pub next_run: Instant,
    pub failed_count: u32,
    pub tracker_id: Option<String>,
}

impl PartialEq for Tracker {
    fn eq(&self, other: &Self) -> bool {
        self.next_run == other.next_run
    }
}

impl Ord for Tracker {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.next_run.cmp(&other.next_run).reverse()
    }
}

impl PartialOrd for Tracker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Tracker {
    pub fn new(announce_url: &str, backup_urls: Vec<String>) -> Tracker {
        let backup_urls = VecDeque::from(backup_urls);
        Tracker {
            announce_url: announce_url.to_string(),
            backup_urls,
            next_run: Instant::now(),
            failed_count: 0,
            tracker_id: None,
        }
    }

    pub fn rotate_url(&mut self) {
        self.backup_urls.push_back(self.announce_url.clone());
        self.announce_url = self.backup_urls.pop_front().unwrap();
    }
    pub async fn announce(&self, params: &TrackerAnnounceParams) -> Result<TrackerResponse> {
        let mut url = format!(
            "{}?info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact={}&no_peer_id={}",
            self.announce_url,
            url_encode_bytes(&params.info_hash),
            url_encode_bytes(&params.peer_id),
            params.port,
            params.uploaded,
            params.downloaded,
            params.left,
            params.compact as u8,
            params.no_peer_id as u8,
        );
        info!("Announcing to {}", url);

        if let Some(ref event) = params.event {
            let event_str = match event {
                TrackerEvent::Started => "started",
                TrackerEvent::Completed => "completed",
                TrackerEvent::Stopped => "stopped",
            };
            url.push_str("&event=");
            url.push_str(event_str);
        }
        if let Some(ref ip) = params.ip {
            url.push_str("&ip=");
            url.push_str(ip);
        }
        if let Some(ref num_want) = params.num_want {
            url.push_str("&numwant=");
            url.push_str(&num_want.to_string());
        }
        if let Some(ref key) = params.key {
            url.push_str("&key=");
            url.push_str(key);
        }
        if let Some(ref tracker_id) = params.tracker_id {
            url.push_str("&tracker_id=");
            url.push_str(tracker_id);
        }

        let client = reqwest::Client::new();
        let res = client.get(&url).send().await?;
        let bytes = res.bytes().await?;

        let response = bencode::from_bytes::<TrackerResponse>(&bytes)
            .map_err(crate::error::Error::from)
            .or_else(|_| {
                let raw = bencode::from_bytes::<TrackerResponseRawPeer>(&bytes)?;
                TrackerResponse::try_from(raw)
            })?;
        Ok(response)
    }
}
