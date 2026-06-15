use serde::{Deserialize, Serialize};

use crate::bencode;
use crate::error::Result;

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

#[derive(Debug)]
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrackerEvent {
    Started,
    Completed,
    Stopped,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackerResponse {
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
    pub peers: Option<Vec<Peer>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Peer {
    #[serde(rename = "peer id")]
    pub peer_id: Option<[u8; 20]>,
    pub ip: String,
    pub port: u16,
}

pub struct Tracker {
    pub announce_url: String,
}

impl Tracker {
    pub fn new(announce_url: String) -> Tracker {
        Tracker { announce_url }
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
        let response = bencode::from_bytes::<TrackerResponse>(&bytes)?;
        Ok(response)
    }
}
