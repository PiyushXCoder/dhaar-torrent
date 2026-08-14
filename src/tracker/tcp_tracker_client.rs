use super::tracker_client_messages::*;
use tracing::info;

use super::tracker_client::TrackerClient;
use crate::error::Result;
use crate::helpers::url_safe_string_hash;
use bencode;

#[derive(Debug, Clone)]
pub struct TcpTrackerClient {
    pub announce_url: String,
    pub tracker_id: Option<String>,
}

impl TcpTrackerClient {
    pub fn new(announce_url: &str) -> Self {
        Self {
            announce_url: announce_url.to_string(),
            tracker_id: None,
        }
    }
}

#[async_trait::async_trait]
impl TrackerClient for TcpTrackerClient {
    async fn announce(&self, params: &TrackerAnnounceQuery) -> Result<TrackerResponse> {
        let mut url = format!(
            "{}?info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact={}&no_peer_id={}",
            self.announce_url,
            url_safe_string_hash(&params.info_hash),
            url_safe_string_hash(&params.peer_id),
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
