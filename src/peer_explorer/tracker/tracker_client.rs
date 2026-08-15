use super::tracker_client_messages::{TrackerAnnounceQuery, TrackerResponse};
use crate::error::Result;

#[async_trait::async_trait]
pub trait TrackerClient {
    async fn announce(&self, params: &TrackerAnnounceQuery) -> Result<TrackerResponse>;
}
