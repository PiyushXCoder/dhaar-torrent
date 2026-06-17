use crate::{
    error::Result,
    torrent::{Torrent, TorrentHandle},
};

pub struct Client {
    torrents: Vec<Torrent>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Client {
        Client { torrents: vec![] }
    }

    pub async fn add_torrent(&mut self, mut torrent: Torrent) -> Result<TorrentHandle> {
        let handle = torrent.start().await?;
        self.torrents.push(torrent);
        Ok(handle)
    }
}
