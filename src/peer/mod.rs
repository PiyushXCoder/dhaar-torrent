mod messages;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use crate::TorrentEvent;
use crate::error::{Error, Result};
use crate::peer::messages::Message;
use crate::piece_bag::PieceBag;
use crate::tracker::Peer as TrackerPeer;

const PROTOCOL: &[u8] = b"BitTorrent protocol";

pub struct Peer {
    pub join_handler: Option<tokio::task::JoinHandle<()>>,
    pub piece_bag: Arc<PieceBag>,
    pub tracker_peer: TrackerPeer,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub im_choking: bool,
    pub im_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub bitfield: Option<Vec<u8>>,
}

impl Peer {
    pub fn new(
        piece_bag: Arc<PieceBag>,
        tracker_peer: TrackerPeer,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Self {
        Self {
            join_handler: None,
            piece_bag,
            tracker_peer,
            info_hash,
            peer_id,
            im_choking: false,
            im_interested: false,
            peer_choking: false,
            peer_interested: false,
            bitfield: None,
        }
    }

    pub async fn run(
        this: Arc<Mutex<Self>>,
        torrent_event_tx: mpsc::Sender<TorrentEvent>,
    ) -> Result<()> {
        let addr = {
            let p = this.lock().await;
            format!("{}:{}", p.tracker_peer.ip, p.tracker_peer.port)
        };
        info!(%addr, "connecting to peer");

        // TCP connect and handshake — no lock held during I/O
        let mut tcp_stream = TcpStream::connect(&addr).await?;
        this.lock().await.handshake(&mut tcp_stream).await?;
        info!(%addr, "handshake complete");
        let _ = torrent_event_tx.send(TorrentEvent::PeerConnected).await;

        let (reader, writer) = tokio::io::split(tcp_stream);
        let (inbound_tx, mut inbound_rx) = mpsc::channel::<Message>(32);
        let (outbound_tx, outbound_rx) = mpsc::channel::<Message>(32);

        let read_handle = tokio::spawn(Self::read_loop(reader, inbound_tx));
        let write_handle = tokio::spawn(Self::write_loop(writer, outbound_rx));

        // Lock held only during message handling, not across TCP reads
        while let Some(msg) = inbound_rx.recv().await {
            let mut guard = this.lock().await;
            guard
                .handle_message(msg, &outbound_tx, &torrent_event_tx)
                .await;
        }

        info!(%addr, "peer disconnected");
        let _ = torrent_event_tx.send(TorrentEvent::PeerDisconnected).await;
        drop(outbound_tx);

        let (t1, t2) = tokio::join!(read_handle, write_handle);
        let _ = (t1?, t2?);
        Ok(())
    }

    async fn handle_message(
        &mut self,
        msg: Message,
        out: &mpsc::Sender<Message>,
        tx: &mpsc::Sender<TorrentEvent>,
    ) {
        match msg {
            Message::Choke => {
                debug!("peer choked us");
                self.peer_choking = true;
            }
            Message::Unchoke => {
                debug!("peer unchoked us");
                self.peer_choking = false;
                // TODO: send pending Request messages
            }
            Message::Interested => {
                debug!("peer is interested");
                self.peer_interested = true;
            }
            Message::NotInterested => {
                debug!("peer not interested");
                self.peer_interested = false;
            }
            Message::Bitfield(_bits) => {
                debug!("received bitfield");
                // TODO: store which pieces peer has
                self.im_interested = true;
                let _ = out.send(Message::Interested).await;
            }
            Message::Have(idx) => {
                debug!(piece = idx, "peer has piece");
                // TODO: update peer bitfield, maybe send Interested
            }
            Message::Piece { index, begin, data } => {
                let bytes = data.len() as u64;
                trace!(piece = index, begin, bytes, "received block");
                self.piece_bag
                    .downloaded
                    .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
                let _ = tx.send(TorrentEvent::Downloaded(bytes)).await;
                // TODO: write block to file, check if piece complete → PieceComplete(index)
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                debug!(piece = index, begin, length, "peer requested block");
                // TODO: serve block if not choking peer
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                debug!(piece = index, begin, length, "peer cancelled request");
                // TODO: cancel pending upload for this block
            }
            Message::KeepAlive => {
                trace!("keep-alive");
            }
        }
    }

    async fn read_loop(mut reader: tokio::io::ReadHalf<TcpStream>, tx: mpsc::Sender<Message>) {
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut len_buf).await {
                warn!(error = %e, "read_loop: IO error reading length");
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len == 0 {
                trace!("read_loop: keep-alive");
                continue;
            }
            let mut buf = vec![0u8; len];
            if let Err(e) = reader.read_exact(&mut buf).await {
                warn!(error = %e, "read_loop: IO error reading payload");
                break;
            }
            match Message::from_bytes(buf[0], &buf[1..]) {
                Ok(msg) => {
                    if tx.send(msg).await.is_err() {
                        debug!("read_loop: inbound channel closed, stopping");
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "read_loop: unknown message, dropping peer");
                    break;
                }
            }
        }
    }

    async fn write_loop(
        mut writer: tokio::io::WriteHalf<TcpStream>,
        mut rx: mpsc::Receiver<Message>,
    ) {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = writer.write_all(&msg.to_bytes()).await {
                warn!(error = %e, "write_loop: IO error, stopping");
                break;
            }
        }
        debug!("write_loop: outbound channel closed");
    }

    pub async fn handshake(&mut self, stream: &mut TcpStream) -> Result<()> {
        debug!("sending handshake");
        let mut msg = [0u8; 68];
        msg[0] = PROTOCOL.len() as u8;
        msg[1..20].copy_from_slice(PROTOCOL);
        // msg[20..28] reserved — stay zero
        msg[28..48].copy_from_slice(&self.info_hash);
        msg[48..68].copy_from_slice(&self.peer_id);
        stream.write_all(&msg).await?;

        let mut resp = [0u8; 68];
        stream.read_exact(&mut resp).await?;

        if resp[0] != PROTOCOL.len() as u8 || &resp[1..20] != PROTOCOL {
            warn!("handshake: bad protocol string");
            return Err(Error::Error("handshake: bad protocol string".into()));
        }
        if resp[28..48] != self.info_hash {
            warn!("handshake: info_hash mismatch");
            return Err(Error::Error("handshake: info_hash mismatch".into()));
        }

        Ok(())
    }
}
