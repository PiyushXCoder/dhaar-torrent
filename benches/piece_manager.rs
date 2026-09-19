//! The serial path: one `PieceManager` task, and the bitfield it maintains.
//!
//! Every connection funnels its blocks through a single task, so whatever that
//! task can retire per second is a ceiling no number of peers can beat. The
//! swarm benchmark (`scripts/bench-seeders.sh`) measures what the client
//! actually achieves; this measures what the manager alone could, with no
//! network, no disk and no contention in the way. When the two agree, the
//! manager is the bottleneck — which is what the mailbox depth in the
//! `request_window` trace has been saying.
//!
//! Run with `cargo bench --bench piece_manager`.

use std::sync::Arc;

use divan::counter::{BytesCount, ItemsCount};
use serde_bytes::ByteBuf;
use sha1::Digest;
use tokio::{runtime::Runtime, sync::watch};

use dhaar_torrent::{
    peer_explorer::Peer,
    piece_manager::{
        PieceManager,
        channel::{PieceManagerMessage, new_piece_manager_channel},
    },
    status::{DownloadStats, PieceProgress},
    store::Store,
    wire_protocol::Bitfield,
};

const PIECE: u64 = 256 * 1024;
/// Enough pieces that per-run setup does not dominate, few enough that one
/// iteration stays in the tens of milliseconds.
const PIECES: u64 = 64;
const TOTAL: u64 = PIECE * PIECES;
/// Every block is this byte, so every piece has the same contents and the same
/// hash — the manager still verifies each one, it just does not need 64
/// distinct fixtures to do it.
const FILL: u8 = 0xAB;

fn main() {
    divan::main();
}

fn runtime() -> &'static Runtime {
    static RT: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}

/// Accepts everything and stores nothing. The point is to time the manager's
/// own loop — the hashing, the block bookkeeping, the bitfield — rather than
/// the disk, which `benches/store.rs` already measures.
///
/// `read` still has to return the real bytes: a piece whose blocks arrive out
/// of order falls back to hashing what the store holds, and a writer that
/// answered with zeroes there would fail every such piece and quietly turn
/// this into a benchmark of the retry path.
struct NullWriter;

#[async_trait::async_trait]
impl Store for NullWriter {
    type Error = std::io::Error;

    async fn initialize(
        &self,
        _piece_hashes: Vec<[u8; 20]>,
    ) -> Result<Option<Bitfield>, Self::Error> {
        Ok(None)
    }

    async fn read(
        &self,
        _piece_index: u32,
        _piece_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![FILL; length as usize])
    }

    async fn write(
        &self,
        _piece_index: u32,
        _piece_offset: u64,
        _data: Vec<u8>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn set_bitfield(&self, _bitfield: Bitfield) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn finalize(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn peer() -> Peer {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    Peer {
        peer_id: None,
        address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6881)),
    }
}

/// A manager already running its loop, plus the ends needed to feed it and to
/// tell when it is finished.
struct Harness {
    sender: dhaar_torrent::piece_manager::channel::PieceManagerChannelSender,
    progress: watch::Receiver<PieceProgress>,
}

fn harness() -> Harness {
    let piece = vec![FILL; PIECE as usize];
    let hash: [u8; 20] = sha1::Sha1::digest(&piece).into();
    let hashes: Vec<u8> = (0..PIECES).flat_map(|_| hash).collect();

    let (progress_tx, progress_rx) = watch::channel(PieceProgress::default());
    let (sender, receiver) = new_piece_manager_channel();

    let manager = PieceManager::new(
        &ByteBuf::from(hashes),
        PIECE,
        TOTAL,
        Arc::new(NullWriter),
        Arc::new(DownloadStats::default()),
        progress_tx,
        [0u8; 20],
    );
    runtime().spawn(manager.start(receiver));

    Harness {
        sender,
        progress: progress_rx,
    }
}

/// Claims every piece and reports each one verified, which is the whole of
/// what the manager does now that connections buffer, hash and write for
/// themselves. Two messages per piece instead of seventeen, and none of them
/// carrying payload.
///
/// This is the ceiling a swarm cannot exceed no matter how many peers it has,
/// because this task is the one thing they all share.
#[divan::bench]
fn coordinate_whole_download(bencher: divan::Bencher) {
    bencher
        .counter(BytesCount::new(TOTAL))
        .counter(ItemsCount::new(PIECES))
        .with_inputs(harness)
        .bench_values(|mut harness| {
            runtime().block_on(async move {
                let mut bitfield = Bitfield(vec![0xFF; (PIECES as usize).div_ceil(8)]);
                bitfield.set_piece(PIECES as u32 - 1, true);
                for piece_index in 0..PIECES as u32 {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    harness
                        .sender
                        .send(PieceManagerMessage::ClaimPiece {
                            bitfield: bitfield.clone(),
                            peer: peer(),
                            response_sender: tx,
                        })
                        .await
                        .unwrap();
                    let _ = rx.await.unwrap();
                    harness
                        .sender
                        .send(PieceManagerMessage::PieceVerified {
                            piece_index,
                            peer: peer(),
                        })
                        .await
                        .unwrap();
                }
                harness
                    .progress
                    .wait_for(|p| p.completed_pieces as u64 == PIECES)
                    .await
                    .unwrap();
            })
        });
}

/// What maintaining the bitfield across a whole download costs, the way the
/// manager does it now: one bit per completed piece.
#[divan::bench(args = [4096, 16384])]
fn bitfield_incremental(bencher: divan::Bencher, pieces: u32) {
    bencher
        .counter(ItemsCount::new(pieces))
        .with_inputs(|| Bitfield(vec![0u8; (pieces as usize).div_ceil(8)]))
        .bench_values(|mut bitfield| {
            for index in 0..pieces {
                bitfield.set_piece(index, true);
            }
            bitfield
        });
}

/// The same thing rebuilt from the piece list on every completion, which is
/// what the manager used to do. Quadratic in the piece count: at 4096 pieces
/// it is a rounding error, and the ratio to the bench above is how much worse
/// it gets on a torrent sixteen times that size.
#[divan::bench(args = [4096, 16384])]
fn bitfield_rebuilt(bencher: divan::Bencher, pieces: u32) {
    bencher
        .counter(ItemsCount::new(pieces))
        .with_inputs(|| vec![false; pieces as usize])
        .bench_values(|mut complete| {
            let mut bitfield = Bitfield(vec![0u8; (pieces as usize).div_ceil(8)]);
            for index in 0..pieces as usize {
                complete[index] = true;
                // The old rebuild: every completed piece walked the whole list.
                let mut bytes = vec![0u8; (pieces as usize).div_ceil(8)];
                for (i, done) in complete.iter().enumerate() {
                    if *done {
                        bytes[i / 8] |= 1 << (7 - (i % 8));
                    }
                }
                bitfield = Bitfield(bytes);
            }
            bitfield
        });
}
