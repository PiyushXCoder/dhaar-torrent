//! Disk path benchmarks.
//!
//! These measure the real `DiskPieceWriter`, not a model of it — a hand-rolled
//! approximation of this path once reported a figure 100x off and sent a whole
//! afternoon after the wrong bottleneck.
//!
//! Run with `cargo bench`. Divan prints median and range; compare those rather
//! than a single number, and remember these are microbenchmarks: a faster
//! writer here does not imply a faster download. `scripts/test-inbound.sh` is
//! the end-to-end counterpart.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use divan::counter::BytesCount;
use sha1::Digest;
use tokio::runtime::Runtime;

use dhaar_torrent::{
    piece_writer::{DiskPieceWriter, PieceWriter},
    wire_protocol::Bitfield,
};

const BLOCK: u64 = 16 * 1024;
const PIECE: u64 = 256 * 1024;
const PIECES: u64 = 256;
const TOTAL: u64 = PIECE * PIECES;
const BITFIELD_LEN: usize = (PIECES as usize).div_ceil(8);

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}

/// One writer over one store, shared by every benchmark. Built on first use so
/// the cost of laying out the file is not charged to whichever bench runs
/// first. The mutex is uncontended — divan runs a bench on one thread unless
/// told otherwise — so it costs tens of nanoseconds against tens of micros.
fn writer() -> &'static Mutex<DiskPieceWriter> {
    static WRITER: OnceLock<Mutex<DiskPieceWriter>> = OnceLock::new();
    WRITER.get_or_init(|| {
        let w = DiskPieceWriter::new(TOTAL, PIECE, &"bench".to_string(), &None, &None, [7u8; 20]);
        // The store is always fresh — `main` runs from a directory named after
        // this process — so `initialize` lays it out and never reaches the
        // repair loop these hashes feed. They still have to number `PIECES`:
        // the writer checks the count against its own geometry.
        runtime()
            .block_on(w.initialize(vec![[0u8; 20]; PIECES as usize]))
            .unwrap();
        Mutex::new(w)
    })
}

/// Rotates the offset so successive iterations do not all hit one page, which
/// would measure the page cache rather than the write path.
fn next_piece() -> u32 {
    static N: AtomicU64 = AtomicU64::new(0);
    (N.fetch_add(1, Ordering::Relaxed) % PIECES) as u32
}

fn main() {
    // `DiskPieceWriter` names its store relative to the current directory, so
    // the benchmark moves itself somewhere disposable rather than writing a
    // 64 MiB file into the repository.
    let dir = std::env::temp_dir().join(format!("dhaar-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_current_dir(&dir).unwrap();

    divan::main();

    let _ = std::fs::remove_dir_all(&dir);
}

/// The hot path: one block arriving from a peer and going to disk. No barrier
/// here by design — `set_bitfield` is where durability is paid for.
#[divan::bench]
fn write_block(bencher: divan::Bencher) {
    bencher
        .counter(BytesCount::new(BLOCK))
        .with_inputs(|| vec![0xABu8; BLOCK as usize])
        .bench_values(|data| {
            let w = writer().lock().unwrap();
            runtime().block_on(w.write(next_piece(), 0, data)).unwrap()
        });
}

/// Serving a block to a peer. Reads here hit the page cache; a real seed of a
/// large torrent often will not, and then this is a disk seek instead.
#[divan::bench]
fn read_block(bencher: divan::Bencher) {
    bencher.counter(BytesCount::new(BLOCK)).bench(|| {
        let w = writer().lock().unwrap();
        runtime().block_on(w.read(next_piece(), 0, BLOCK)).unwrap()
    });
}

/// The durability barrier, paid once per completed piece: sync the data, write
/// the claim, sync the claim. This is the expensive call in the whole file.
#[divan::bench]
fn set_bitfield(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| Bitfield(vec![0xFF; BITFIELD_LEN]))
        .bench_values(|bits| {
            let w = writer().lock().unwrap();
            runtime().block_on(w.set_bitfield(bits)).unwrap()
        });
}

/// What a completed piece actually costs on disk: sixteen blocks plus one
/// barrier. The closest single number to "how fast can we take data".
#[divan::bench]
fn write_whole_piece(bencher: divan::Bencher) {
    bencher.counter(BytesCount::new(PIECE)).bench(|| {
        let w = writer().lock().unwrap();
        let piece = next_piece();
        runtime().block_on(async {
            for block in 0..(PIECE / BLOCK) {
                w.write(piece, block * BLOCK, vec![0xABu8; BLOCK as usize])
                    .await
                    .unwrap();
            }
            w.set_bitfield(Bitfield(vec![0xFF; BITFIELD_LEN]))
                .await
                .unwrap();
        })
    });
}

/// What the `spawn_blocking` around each write actually costs, as opposed to
/// the write itself. `write` hands every block to the blocking pool, which is
/// the documented thing to do with blocking I/O -- but a 16 KiB `pwrite` that
/// lands in page cache is a short, bounded syscall, and the handoff may well
/// cost more than the work. The gap between these two is the answer, and it is
/// what decides whether batching, a writer task, or dropping the handoff is
/// the right fix.
mod handoff {
    use super::*;
    use std::os::unix::fs::FileExt;

    fn file() -> &'static std::sync::Arc<std::fs::File> {
        static F: OnceLock<std::sync::Arc<std::fs::File>> = OnceLock::new();
        F.get_or_init(|| {
            let path = std::env::current_dir().unwrap().join("handoff.bin");
            let f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            f.set_len(TOTAL).unwrap();
            std::sync::Arc::new(f)
        })
    }

    /// The syscall on its own, no pool in the way.
    #[divan::bench]
    fn write_block_direct(bencher: divan::Bencher) {
        bencher
            .counter(BytesCount::new(BLOCK))
            .with_inputs(|| vec![0xABu8; BLOCK as usize])
            .bench_values(|data| {
                let offset = next_piece() as u64 * PIECE;
                file().write_all_at(&data, offset).unwrap()
            });
    }

    /// The same syscall reached the way `DiskPieceWriter::write` reaches it.
    #[divan::bench]
    fn write_block_spawn_blocking(bencher: divan::Bencher) {
        bencher
            .counter(BytesCount::new(BLOCK))
            .with_inputs(|| vec![0xABu8; BLOCK as usize])
            .bench_values(|data| {
                let offset = next_piece() as u64 * PIECE;
                let f = file().clone();
                runtime()
                    .block_on(async move {
                        tokio::task::spawn_blocking(move || f.write_all_at(&data, offset)).await
                    })
                    .unwrap()
                    .unwrap()
            });
    }

    /// A whole piece in one syscall instead of sixteen. If the handoff is the
    /// cost, this is what batching would buy.
    #[divan::bench]
    fn write_whole_piece_one_call(bencher: divan::Bencher) {
        bencher
            .counter(BytesCount::new(PIECE))
            .with_inputs(|| vec![0xABu8; PIECE as usize])
            .bench_values(|data| {
                let offset = next_piece() as u64 * PIECE;
                let f = file().clone();
                runtime()
                    .block_on(async move {
                        tokio::task::spawn_blocking(move || f.write_all_at(&data, offset)).await
                    })
                    .unwrap()
                    .unwrap()
            });
    }
}

/// Verification, which runs inline in the piece manager's loop. Worth watching
/// in debug builds: it is an order of magnitude slower there.
#[divan::bench]
fn hash_piece(bencher: divan::Bencher) {
    bencher
        .counter(BytesCount::new(PIECE))
        .with_inputs(|| vec![0xABu8; PIECE as usize])
        .bench_refs(|piece| {
            let hash: [u8; 20] = sha1::Sha1::digest(&*piece).into();
            hash
        });
}
