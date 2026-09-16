![cover](assets/cover.png)

# Dhaar Torrent _(धार टॉरेंट)_

A torrent client written in Rust. Unserious. Built for fun.

![the reference GUI client, downloading two torrents](assets/dhaar-gui.png)

## Status

~60% complete, and a working download end to end. Peers are discovered over HTTP
trackers, connections are handshaked and framed with a `tokio-util` codec, blocks
are requested with pipelining, completed pieces are SHA-1 verified and written to
disk, and the finished download is split into the torrent's real file layout. The
last piece is short, and its block count, request lengths, hash check and disk
reads are all sized to it rather than to the full piece length.

The tail of a download does not stall behind one slow peer: once every remaining
piece is spoken for, the same blocks are requested from several peers at once and
the losers are cancelled as soon as somebody else delivers.

Peers can reach us, not just the other way round — the peer manager binds a TCP
listener and supervises accepted connections alongside dialled ones. A download
survives a restart: the partial `.dhaar` store is read back on start and its
verified pieces kept, which is what lets an instance come up already seeding.

The whole thing is a library. `Download` owns the wiring, hands back a handle,
and reports live status — bytes, rates, peers, pieces in flight, wasted bytes,
hash failures — which is what the GUI above is rendering.

Still missing: web seeds, DHT, UDP trackers, and magnet links.

### Known rough edges

- **Trackers are the only peer source.** `announce` is optional and
  `announce-list` alone is enough, but a torrent that ships neither — Arch
  Linux's ISO torrent, which carries only a BEP 19 `url-list` of web seeds —
  parses fine and then finds no peers at all. It logs a warning and sits idle.
- **A resumed store is invisible to the tracker.** Pieces recovered from disk
  set the progress counters but never pass through `piece_verified`, which is
  what `verified_bytes` counts. An instance that comes up complete announces
  `left` as the full torrent length and never leaves `Downloading`, so trackers
  and peers read it as having nothing.
- **A store belonging to another torrent is never reclaimed.** If the info hash
  on disk does not match, `initialize` returns no bitfield but leaves the old
  identity in place, so the same mismatch happens on the next start. That store
  rediscards its progress every time, silently and permanently.
- **The store format has no version.** The trailer has grown once already, and a
  store written by an older build fails the size check and is laid out fresh —
  the payload survives but the bitfield is zeroed. There is no way to tell "old
  format" from "corrupt", which are opposite situations.
- **Completion can go unreported.** The status feed is sampled once a second, so
  a download that finishes and exits inside one interval never publishes its
  final state. The data is correct on disk; only the report is missing.
- **Inbound connections are uncapped.** The 50-connection limit is checked
  before dialling out, but nothing bounds how many we accept, and accepted peers
  draw on the same budget — enough incoming can crowd out dialling entirely.
- **An inbound peer cannot be reconnected to.** A peer that dials us is known
  only by the ephemeral source port it called from, which nothing listens on.
  The base protocol carries no way to learn the real one; that needs the BEP 10
  extended handshake.
- **Torrents without a `creation date` are rejected.** The field is `Option`,
  but pairing `Option` with a `deserialize_with` and no `#[serde(default)]`
  makes serde require it anyway, so a valid file fails to parse.
- **A failed `accept()` spins.** The listener arm matches on `Ok(..)`, so an
  error falls through the pattern and the branch re-arms immediately; a
  persistent failure like running out of file descriptors becomes a busy loop.

## Usage

### GUI

```sh
cargo run -p dhaar-gui
```

Press **Add torrent** to choose a `.torrent` file, and add as many as you like.
Paths given on the command line start immediately. See
[`crates/dhaar-gui`](crates/dhaar-gui) for what it does and does not do.

### CLI

```sh
dhaar-torrent <torrent_file> [OPTIONS]
```

| Flag | Description |
| --- | --- |
| `-c, --config-file <PATH>` | Config file (default: `~/.config/dhaar-torrent/config.toml`) |
| `-l, --listening-port <PORT>` | Port to accept incoming peers on, and the one announced to the tracker (default: 6881) |

```sh
dhaar-torrent ubuntu.torrent
dhaar-torrent ubuntu.torrent --listening-port 51413
```

Progress is logged once a second. `RUST_LOG` controls the detail:

```sh
RUST_LOG=dhaar_torrent=debug dhaar-torrent ubuntu.torrent
```

### Library

```rust
use dhaar_torrent::Download;
use std::path::Path;

let download = Download::from_torrent_file(Path::new("ubuntu.torrent"), 6881)?;
let mut updates = download.subscribe();  // taken before starting, so nothing is missed

// Held for as long as the download should live: dropping it stops the download.
let _handle = download.spawn();          // returns immediately

while updates.changed().await.is_ok() {
    println!("{:.1}%", updates.borrow().progress() * 100.0);
}
```

Downloads land in the current working directory. While in flight the data lives
in a single `<name>.dhaar` file; once every piece verifies it is split into the
torrent's real file layout.

## Testing

`cargo test --workspace` covers the codec and parsing. Behaviour that only shows
up between two processes has its own scripts, each of which stands up a private
tracker and real client processes:

```sh
scripts/test-inbound.sh          # --keep on any of these retains the logs
scripts/test-repair.sh
```

`test-inbound.sh` runs a seeder and a leecher against each other, with the seeder
announcing into an empty swarm so it has nobody to dial — which is what makes
every byte that moves proof that an *accepted* connection carried it.

`test-repair.sh` hands two clients the same deliberately damaged store — a
complete bitfield, one piece whose bytes do not match its hash — differing only
in the flag byte. With the clean bit clear the client must re-hash and disown the
bad piece; with it set the client must take the bitfield at its word. The second
arm is what proves the flag gates the work rather than repair running
unconditionally.

## Benchmarks

```sh
scripts/bench-seeders.sh         # end to end: N seeders, one leecher, timed
cargo bench                      # divan; component costs of the disk path
```

One peer's throughput is capped by one design decision: a peer may hold only one
piece at a time, so its outstanding requests can never exceed a piece's worth of
blocks — sixteen at a 256 KiB piece length. Per-peer throughput is therefore
`piece_length / RTT` however many peers connect, and adding peers is the only
lever the client has. `bench-seeders.sh` pulls it directly.

Release build, store on ext4, 128 MiB, median of three runs:

| seeders | rate | vs 1 peer |
| ---: | ---: | ---: |
| 1 | 6.1 MB/s | 1.0x |
| 2 | 16.3 MB/s | 2.7x |
| 4 | 33.2 MB/s | 5.4x |
| 8 | 44.5 MB/s | 7.3x |
| 16 | 143.6 MB/s | 23.4x |
| 32 | 152.0 MB/s | 24.8x |

**Nothing plateaus**, but the last two rows are floors rather than measurements:
at 128 MiB those arms finish in under a second and the timer starts at process
launch, so a fixed startup cost is being divided into a shrinking transfer. At
512 MiB the same arms report 155 and 184 MB/s. The eight-peer arm measures the
same either way — 44.5 against 44.9 — which is what says the shorter payload is
sound everywhere above a second.

The disk is not the limit. `write_whole_piece` — sixteen blocks plus the bitfield
update, the real cost of a completed piece — runs at 977 MB/s on ext4, some two
hundred times faster than the client can fill it. What matters there is the
spread rather than the median: `read_block` is 12 µs typically and 10.3 ms at its
worst, because most reads are served from the page cache and the occasional one
is a real seek. That is why the read path stays on a blocking thread pool, and it
is the figure that counts when seeding something too large to cache.

### Reading these honestly

- **Name the filesystem.** `cargo bench` follows `TMPDIR`, which on most machines
  is tmpfs — where `fsync` has no device to reach and the disk path stops being a
  disk path. The store used to need a RAM disk for the client's own shape to be
  visible at all; since the per-piece flush was removed, tmpfs and ext4 agree
  within a few percent, but that was a 5x difference until recently and nothing
  guarantees it stays closed.
- **A microbenchmark only says a *function* got faster.** Whether a *download*
  got faster is a separate question with a separate answer. Removing the
  per-piece flush took `write_whole_piece` from 10.3 ms to 268 µs and moved the
  eight-peer arm from 19.5 to 44.5 MB/s — but an earlier disk change was worth
  2.8x on the bench and nothing at all end to end.
- **Re-run on a quiet machine.** Figures shift about 2x under background load,
  and the wide arms run 33 processes on 20 cores with the seeders competing
  against the leecher they feed.
- **Not re-measured since the durability change:** the live-swarm and
  HTTP-mirror figures below. They need a real network and vary ±20% between
  runs, so they are the old numbers and the comparison they support is stale
  until somebody re-runs it.

<img src="assets/benchmarks.svg" alt="Download rate against available link capacity: the link allows 9.9 MB/s, a single dhaar peer over loopback gets 6.1 MB/s, a live swarm reaches 2.5 MB/s, and one HTTP stream gets 0.5 MB/s" width="720">

| | rate | how it was measured |
| --- | ---: | --- |
| What the link allows | 9.9 MB/s | 8 parallel HTTP range requests to an Ubuntu mirror |
| dhaar, live swarm | 2.5 MB/s | Ubuntu ISO, ~20 peers, ±20% between runs |
| One HTTP stream | 0.5 MB/s | single request to the same mirror |

Beating a single HTTP stream 5x is BitTorrent working as intended — many peers
outrunning one server. Against the link as a whole there was four times the
throughput sitting unclaimed when this was last measured.

## Architecture

Components are independent tokio tasks talking over mpsc channels:

- **`Download`** — assembles every actor and their channels, spawns them, and hands back a `DownloadHandle` for status and shutdown
- **`peer_explorer`** — owns peer sources (currently `TrackerManager` over HTTP) and streams discovered peers out
- **`peer_manager`** — pulls peers through a selection strategy, caps concurrency at 50 connections, stops dialling once every piece is verified, and supervises the connection tasks directly: a task that panics or is dropped reports nothing, so its ending is observed rather than announced
- **`peer_connection`** — TCP connect, handshake, bitfield exchange, then hands the framed stream to `request_manager`
- **`request_manager`** — per-peer state machine (choke/interest, pipelined block requests, idle/request timeouts, cancels and `Have` announcements)
- **`piece_manager`** — the sole arbiter of who downloads what: it picks a peer's piece, registers its blocks and reports back in a single message, so two peers cannot claim the same work in the gap between asking and taking. Also SHA-1 verification and writes via a `PieceWriter` trait (`DiskPieceWriter` is the disk impl)
- **`status`** — atomics for the counters that move too often to be worth a message, and a `watch` of piece progress the piece manager builds in one turn of its loop

Workspace crates: [`crates/bencode`](crates/bencode) (serde codec) and
[`crates/dhaar-gui`](crates/dhaar-gui) (the reference client).

### The store

A download in flight lives in one file, `<name>.dhaar`, laid out as
`[payload][bitfield][info_hash][flags]` and split into the torrent's real shape
only on completion. `flags` is a single byte whose clean bit is set only on a
tidy exit, so a store found without it was left by a crash and every piece its
bitfield claims is re-hashed before it is believed.

That is a deliberate trade. Writing a piece used to sync the payload, write the
claim, then sync the claim — one device flush per piece, correct by construction
and expensive enough to cap the whole client at 25 MB/s. Correcting a bad claim
on the way back in costs one re-verification after a crash and nothing at all the
rest of the time.

## TODO

- [x] CLI args and config parsing (clap + TOML with merge)
- [x] Bencode deserializer (serde-based: integers, strings, bytes, lists, dicts, `Raw<T>`)
- [x] Bencode serializer (serde-based: integers, strings, bytes, lists, dicts)
- [x] Torrent file parsing (single and multi-file structs, raw `info` capture via serde)
- [x] Info hash computation (SHA-1 of bencoded `info` dict; hex and URL-safe forms)
- [x] Chrono datetime support in bencode (unix timestamp serde)
- [x] Logging/tracing — `tracing` + `tracing-subscriber` with env-filter
- [x] Tracker announce — HTTP GET request, URL rotation, retry with backoff
- [x] Tracker response — support binary model peers (6-byte entries)
- [x] Peer wire protocol — TCP handshake, choke/unchoke, interested, have, bitfield, request/piece/cancel/port messages
- [x] Piece manager — piece indices, bitfield tracking, atomic cross-peer piece and block claiming, SHA-1 verification
- [x] Request manager — per-peer connection state machine, pulled out of `peer_connection`
- [x] Connection timeouts — handshake/bitfield timeouts, 150s idle timeout, 30s outstanding-request timeout
- [x] Request pipelining — outstanding block requests capped at one piece's worth per peer
- [x] Disk I/O — verified pieces written to a sparse `<name>.dhaar` temp file, split into final files on completion
- [x] `lib.rs` for library API
- [x] Endgame mode — once only a few blocks remain, request them from every peer at once and `Cancel` the losers
- [x] Completion state — stop dialing peers once every piece is verified
- [x] Tracker reporting — real `uploaded`/`downloaded`/`left` and `started`/`completed` events
- [x] `Download` wrapper struct — pull the wiring out of `main.rs`
- [x] Status/progress — live counters and a sampled `DownloadStatus` feed
- [x] GUI client — iced, with a file picker and several downloads at once
- [x] Periodic keep-alive messages — sent into silence only, and inbound ones no longer swallowed by the decoder
- [x] Supervise connection tasks — the peer manager watches the tasks rather than waiting to be told
- [x] Inbound connections — TCP listener on a configurable port, accepted connections supervised alongside dialled ones
- [x] Resume support — recover already-downloaded pieces from a partial `.dhaar` file on restart
- [x] Crash-safe resume — a flag byte marks an unclean exit and the bitfield is re-verified on the way back in, instead of a device flush per piece
- [x] Seeding — `finalize` copies rather than moves, so the `.dhaar` store outlives completion and blocks can still be read out of it
- [ ] A version in the store format, so an old store can be migrated rather than discarded
- [ ] Publish completion as an event, not only as a once-a-second sample
- [ ] Let the request window span pieces, so it refills instead of draining once per piece
- [ ] `stopped` tracker event on shutdown
- [ ] Count resumed pieces as verified, so `left` and the seeding state are right after a restart
- [ ] Cap inbound connections, and reserve part of the budget so incoming cannot starve dialling
- [ ] Identify peers by `peer_id` — mutual dials currently make two connections to one client
- [ ] Handle `accept()` errors instead of letting the `select!` arm re-arm on failure
- [ ] Web seeds (BEP 19) — HTTP `url-list` sources for trackerless torrents
- [ ] Per-peer status — the library aggregates today and keeps no peer registry
- [ ] Pause and resume a running download
- [ ] Skip announce URLs we cannot speak — `udp://` and `wss://` go to the HTTP client and fail forever
- [ ] Announce `completed` when the download finishes, rather than at the next scheduled announce
- [ ] Tracker communication — UDP tracker (BEP 15)
- [ ] DHT (BEP 5) — decentralized peer discovery
- [ ] Magnet links (BEP 9/10) — metadata exchange
- [ ] Message Stream Encryption (BEP 8) — WebTorrent opens encrypted handshakes by default, so its outgoing connections cannot talk to us at all
- [ ] Rate limiting
- [ ] `models/` module — shared domain types

## Config

Config file lives at `~/.config/dhaar-torrent/config.toml` by default. TOML
format. Nothing configurable there yet — every knob is still a CLI flag.

## Build

```sh
cargo build --release
```

Requires Rust (stable, edition 2024).

## License

[MIT](LICENSE) — Piyush Raj
