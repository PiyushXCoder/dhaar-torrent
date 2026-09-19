![cover](https://raw.githubusercontent.com/PiyushXCoder/dhaar-torrent/master/assets/cover.png)

# Dhaar Torrent _(धार टॉरेंट)_

[![crates.io](https://img.shields.io/crates/v/dhaar-torrent.svg?logo=rust)](https://crates.io/crates/dhaar-torrent)
[![docs.rs](https://img.shields.io/docsrs/dhaar-torrent?logo=docsdotrs)](https://docs.rs/dhaar-torrent)
[![YouTube](https://img.shields.io/badge/YouTube-build%20log-FF0000?logo=youtube&logoColor=white)](https://www.youtube.com/playlist?list=PLCjPsGYL4lfFoyjCFFrf8qKf20SaW6qfz)

A torrent client written in Rust. Unserious. Built for fun.

![the reference GUI client, pulling an Ubuntu ISO at 12.2 MiB/s](https://raw.githubusercontent.com/PiyushXCoder/dhaar-torrent/master/assets/dhaar-gui.png)

## Status

~60% complete, and a working download end to end. Peers are discovered over HTTP
trackers, connections are handshaked and framed with a `tokio-util` codec, blocks
are requested with pipelining, completed pieces are SHA-1 verified and written to
disk, and the finished download is split into the torrent's real file layout. The
last piece is short, and its block count, request lengths, hash check and disk
reads are all sized to it rather than to the full piece length.

The tail of a download does not stall behind one slow peer: once every remaining
piece is spoken for, a second peer may take a piece somebody is already working,
and whichever finishes first wins. Sharing is capped at two peers per piece —
uncapped, peers pile onto whatever is nearly done and throw away more than the
stall costs. The duplicate piece is counted as wasted rather than cancelled
mid-flight, which on a realistic download comes to about 0.02% of the payload.

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
cargo bench                      # divan; component costs of the store path
```

### The machine

Every figure below comes from one box, and none of them travel:

| | |
| --- | --- |
| CPU | Intel Core i7-12700H, 20 logical cores, SHA-NI |
| RAM | 15 GiB |
| Store | ext4 on LUKS-encrypted NVMe |
| Kernel / rustc | 6.18 LTS / 1.98.1, release build |

Seeders and leecher all run on this one machine over loopback, so the numbers
describe what the client can do when nothing else is in its way — not what a
real swarm would give you. See the caveats below before quoting any of them.

### Peers

1 GiB payload, 256 KiB pieces, median of seven runs per arm:

| seeders | rate | vs 1 peer |
| ---: | ---: | ---: |
| 1 | 378 MB/s | 1.00x |
| 2 | 670 MB/s | 1.77x |
| 4 | 1088 MB/s | 2.88x |
| 8 | 1287 MB/s | 3.41x |

Runs after the first in each arm agree within 2–4%; the first is always slower,
because the store has just been written and nothing is in page cache yet. The
medians ignore that, which is the only reason seven runs rather than three.

**Eight peers is the last arm that measures the client.** Past it the harness
runs 17 to 33 client processes against 20 cores — 846 OS threads at the widest —
and the leecher starts competing with the seeders feeding it. Pinning the
seeders to four cores each recovers 38% of the thirty-two-peer arm, which is the
harness's overhead showing up as the client's. Those arms are not quoted here
because they say more about the box than the code.

Scaling is sub-linear and stops mattering around eight for a plainer reason: the
leecher peaks at 5 of 20 cores and the machine is not saturated at any arm, so
extra peers are not extra bandwidth — they are extra processes sharing one
loopback stack.

### Where a piece's time goes

Per completed 256 KiB piece, from `cargo bench --bench store`:

| step | cost |
| --- | ---: |
| SHA-1 verify (`hash_piece`) | 213 µs |
| Write the piece (`write_whole_piece_one_call`) | 80 µs |
| Record it in the bitfield (`record_piece`) | 11 µs |

The writes are one call rather than sixteen because a connection assembles a
whole piece before touching the store. Sixteen block-sized writes cost 252 µs
for the same bytes: `write_block_direct` is 5.4 µs of syscall wrapped in 14.6 µs
of `spawn_blocking` handoff, so three quarters of the old cost was thread
ceremony rather than I/O.

`read_block` is the figure with a tail rather than a median — 10.7 µs typically
and 2.2 ms at its worst, because most reads are page-cache hits and the
occasional one reaches the device. That spread is why the store path stays on a
blocking pool, and it is the number that matters when seeding something too
large to cache.

### Reading these honestly

- **Loopback is not a network.** Every peer here is a local process with
  effectively infinite bandwidth and no latency. The one-piece-at-a-time
  request window that limits a real peer never binds,
  so these figures are an upper bound the internet will not reproduce.
- **Name the filesystem.** `cargo bench` follows `TMPDIR`, which on most
  machines is tmpfs — where the disk path stops being a disk path. tmpfs and
  ext4 currently agree within a few percent, but that was a 5x gap before the
  per-piece flush came out and nothing guarantees it stays closed.
- **Know which numbers reproduce.** `hash_piece` lands within 1% run to run,
  because it is pure CPU. Anything touching the disk moves by tens of percent.
  Quote a disk figure to more than two significant figures and you are
  reporting the page cache's mood.
- **Only trust a difference bigger than the spread.** Arms agree within 2–4%
  here, so anything under about 10% is not a result yet. A single run proves
  nothing: the same arm has measured 492 and 1468 MB/s on this machine.
- **A microbenchmark only says a *function* got faster.** Whether a *download*
  got faster is a separate question. Removing the `spawn_blocking` around each
  write looked like a free 2% on two arms and cost 45% at eight peers, which
  only the end-to-end bench showed.

### Against a real link

The only measurement that says how much of a real connection actually gets
used. All of these come from one session against the Ubuntu 26.04 desktop ISO
(6.5 GB, 24,868 pieces) on a domestic link, tracker-only — no DHT, no PEX:

| | rate |
| --- | ---: |
| Link capacity (8 parallel HTTP streams) | 10.24 MB/s |
| Single HTTP stream from the mirror | 3.32 MB/s |
| **dhaar** | **8.96 MB/s** steady, 10.67 p90, 12.14 peak |
| Transmission 4.1.3, tracker-only, same window | 7.68 MB/s |

So the client saturates the link. Correctness was checked against the swarm
both ways: a full 6.5 GB download verified all 24,868 pieces against the
torrent's own hashes, and a part-finished store from the multi-piece window
verified all 14,603 it claimed — the check that matters for that change, since
several pieces are assembled at once and a misfiled block would land in the
wrong one.

That number was **1.05 MB/s** until a one-line constant changed, and the story
is worth keeping because the benchmark above is what hid it. A connection used
to assemble one piece at a time, which capped the window at sixteen blocks —
256 KiB — however high `MAX_REQUESTS` went, and left it silent for a full round
trip at every piece boundary while it hashed, wrote and claimed the next piece.
On loopback, where the round trip is 0.05 ms, neither costs anything: the
eight-peer arm reads 1370 MB/s with the old window and 1370 MB/s with the new
one. Against real peers it was the difference between 150 KB/s per peer and
saturating the line.

Letting a connection hold four pieces costs memory — peak RSS went from 195 to
313 MiB on the worst case here, thirty-two peers at an 8 MiB piece length,
because that is four buffers per connection rather than one. It also means a
dying connection strands four pieces instead of one.

Peer count still moves by a factor of four between runs an hour apart, and the
tracker is the only peer source, so treat the rate as the shape of one
afternoon rather than a constant.

## Architecture

Components are independent tokio tasks talking over mpsc channels. The split
that matters is between coordination and data: the piece manager decides *who
fetches what* and every connection does the fetching, hashing and writing
itself, so verification runs on as many cores as there are peers.

- **`Download`** — assembles every actor and their channels, spawns them, and hands back a `DownloadHandle` for status and shutdown
- **`peer_explorer`** — owns peer sources (currently `TrackerManager` over HTTP) and streams discovered peers out
- **`peer_manager`** — pulls peers through a selection strategy, caps concurrency at 50 connections, stops dialling once every piece is verified, and supervises the connection tasks directly: a task that panics or is dropped reports nothing, so its ending is observed rather than announced
- **`peer_connection`** — TCP connect, handshake, bitfield exchange, then hands the framed stream to `request_manager`
- **`request_manager`** — per-peer state machine (choke/interest, pipelined block requests, idle/request timeouts, `Have` announcements) and the data path: it assembles up to four pieces at once in memory, hashes each as its blocks land, verifies and writes them, reporting to `piece_manager` only once the bytes are down. Several pieces rather than one so the request window never drains while a piece is being finished
- **`piece_manager`** — the arbiter of who downloads what, and nothing else: it hands a peer a whole piece and registers the claim in one message, so two peers cannot take the same work in the gap between asking and being answered. It also owns the bitfield, the totals and the completion announcement. Payload never passes through it
- **`store`** — the download's one file, behind a `Store` trait (`DiskStore` is the disk impl). Shared by every connection: access is positional, and pieces occupy disjoint ranges, so two connections writing different pieces never address the same byte
- **`status`** — atomics for the counters that move too often to be worth a message, and a `watch` of piece progress the piece manager builds in one turn of its loop

Workspace crates: [`bencode-dhaar`](crates/bencode-dhaar) (serde codec) and
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
- [x] Piece manager — piece indices, bitfield tracking, atomic cross-peer piece claiming
- [x] Request manager — per-peer connection state machine, pulled out of `peer_connection`
- [x] Connection timeouts — handshake/bitfield timeouts, 150s idle timeout, 30s outstanding-request timeout
- [x] Request pipelining — several pieces in flight per peer, so the window survives a piece boundary
- [x] Per-connection data path — each peer buffers, hashes and writes its own piece, so verification scales with peer count
- [x] Disk I/O — verified pieces written to a sparse `<name>.dhaar` temp file, split into final files on completion
- [x] `lib.rs` for library API
- [x] Endgame mode — once every remaining piece is claimed, let a second peer take one too; capped at two per piece
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
