![cover](assets/cover.png)

# Dhaar Torrent _(धार टॉरेंट)_

A torrent client written in Rust. Unserious. Built for fun.

![the reference GUI client, downloading two torrents](assets/dhaar-gui.png)

## Status

~60% complete. Bencode codec, torrent file parsing, tracker announce, and the peer wire protocol are done. Downloading works end to end: peers are discovered over HTTP trackers, connections are handshaked and framed with a `tokio-util` codec, blocks are requested with pipelining (up to 8 outstanding requests per peer), completed pieces are SHA-1 verified and written to disk, and the finished download is split into its final file layout. The last piece of a torrent is short, and its block count, request lengths, hash check and disk reads are all sized to it rather than to the full piece length.

The tail of a download no longer stalls behind one slow peer: once every remaining piece is spoken for, the same blocks are requested from several peers at once and the losers are cancelled as soon as somebody else delivers. Finished pieces are announced to every connected peer with `Have`, and the tracker is told the real `uploaded`/`downloaded`/`left` figures along with `started` and `completed` events.

The whole thing is a library. `Download` owns the wiring, hands back a handle, and reports live status — bytes, rates, peers, pieces in flight, wasted bytes, hash failures — which is what the GUI above is rendering.

Still missing: keep-alive, resume across restarts, seeding, inbound connections, web seeds, DHT, UDP trackers, and magnet links.

### Known rough edges

- **Trackers are the only peer source.** `announce` is optional and `announce-list` alone is enough, but a torrent that ships neither — Arch Linux's ISO torrent, for example, which carries only a BEP 19 `url-list` of web seeds — parses fine and then finds no peers at all. It logs a warning and sits idle.
- **Outbound only.** There is no TCP listener, so no peer can ever connect to us. Blocks are served to peers we dialled, but once a download completes the peer manager stops dialling, so in practice nothing is ever seeded.
- **A connection that panics leaks its slot.** Teardown is skipped on unwind and nothing observes the dropped `JoinHandle`, so the peer manager's connection count never comes back down. The piece itself is safe — a guard hands it back when dropped.
- **Nothing survives a restart.** A partial `.dhaar` file is not read back, so an interrupted download starts from zero.

### Architecture

Components are independent tokio tasks talking over mpsc channels:

- **`Download`** — assembles every actor and their channels, spawns them, and hands back a `DownloadHandle` for status and shutdown
- **`peer_explorer`** — owns peer sources (currently `TrackerManager` over HTTP) and streams discovered peers out
- **`peer_manager`** — pulls peers through a selection strategy, caps concurrency at 50 connections, isolates per-peer failures, stops dialling once every piece is verified
- **`peer_connection`** — TCP connect, handshake, bitfield exchange, then hands the framed stream to `request_manager`
- **`request_manager`** — per-peer state machine (choke/interest, pipelined block requests, idle/request timeouts, cancels and `Have` announcements)
- **`piece_manager`** — the sole arbiter of who downloads what: it picks a peer's piece, registers its blocks and reports back in a single message, so two peers cannot claim the same work in the gap between asking and taking. Also SHA-1 verification and writes via a `PieceWriter` trait (`DiskPieceWriter` is the disk impl)
- **`status`** — atomics for the counters that move too often to be worth a message, and a `watch` of piece progress the piece manager builds in one turn of its loop

Workspace crates: [`crates/bencode`](crates/bencode) (serde codec) and [`crates/dhaar-gui`](crates/dhaar-gui) (the reference client).

### TODO

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
- [x] Connection timeouts — handshake/bitfield timeouts, 60s idle timeout, 30s outstanding-request timeout
- [x] Request pipelining — up to 8 outstanding block requests per peer
- [x] Disk I/O — verified pieces written to a sparse `<name>.dhaar` temp file, split into final files on completion
- [x] `lib.rs` for library API
- [x] Endgame mode — once only a few blocks remain, request them from every peer at once and `Cancel` the losers, so one slow peer can no longer hold the tail for a full 30s request timeout
- [x] Completion state — stop dialing peers once every piece is verified
- [x] Tracker reporting — real `uploaded`/`downloaded`/`left` and `started`/`completed` events
- [x] `Download` wrapper struct — pull the wiring out of `main.rs`
- [x] Status/progress — live counters and a sampled `DownloadStatus` feed, instead of the internals being silent
- [x] GUI client — iced, with a file picker and several downloads at once
- [ ] Periodic keep-alive messages
- [ ] Supervise connection tasks — reclaim the peer slot when one panics
- [ ] `stopped` tracker event on shutdown
- [ ] Inbound connections — TCP listener (`PeerConnection::from_stream` exists, nothing calls it)
- [ ] Web seeds (BEP 19) — HTTP `url-list` sources for trackerless torrents
- [ ] Resume support — recover already-downloaded pieces from a partial `.dhaar` file on restart
- [ ] Per-peer status — the library aggregates today and keeps no peer registry
- [ ] Pause and resume a running download
- [ ] Tracker communication — UDP tracker (BEP 15)
- [ ] DHT (BEP 5) — decentralized peer discovery
- [ ] Magnet links (BEP 9/10) — metadata exchange
- [ ] Upload/seeding
- [ ] Rate limiting
- [ ] `models/` module — shared domain types

## Usage

### GUI

```sh
cargo run -p dhaar-gui
```

Press **Add torrent** to choose a `.torrent` file, and add as many as you like. Paths given on the command line start immediately. See [`crates/dhaar-gui`](crates/dhaar-gui) for what it does and does not do.

### CLI

```sh
dhaar-torrent <torrent_file> [OPTIONS]
```

| Flag                       | Description                                                          |
| -------------------------- | -------------------------------------------------------------------- |
| `-c, --config-file <PATH>` | Path to config file (default: `~/.config/dhaar-torrent/config.toml`) |

```sh
dhaar-torrent ubuntu.torrent
dhaar-torrent ubuntu.torrent --config-file ./my-config.toml
```

Progress is logged once a second.

### Library

```rust
use dhaar_torrent::Download;
use std::path::Path;

let download = Download::from_torrent_file(Path::new("ubuntu.torrent"))?;
let mut updates = download.subscribe();  // taken before starting, so nothing is missed

// Held for as long as the download should live: dropping it stops the download.
let _handle = download.spawn();          // returns immediately

while updates.changed().await.is_ok() {
    println!("{:.1}%", updates.borrow().progress() * 100.0);
}
```

Downloads land in the current working directory. While in flight the data lives in a single `<name>.dhaar` file; once every piece verifies, it is split into the torrent's real file layout.

Set `RUST_LOG` to control log output:

```sh
RUST_LOG=dhaar_torrent=debug dhaar-torrent ubuntu.torrent
```

## Config

Config file lives at `~/.config/dhaar-torrent/config.toml` by default. TOML format. Nothing configurable there yet — every knob is still a CLI flag.

## Build

```sh
cargo build --release
```

Requires Rust (stable, edition 2024).

## License

[MIT](LICENSE) — Piyush Raj
