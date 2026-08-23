![cover](assets/cover.png)

# Dhaar Torrent _(धार टॉरेंट)_

A torrent client written in Rust. Unserious. Built for fun.

## Status

~50% complete. Bencode codec, torrent file parsing, tracker announce, and the peer wire protocol are done. Downloading works end to end: peers are discovered over HTTP trackers, connections are handshaked and framed with a `tokio-util` codec, blocks are requested with pipelining (up to 8 outstanding requests per peer), completed pieces are SHA-1 verified and written to disk, and the finished download is split into its final file layout. The last piece of a torrent is short, and its block count, request lengths, hash check and disk reads are all sized to it rather than to the full piece length.

Still missing: keep-alive, resume across restarts, seeding, inbound connections, web seeds, DHT, UDP trackers, and magnet links.

### Known rough edges

- **Trackers are the only peer source.** `announce` is optional and `announce-list` alone is enough, but a torrent that ships neither — Arch Linux's ISO torrent, for example, which carries only a BEP 19 `url-list` of web seeds — parses fine and then finds no peers at all. It logs a warning and sits idle.
- **Nothing happens when the download finishes.** There is no completion state: the peer manager keeps dialing, connections go silent once no piece is interesting, the 60s idle timeout kills them, and each one is requeued for a retry 23 seconds later. The result is a steady stream of handshake failures in the log after the download is already on disk.
- **Outbound only.** There is no TCP listener, so no peer can ever connect to us.

### Architecture

Components are independent tokio tasks talking over mpsc channels:

- **`peer_explorer`** — owns peer sources (currently `TrackerManager` over HTTP) and streams discovered peers out
- **`peer_manager`** — pulls peers through a selection strategy, caps concurrency at 50 connections, isolates per-peer failures
- **`peer_connection`** — TCP connect, handshake, bitfield exchange, then hands the framed stream to `request_manager`
- **`request_manager`** — per-peer state machine (choke/interest, piece locking, pipelined block requests, idle/request timeouts)
- **`piece_manager`** — piece/bitfield bookkeeping, piece locking across peers, SHA-1 verification, writes via a `PieceWriter` trait (`DiskPieceWriter` is the disk impl)

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
- [x] Piece manager — piece indices, bitfield tracking, cross-peer piece locking, SHA-1 verification
- [x] Request manager — per-peer connection state machine, pulled out of `peer_connection`
- [x] Connection timeouts — handshake/bitfield timeouts, 60s idle timeout, 30s outstanding-request timeout
- [x] Request pipelining — up to 8 outstanding block requests per peer
- [x] Disk I/O — verified pieces written to a sparse `<name>.dhaar` temp file, split into final files on completion
- [x] `lib.rs` for library API
- [ ] Periodic keep-alive messages
- [ ] Completion state — stop dialing peers once every piece is verified
- [ ] Inbound connections — TCP listener (`PeerConnection::from_stream` exists, nothing calls it)
- [ ] Tracker reporting — real `left` and `started`/`completed`/`stopped` events (both are hardcoded today)
- [ ] Web seeds (BEP 19) — HTTP `url-list` sources for trackerless torrents
- [ ] Resume support — recover already-downloaded pieces from a partial `.dhaar` file on restart
- [ ] `Download` wrapper struct — pull the wiring out of `main.rs` (currently cluttered)
- [ ] Status/progress events — download internals report events back to `main` instead of being silent
- [ ] CLI progress bar driven by those events
- [ ] Tracker communication — UDP tracker (BEP 15)
- [ ] DHT (BEP 5) — decentralized peer discovery
- [ ] Magnet links (BEP 9/10) — metadata exchange
- [ ] Upload/seeding
- [ ] Rate limiting
- [ ] `models/` module — shared domain types

## Usage

```sh
dhaar-torrent <torrent_file> [OPTIONS]
```

### Options

| Flag                       | Description                                                          |
| -------------------------- | -------------------------------------------------------------------- |
| `-c, --config-file <PATH>` | Path to config file (default: `~/.config/dhaar-torrent/config.toml`) |

### Example

```sh
dhaar-torrent ubuntu.torrent
dhaar-torrent ubuntu.torrent --config-file ./my-config.toml
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
