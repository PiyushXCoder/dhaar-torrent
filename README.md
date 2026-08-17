![cover](assets/cover.png)

# Dhaar Torrent _(धार टॉरेंट)_

A torrent client written in Rust. Unserious. Built for fun.

## Status

~40% complete. Bencode codec, torrent file parsing, and tracker announce done. Peer wire protocol (handshake + choke/unchoke/interested/have/bitfield/request/piece) implemented with a basic single-block-at-a-time download loop; pipelining, timeouts, and DHT still missing.

### TODO

- [x] CLI args and config parsing (clap + TOML with merge)
- [x] Bencode deserializer (serde-based: integers, strings, bytes, lists, dicts, `Raw<T>`)
- [x] Bencode serializer (serde-based: integers, strings, bytes, lists, dicts)
- [x] Torrent file parsing (single and multi-file structs, raw `info` capture via serde)
- [x] Info hash computation (SHA-1 of bencoded `info` dict; hex and URL-safe forms)
- [x] Chrono datetime support in bencode (unix timestamp serde)
- [x] Logging/tracing — add `tracing` + `tracing-subscriber` with env-filter
- [x] Tracker announce — HTTP GET request, URL rotation, retry with backoff
- [x] Tracker response — support binary model peers (6-byte entries)
- [x] Peer wire protocol — TCP handshake, choke/unchoke, interested, have, bitfield, request/piece messages
- [x] Piece manager — piece indices, bitfield tracking, sequential block download/upload (no pipelining yet)
- [ ] Connection timeouts (connect/handshake/read) and periodic keep-alive
- [ ] Request pipelining (multiple outstanding block requests per peer)
- [ ] Disk I/O — writing verified pieces to disk done; resume support (recovering already-downloaded pieces on restart) pending
- [ ] `Download` wrapper struct — pull the wiring out of `main.rs` (currently cluttered)
- [ ] Status/progress events — download internals report events back to `main` instead of being silent
- [ ] CLI progress bar driven by those events
- [ ] Tracker communication — UDP tracker (BEP 15)
- [ ] DHT (BEP 5) — decentralized peer discovery
- [ ] Magnet links (BEP 9/10) — metadata exchange
- [ ] Upload/seeding
- [ ] Rate limiting
- [ ] `lib.rs` for library API
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

## Config

Config file lives at `~/.config/dhaar-torrent/config.toml` by default. TOML format.

## Build

```sh
cargo build --release
```

Requires Rust (stable).

## License

[MIT](LICENSE) — Piyush Raj
