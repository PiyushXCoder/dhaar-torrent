![cover](assets/cover.png)

# Dhaar Torrent _(धार टॉरेंट)_

A torrent client written in Rust. Unserious. Built for fun.

## Status

~25% complete. Bencode codec and torrent file parsing done. Tracker announce started, peer networking not started.

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
- [ ] Tracker communication — UDP tracker (BEP 15)
- [ ] Peer wire protocol — TCP handshake, choke/unchoke, interested, request messages
- [ ] Piece manager — piece indices, bitfield tracking, download/upload queues
- [ ] Disk I/O — writing verified pieces to disk, resume support
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
