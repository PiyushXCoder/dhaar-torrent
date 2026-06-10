# dhaar-torrent

![cover](assets/cover.png)

A torrent client written in Rust. Unserious. Built for fun.

## Status

Very early. Actual torrenting: coming eventually, maybe.

- [x] CLI args and config parsing
- [x] Bencode deserializer (serde-based: integers, strings, bytes, lists, dicts)
- [x] Bencode serializer (serde-based: integers, strings, bytes, lists, dicts)
- [ ] Torrent file parsing
- [ ] Tracker communication
- [ ] Peer wire protocol

## Usage

```sh
dhaar-torrent <torrent_file> [OPTIONS]
```

### Options

| Flag | Description |
|------|-------------|
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
