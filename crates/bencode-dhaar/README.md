# bencode-dhaar

A [serde](https://serde.rs) codec for bencode, the encoding BitTorrent uses for
`.torrent` files and tracker responses.

It was written for [`dhaar-torrent`](https://crates.io/crates/dhaar-torrent) and
still lives in that repository, but it depends on nothing from it and is
published on its own. If you need to read a torrent file and do not want a
whole client, this is the part you want.

## Using it

The two entry points are the ones serde crates usually have:

```rust
let bytes: Vec<u8> = bencode_dhaar::to_bytes(&value)?;
let value: Torrent = bencode_dhaar::from_bytes(&bytes)?;
```

Anything implementing `Serialize` or `Deserialize` works, so the shape of a
torrent file is just a struct with `#[derive(Deserialize)]` on it. Byte strings
are the one place bencode differs from what derive gives you by default —
bencode has no separate text type, so fields that hold arbitrary bytes rather
than UTF-8 want [`serde_bytes`](https://crates.io/crates/serde_bytes).

## `Raw<T>`, and why it exists

Bencode is canonical on paper and not in practice: a dictionary is supposed to
have its keys sorted, but a decode-then-re-encode round trip still loses
anything the struct did not model, and unknown keys are common in the wild.

That matters more than it sounds, because a torrent's info hash is the SHA-1 of
the `info` dictionary **as it was written**, not as you would write it. Get the
bytes from re-serializing your own struct and the hash comes out wrong, the
tracker does not know what you are asking for, and nothing works — with no
error anywhere to say why.

`Raw<T>` solves it by capturing the byte span a value was decoded from. It does
not hold the decoded value — `T` is a phantom marker saying what those bytes
are, and the bytes themselves are a public field:

```rust
#[derive(Deserialize)]
struct JustTheInfo {
    info: Raw<Info>,
}

let raw: JustTheInfo = bencode_dhaar::from_bytes(file)?;
let info_hash = Sha1::digest(raw.info.bytes);
```

So getting both the hash and the data means decoding twice — once through a
small struct that captures the span, once through the real one. That is exactly
what `dhaar-torrent` does, and the second pass is cheap next to the SHA-1.

`Raw<T>` is deserialize-only; there is no `Serialize` impl, because writing the
bytes back out is just writing the bytes back out. This is the same trick
`serde_json::value::RawValue` uses, and it is the main reason to reach for this
crate over another bencode implementation.

## Dates

Tracker and torrent timestamps are Unix seconds, not RFC 3339. The `chrono`
module is a serde adapter for that, used through `#[serde(with = ...)]` on an
`Option<DateTime<Utc>>` field.

## What it does not do

No `Value` type — there is no untyped tree to walk, so everything goes through
a type you have defined. No streaming: `from_bytes` takes a whole slice and
`to_bytes` returns a whole `Vec`, which is the right shape for torrent files
and the wrong one for a network stream you are still reading.
