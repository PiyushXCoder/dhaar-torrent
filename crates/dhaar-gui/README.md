# dhaar-gui

The reference client for the `dhaar-torrent` library.

The library is the point of this repository; this crate is what using it looks
like from the outside. It is meant to be genuinely usable — pick a torrent,
watch it run, add another — while staying small enough to read in one sitting.
If something here starts to feel clever, it probably belongs in the library
instead.

## Running it

```sh
cargo run -p dhaar-gui
```

Press **Add torrent** to choose a `.torrent` file. Add as many as you like;
each row shows progress, transfer rates, connected peers, pieces in flight,
and how many bytes were fetched and then thrown away. **Remove** stops a
download and drops it from the list.

A path can still be passed on the command line if you prefer, but the picker
is the normal route.

## What it demonstrates

Starting a download is three lines, because the library is meant to be usable
in about that much:

```rust
let download = Download::from_torrent_file(path)?;
let _guard = runtime.enter();
let handle = download.spawn();   // returns immediately
```

Four things worth noticing, since they are the parts that were designed rather
than assembled:

- **One runtime, many downloads.** The client owns a single tokio runtime and
  spawns every download's actors onto it. `Download::spawn` returns at once, so
  adding a torrent never blocks the interface, and downloads are not paying for
  a thread each.

- **The interface never runs on that runtime.** iced has its own event loop.
  The only thing crossing between the two is `DownloadHandle`, whose status can
  be read without a runtime at all — which is why the view can just ask each
  handle where it is up to.

- **Removing a row *is* stopping the download.** `DownloadHandle` aborts its
  actors when dropped, so there is no separate teardown path to forget to call.

- **Torrents are identified by info hash, not by filename.** The same content
  added twice from two different `.torrent` files is caught.

## What it does not do yet

No pause or resume, no settings, no per-peer detail, no choice of where files
land, no persistence across restarts, and no attempt at looking good.

Per-peer detail is the one with a real dependency: the library aggregates its
status today, and exposing individual peers needs a registry it does not keep.
The rest is client-side work.
