#!/usr/bin/env bash
#
# Runs the client with tokio-console telemetry switched on.
#
# Two things have to line up or the console connects and shows nothing:
#
#   --features console   pulls in `console-subscriber` and turns on
#                        `tokio/tracing`, which is what makes the runtime
#                        emit a span per task
#   --cfg tokio_unstable  the runtime's task instrumentation is behind this
#                        flag, and it has to be set for the whole build, not
#                        just this crate
#
# The flag changes the cfg for every dependency, so this build does not share
# artefacts with an ordinary `cargo build` -- expect a full rebuild the first
# time, and again the first time you go back.
#
# Built with the `profiling` profile, which is `release` plus symbols. This is
# not a detail: a debug build of this client measures at 4.53 MiB/s against
# 5.37 for an optimised one, and every per-poll figure the console reports is
# inflated by the optimiser being off. Profiling an unoptimised binary tells
# you about a program you do not ship.
#
# The client's own logs still go to the terminal; the console layer is added
# alongside the formatter rather than replacing it. RUST_LOG works as usual.
#
# Usage: scripts/console.sh <torrent-file> [extra client args...]
#
# Then, in another terminal:
#
#   tokio-console
#
# What to look for, in rough order of payoff:
#
#   Busy vs Idle       a task that is always idle is waiting on something; a
#                      task that is always busy is doing work on the runtime
#                      that probably belongs on `spawn_blocking`
#   Polls              a high poll count against little total busy time means
#                      the task is being woken and finding nothing to do
#   the warning list   the console flags tasks that block the executor for too
#                      long in one poll. This client hashes 256 KiB pieces and
#                      calls into the store from inside the piece manager loop,
#                      so that is the first place to look
#
# The console binds to 127.0.0.1:6669 by default; set TOKIO_CONSOLE_BIND to
# move it if that clashes.

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $(basename "$0") <torrent-file> [client args...]" >&2
    exit 1
fi

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)

command -v tokio-console >/dev/null \
    || echo "note: the tokio-console viewer is not on PATH (cargo binstall tokio-console)" >&2

export RUSTFLAGS="${RUSTFLAGS:-} --cfg tokio_unstable"
# Without this the console has no client events to show alongside the task
# view; keep it low so the terminal stays readable while the console is open.
export RUST_LOG="${RUST_LOG:-dhaar_torrent=info}"

echo "building with the console feature (first build after switching flags is a full one)"
cargo build --manifest-path "$REPO/Cargo.toml" --profile profiling --features console

echo "starting client; run 'tokio-console' in another terminal"
exec "$REPO/target/profiling/dhaar-torrent" "$@"
