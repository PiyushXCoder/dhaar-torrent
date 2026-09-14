#!/usr/bin/env bash
#
# Download throughput against a swarm of 1, 2, 4 and 8 seeders.
#
# The question is how much a leecher gains from extra sources. Everything runs
# on loopback with a payload that fits in page cache, so neither the network
# nor the disk is the limit -- what varies between arms is only the number of
# peers the client can pull from at once.
#
# Every seeder starts from a complete .dhaar store and reaches 100% before the
# leecher is launched, so no run is measuring a seeder that is still warming
# up. The tracker hands its peer list to the leecher alone (see below), which
# means every connection in a run is leecher -> seeder: seeders never dial each
# other, and the arm labelled "4 seeders" really is four upload sources.
#
# Timing runs from launching the leecher to the moment its finished payload
# appears on disk, so it includes process startup, the first tracker announce
# and the handshakes. Those are real costs and roughly constant in seconds, so
# they weigh most on the fastest arm -- the eight-seeder figure is if anything
# a floor, and the payload wants to stay large enough to keep that share small.
#
# Usage: scripts/bench-seeders.sh [--keep]
#          --keep   leave the work directory, logs and CSV behind
#
# Knobs, all environment variables:
#   PAYLOAD_MB=128      payload size; the fast arms finish in seconds, so a
#                       smaller payload leaves startup dominating them
#   REPEATS=3           runs per arm; the table reports the median
#   SEEDER_COUNTS="1 2 4 8"
#   PIECE_LENGTH=262144
#   TIMEOUT=180         seconds allowed for one download
#   TRACKER_PORT / LEECHER_PORT / SEEDER_PORT_BASE

set -euo pipefail

PAYLOAD_MB=${PAYLOAD_MB:-128}
REPEATS=${REPEATS:-3}
SEEDER_COUNTS=${SEEDER_COUNTS:-"1 2 4 8 16 32"}
PIECE_LENGTH=${PIECE_LENGTH:-262144}
TIMEOUT=${TIMEOUT:-180}
TRACKER_PORT=${TRACKER_PORT:-8100}
LEECHER_PORT=${LEECHER_PORT:-6890}
SEEDER_PORT_BASE=${SEEDER_PORT_BASE:-6891}

PAYLOAD_SIZE=$((PAYLOAD_MB * 1024 * 1024))

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
BIN="$REPO/target/release/dhaar-torrent"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/dhaar-bench.XXXXXX")
CSV="$WORK/results.csv"

MAX_SEEDERS=0
for n in $SEEDER_COUNTS; do
    [ "$n" -gt "$MAX_SEEDERS" ] && MAX_SEEDERS=$n
done

TRACKER_PID=""
LEECHER_PID=""
SEEDER_PIDS=()

kill_swarm() {
    for pid in "$LEECHER_PID" "${SEEDER_PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    LEECHER_PID=""
    SEEDER_PIDS=()
}

kill_tracker() {
    [ -n "$TRACKER_PID" ] && kill "$TRACKER_PID" 2>/dev/null || true
    TRACKER_PID=""
}

cleanup() {
    kill_swarm
    kill_tracker
    wait 2>/dev/null || true
    if [ "$KEEP" = "1" ]; then
        echo "logs and results kept in $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

wait_for_port() {
    local port=$1
    for _ in $(seq 100); do
        ss -ltn 2>/dev/null | grep -q ":$port " && return 0
        sleep 0.1
    done
    return 1
}

for port in $TRACKER_PORT $LEECHER_PORT \
            $(seq $SEEDER_PORT_BASE $((SEEDER_PORT_BASE + MAX_SEEDERS - 1))); do
    if ss -ltn 2>/dev/null | grep -q ":$port "; then
        fail "port $port is already in use"
    fi
done

# --- a tracker ---------------------------------------------------------------
# Same minimal announce responder as scripts/test-inbound.sh, with one change:
# the peer list goes to the leecher's port and nobody else. A seeder that is
# handed peers will dial them, and seeder-to-seeder chatter would put
# connections in the swarm that have nothing to do with the number under test.

cat > "$WORK/tracker.py" <<'PYEOF'
"""Minimal HTTP BitTorrent tracker that answers one designated leecher."""
import socket, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit, unquote_to_bytes

LEECHER_PORT = int(sys.argv[2])
swarms = {}

class Tracker(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        sys.stderr.write("tracker: " + fmt % a + "\n")

    def do_GET(self):
        parts = urlsplit(self.path)
        if not parts.path.startswith('/announce'):
            self.send_error(404); return

        q = {}
        for pair in parts.query.split('&'):
            if '=' in pair:
                k, v = pair.split('=', 1)
                q[unquote_to_bytes(k).decode('latin1')] = unquote_to_bytes(v)

        info_hash = q.get('info_hash', b'')
        port = int(q.get('port', b'0'))
        ip = self.client_address[0]

        swarm = swarms.setdefault(info_hash, {})
        if q.get('event', b'').decode('latin1') == 'stopped':
            swarm.pop((ip, port), None)
        elif port:
            swarm[(ip, port)] = q.get('peer_id', b'')

        # Compact format: 4-byte IPv4 + 2-byte big-endian port, self excluded.
        peers = [p for p in swarm if p != (ip, port)] if port == LEECHER_PORT else []
        blob = b''.join(socket.inet_aton(h) + p.to_bytes(2, 'big') for (h, p) in peers)
        body = b'd8:intervali30e5:peers%d:%se' % (len(blob), blob)

        self.log_message("announce port=%d -> returned %d peer(s)", port, len(peers))
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

HTTPServer(('127.0.0.1', int(sys.argv[1])), Tracker).serve_forever()
PYEOF

# --- a torrent, and one complete store to clone the seeders from -------------
# A .dhaar store is [payload][bitfield][info_hash]; a complete one makes a
# seeder resume at 100% without ever needing a peer. `finalize` copies out of
# that store rather than consuming it, so the same directory can be reused for
# every run of every arm.

cat > "$WORK/mkfixture.py" <<'PYEOF'
import hashlib, os, sys

work, piece_len, announce = sys.argv[1], int(sys.argv[2]), sys.argv[3]
payload = open(os.path.join(work, 'payload.bin'), 'rb').read()

def benc(v):
    if isinstance(v, int):   return b'i%de' % v
    if isinstance(v, bytes): return b'%d:%s' % (len(v), v)
    if isinstance(v, str):   return benc(v.encode())
    if isinstance(v, list):  return b'l' + b''.join(benc(x) for x in v) + b'e'
    if isinstance(v, dict):
        return b'd' + b''.join(benc(k) + benc(v[k]) for k in sorted(v)) + b'e'
    raise TypeError(v)

pieces = [payload[i:i+piece_len] for i in range(0, len(payload), piece_len)]
info = {
    'name': b'payload.bin',
    'piece length': piece_len,
    'pieces': b''.join(hashlib.sha1(p).digest() for p in pieces),
    'length': len(payload),
}
# `creation date` is not optional in practice: the field pairs Option with a
# deserialize_with, and serde only treats Option as absent-able given a default.
torrent = {'announce': announce.encode(), 'info': info,
           'creation date': 1757000000, 'created by': b'bench-seeders.sh'}
open(os.path.join(work, 'bench.torrent'), 'wb').write(benc(torrent))

bitfield = bytearray((len(pieces) + 7) // 8)
for i in range(len(pieces)):
    bitfield[i // 8] |= 0x80 >> (i % 8)

with open(os.path.join(work, 'template', 'payload.bin.dhaar'), 'wb') as f:
    f.write(payload)
    f.write(bitfield)
    f.write(hashlib.sha1(benc(info)).digest())

print(len(pieces))
PYEOF

# --- set up ------------------------------------------------------------------
# Release build: a debug build measures rustc's bounds checks, not the client.

echo "workdir: $WORK"

echo "building (release)"
cargo build --release --quiet --manifest-path "$REPO/Cargo.toml"

mkdir -p "$WORK/template" "$WORK/leecher"
: > "$WORK/empty.toml"   # the client currently requires a config file to exist
head -c "$PAYLOAD_SIZE" /dev/urandom > "$WORK/payload.bin"
WANT=$(sha256sum < "$WORK/payload.bin" | cut -d' ' -f1)

PIECES=$(python3 "$WORK/mkfixture.py" "$WORK" "$PIECE_LENGTH" \
    "http://127.0.0.1:$TRACKER_PORT/announce")
echo "payload ${PAYLOAD_MB} MiB in $PIECES pieces of $PIECE_LENGTH bytes"

for i in $(seq 0 $((MAX_SEEDERS - 1))); do
    mkdir -p "$WORK/seeder$i"
    cp --reflink=auto "$WORK/template/payload.bin.dhaar" "$WORK/seeder$i/"
done

echo "seeders,run,seconds,MB_per_s,peers_seen,seeders_uploading" > "$CSV"

# --- one run -----------------------------------------------------------------
# $1 seeders, $2 run index. Appends a row to the CSV, or exits on a bad run:
# a wrong hash or a short peer count means the number would be meaningless.

run_once() {
    local n=$1 run=$2 i port t0 t1 secs rate peers uploading handed got

    # A fresh tracker each run, so a peer left over from the previous run
    # cannot be handed out on a port that has since been reused.
    python3 -u "$WORK/tracker.py" "$TRACKER_PORT" "$LEECHER_PORT" \
        > "$WORK/tracker.log" 2>&1 &
    TRACKER_PID=$!
    wait_for_port "$TRACKER_PORT" || fail "tracker did not start"

    for i in $(seq 0 $((n - 1))); do
        port=$((SEEDER_PORT_BASE + i))
        ( cd "$WORK/seeder$i" && RUST_LOG=dhaar_torrent=info exec "$BIN" \
            ../bench.torrent -c ../empty.toml -l "$port" ) \
            > "$WORK/seeder$i.log" 2>&1 &
        SEEDER_PIDS+=($!)
    done

    # Every seeder listening *and* reporting a full piece count, so the leecher
    # never waits on one that is still verifying what it holds. The count is
    # the signal rather than the state: a resumed store completes no piece
    # during the run, so such a seeder sits at "Downloading 100.0%" forever
    # and never announces itself as Seeding.
    for i in $(seq 0 $((n - 1))); do
        wait_for_port $((SEEDER_PORT_BASE + i)) || fail "seeder $i did not listen"
        for _ in $(seq $((TIMEOUT * 10))); do
            grep -q "$PIECES/$PIECES pieces" "$WORK/seeder$i.log" 2>/dev/null && break
            sleep 0.1
        done
        grep -q "$PIECES/$PIECES pieces" "$WORK/seeder$i.log" \
            || fail "seeder $i never reported a complete store"
    done

    rm -f "$WORK/leecher/payload.bin" "$WORK/leecher/payload.bin.dhaar"

    t0=$(date +%s.%N)
    ( cd "$WORK/leecher" && RUST_LOG=dhaar_torrent=info exec "$BIN" \
        ../bench.torrent -c ../empty.toml -l "$LEECHER_PORT" ) \
        > "$WORK/leecher.log" 2>&1 &
    LEECHER_PID=$!

    # `finalize` creates the payload the instant the last piece verifies, which
    # is a far sharper edge than the status line -- that is only published on a
    # one-second tick and would blur every measurement by up to a second.
    for _ in $(seq $((TIMEOUT * 20))); do
        [ -e "$WORK/leecher/payload.bin" ] && break
        sleep 0.05
    done
    t1=$(date +%s.%N)
    [ -e "$WORK/leecher/payload.bin" ] \
        || fail "$n seeders, run $run: no download after ${TIMEOUT}s"

    # finalize streams the whole payload out of the store after the last piece
    # verifies. Let that copy land before killing anything, or the hash below
    # reads a file the leecher was still writing.
    for _ in $(seq 400); do
        [ "$(stat -c %s "$WORK/leecher/payload.bin")" = "$PAYLOAD_SIZE" ] && break
        sleep 0.05
    done

    # Two views of how many sources the run really had. Peers-seen comes from
    # the leecher's status line, which is only sampled once a second, so a
    # short run can miss connections that were live the whole time -- it is
    # reported, not asserted on. How many peers the tracker handed over is
    # exact, and a run that got fewer than it asked for is not the arm it
    # claims to be.
    peers=$(grep -oE '\| [0-9]+ peers' "$WORK/leecher.log" \
            | grep -oE '[0-9]+' | sort -n | tail -1)
    peers=${peers:-0}

    uploading=0
    for i in $(seq 0 $((n - 1))); do
        grep -qE 'up [1-9][0-9]* KiB/s' "$WORK/seeder$i.log" \
            && uploading=$((uploading + 1))
    done

    kill_swarm
    kill_tracker
    wait 2>/dev/null || true

    handed=$(grep -oE "port=$LEECHER_PORT -> returned [0-9]+ peer" "$WORK/tracker.log" \
             | grep -oE '[0-9]+ peer' | grep -oE '[0-9]+' | sort -n | tail -1)
    [ "${handed:-0}" -ge "$n" ] \
        || fail "$n seeders, run $run: tracker offered only ${handed:-0} peer(s)"

    got=$(sha256sum < "$WORK/leecher/payload.bin" | cut -d' ' -f1)
    [ "$WANT" = "$got" ] || fail "$n seeders, run $run: payload mismatch"

    secs=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f", b - a }')
    rate=$(awk -v s="$secs" -v b="$PAYLOAD_SIZE" 'BEGIN { printf "%.2f", b / s / 1000000 }')
    printf '%s,%s,%s,%s,%s,%s\n' \
        "$n" "$run" "$secs" "$rate" "$peers" "$uploading" >> "$CSV"
    printf '  run %d/%d: %6.2fs  %6.2f MB/s  (%d peers seen, %d/%d seeders uploaded)\n' \
        "$run" "$REPEATS" "$secs" "$rate" "$peers" "$uploading" "$n"
}

# --- run ---------------------------------------------------------------------

for n in $SEEDER_COUNTS; do
    echo
    echo "$n seeder(s), $REPEATS run(s) of ${PAYLOAD_MB} MiB"
    for run in $(seq "$REPEATS"); do
        run_once "$n" "$run"
    done
done

# --- report ------------------------------------------------------------------

echo
awk -F, -v mb="$PAYLOAD_MB" '
    NR > 1 { rates[$1] = rates[$1] " " $4; secs[$1] = secs[$1] " " $3 }
    function median(list,   a, k, i) {
        k = split(list, a, " ")
        # Insertion sort: k is at most a handful of runs.
        for (i = 2; i <= k; i++) {
            v = a[i]; j = i - 1
            while (j > 0 && a[j] > v) { a[j+1] = a[j]; j-- }
            a[j+1] = v
        }
        return (k % 2) ? a[(k+1)/2] : (a[k/2] + a[k/2+1]) / 2
    }
    END {
        n = asorti_keys(rates, keys)
        printf "%-9s %10s %12s %10s\n", "seeders", "median s", "median MB/s", "vs 1"
        for (i = 1; i <= n; i++) {
            k = keys[i]
            r = median(rates[k]); t = median(secs[k])
            if (base == 0) base = r
            printf "%-9s %10.2f %12.2f %9.2fx\n", k, t, r, r / base
        }
    }
    function asorti_keys(arr, out,   i, c, tmp, j, v) {
        c = 0
        for (i in arr) out[++c] = i + 0
        for (i = 2; i <= c; i++) {
            v = out[i]; j = i - 1
            while (j > 0 && out[j] > v) { out[j+1] = out[j]; j-- }
            out[j+1] = v
        }
        return c
    }
' "$CSV"

echo
echo "payload ${PAYLOAD_MB} MiB, $REPEATS run(s) per arm, release build, loopback"
echo "raw rows: $CSV"
[ "$KEEP" = "1" ] || echo "(pass --keep to preserve them)"
