#!/usr/bin/env bash
#
# End-to-end check that a dhaar instance accepts connections it did not dial.
#
# Two instances share a private tracker on the loopback interface. The seeder
# starts with the whole payload already on disk and announces first, so when it
# asks the tracker for peers the swarm is empty and it has nobody to dial. The
# leecher starts second, learns about the seeder, and dials it. Every byte that
# moves therefore crosses a connection the seeder accepted rather than opened,
# which is the thing under test.
#
# Usage: scripts/test-inbound.sh [--keep]
#          --keep   leave the work directory and logs behind for inspection
#
# Exits non-zero on the first failed assertion.

set -euo pipefail

TRACKER_PORT=8000
SEEDER_PORT=6881
LEECHER_PORT=6882
PIECE_LENGTH=262144
PAYLOAD_SIZE=$((4 * 1024 * 1024))
TIMEOUT=30

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
BIN="$REPO/target/debug/dhaar-torrent"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/dhaar-inbound.XXXXXX")

TRACKER_PID=""
SEEDER_PID=""
LEECHER_PID=""

cleanup() {
    # Kill by recorded pid only. A pattern match on "tracker.py" would also
    # match this script's own command line and take the script down with it.
    for pid in "$LEECHER_PID" "$SEEDER_PID" "$TRACKER_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    if [ "$KEEP" = "1" ]; then
        echo "logs kept in $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  ok: $*"; }

for port in $TRACKER_PORT $SEEDER_PORT $LEECHER_PORT; do
    if ss -ltn 2>/dev/null | grep -q ":$port "; then
        fail "port $port is already in use"
    fi
done

# --- a tracker ---------------------------------------------------------------
# The published trackers on npm drag in a native WebRTC module that often will
# not build. Announce is a small enough protocol to just answer directly.

cat > "$WORK/tracker.py" <<'PYEOF'
"""Minimal HTTP BitTorrent tracker: compact peer lists, no persistence."""
import socket, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit, unquote_to_bytes

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
        blob = b''.join(socket.inet_aton(h) + p.to_bytes(2, 'big')
                        for (h, p) in swarm if (h, p) != (ip, port))
        body = b'd8:intervali30e5:peers%d:%se' % (len(blob), blob)

        self.log_message("announce port=%d -> returned %d peer(s)",
                         port, len(blob) // 6)
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

import sys as _s
HTTPServer(('127.0.0.1', int(_s.argv[1])), Tracker).serve_forever()
PYEOF

# --- a torrent, and a pre-seeded store for the seeder ------------------------
# `pnpx create-torrent` ignores its own -a and -l flags here and writes a UDP
# announce, which this client does not speak, so the file is built directly.
#
# A .dhaar store is [payload][bitfield][info_hash]. Handing the seeder a
# complete one makes it resume at 100% without ever needing a peer.

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
           'creation date': 1757000000, 'created by': b'test-inbound.sh'}
open(os.path.join(work, 'test.torrent'), 'wb').write(benc(torrent))

bitfield = bytearray((len(pieces) + 7) // 8)
for i in range(len(pieces)):
    bitfield[i // 8] |= 0x80 >> (i % 8)

with open(os.path.join(work, 'seeder', 'payload.bin.dhaar'), 'wb') as f:
    f.write(payload)
    f.write(bitfield)
    f.write(hashlib.sha1(benc(info)).digest())

print(len(pieces))
PYEOF

# --- set up ------------------------------------------------------------------

echo "building"
cargo build --quiet --manifest-path "$REPO/Cargo.toml"

mkdir -p "$WORK/seeder" "$WORK/leecher"
: > "$WORK/empty.toml"   # the client currently requires a config file to exist
head -c "$PAYLOAD_SIZE" /dev/urandom > "$WORK/payload.bin"

PIECES=$(python3 "$WORK/mkfixture.py" "$WORK" "$PIECE_LENGTH" \
    "http://127.0.0.1:$TRACKER_PORT/announce")
echo "payload $PAYLOAD_SIZE bytes in $PIECES pieces"

python3 -u "$WORK/tracker.py" "$TRACKER_PORT" > "$WORK/tracker.log" 2>&1 &
TRACKER_PID=$!

for _ in $(seq 50); do
    ss -ltn 2>/dev/null | grep -q ":$TRACKER_PORT " && break
    sleep 0.1
done
ss -ltn 2>/dev/null | grep -q ":$TRACKER_PORT " || fail "tracker did not start"

# --- run ---------------------------------------------------------------------
# Seeder first and alone in the swarm, so it cannot dial anyone.

echo "starting seeder on :$SEEDER_PORT (holds all $PIECES pieces)"
( cd "$WORK/seeder" && RUST_LOG=dhaar_torrent=debug exec "$BIN" \
    ../test.torrent -c ../empty.toml -l "$SEEDER_PORT" ) > "$WORK/seeder.log" 2>&1 &
SEEDER_PID=$!

for _ in $(seq 100); do
    grep -q "Tracker .* responded" "$WORK/seeder.log" 2>/dev/null && break
    sleep 0.1
done
grep -q "responded: 0 peers" "$WORK/seeder.log" \
    || fail "seeder was handed peers; it may dial out and invalidate the test"
pass "seeder announced into an empty swarm, so it has nobody to dial"

echo "starting leecher on :$LEECHER_PORT (holds nothing)"
( cd "$WORK/leecher" && RUST_LOG=dhaar_torrent=debug exec "$BIN" \
    ../test.torrent -c ../empty.toml -l "$LEECHER_PORT" ) > "$WORK/leecher.log" 2>&1 &
LEECHER_PID=$!

echo "waiting for transfer (timeout ${TIMEOUT}s)"
for _ in $(seq $((TIMEOUT * 10))); do
    grep -q "Seeding 100.0%" "$WORK/leecher.log" 2>/dev/null && break
    sleep 0.1
done

# --- assertions --------------------------------------------------------------

echo "checks:"

grep -q "Inbound handshake complete" "$WORK/seeder.log" \
    || fail "seeder logged no inbound handshake"
pass "seeder accepted a connection it did not dial"

grep -q "Outbound handshake complete with 127.0.0.1:$SEEDER_PORT" "$WORK/leecher.log" \
    || fail "leecher never dialled the seeder"
pass "leecher dialled the seeder"

grep -q "serving block" "$WORK/seeder.log" \
    || fail "seeder served no blocks over the inbound connection"
pass "seeder served blocks over the inbound connection"

grep -q "Seeding 100.0%" "$WORK/leecher.log" \
    || fail "leecher did not reach 100% within ${TIMEOUT}s"
pass "leecher reached 100%"

[ -f "$WORK/leecher/payload.bin" ] || fail "leecher wrote no output file"
want=$(sha256sum < "$WORK/payload.bin" | cut -d' ' -f1)
got=$(sha256sum < "$WORK/leecher/payload.bin" | cut -d' ' -f1)
[ "$want" = "$got" ] || fail "payload mismatch: want $want, got $got"
pass "downloaded payload matches the original"

# The seeder's only peer is inbound, so a zero here means inbound connections
# are not being counted -- it would sit at "0 peers" while visibly uploading.
grep -E "up [1-9][0-9]* KiB/s" "$WORK/seeder.log" | grep -qv "0 peers" \
    || fail "seeder reported 0 peers while uploading"
pass "seeder counted its inbound peer while uploading"

echo
echo "PASS"
