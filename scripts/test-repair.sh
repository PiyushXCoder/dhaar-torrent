#!/usr/bin/env bash
#
# End-to-end check that a store left by a crash has its bitfield repaired.
#
# The store's trailing flag byte carries a clean bit, set only by `finalize` on
# a tidy exit. Finding it clear means the last process died holding the store,
# so the bitfield may claim pieces whose bytes never landed and every claim has
# to be re-hashed before it is believed.
#
# Both arms here are handed the *same* damaged store: a complete bitfield, but
# one piece whose bytes do not match its hash. They differ in one byte.
#
#   unclean (clean bit clear)  the client must re-hash, catch the bad piece and
#                              clear its bit -- 15/16, and the bitfield on disk
#                              is corrected in place
#   clean   (clean bit set)    the client must take the bitfield at its word and
#                              start at 16/16, damage and all
#
# The second arm is not a formality. It is what proves the flag byte is doing
# the gating rather than the repair running unconditionally, and it is why the
# clean bit must never be written anywhere but a tidy exit.
#
# No tracker runs: neither arm needs a peer, only a startup.
#
# Usage: scripts/test-repair.sh [--keep]
#          --keep   leave the work directory and logs behind for inspection
#
# Exits non-zero on the first failed assertion.

set -euo pipefail

PIECE_LENGTH=262144
PAYLOAD_SIZE=$((4 * 1024 * 1024))
# Which piece gets its bytes scribbled on. Any index below the piece count; not
# the last one, so the test does not also depend on short-piece handling.
CORRUPT_PIECE=7
PORT=6890
TIMEOUT=30

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
BIN="$REPO/target/debug/dhaar-torrent"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/dhaar-repair.XXXXXX")

CLIENT_PID=""

cleanup() {
    [ -n "$CLIENT_PID" ] && kill "$CLIENT_PID" 2>/dev/null || true
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

echo "building"
cargo build --manifest-path "$REPO/Cargo.toml" >/dev/null 2>&1 \
    || fail "cargo build failed"

mkdir -p "$WORK/unclean" "$WORK/clean"
: > "$WORK/empty.toml"

# --- a torrent, and two copies of one damaged store --------------------------

cat > "$WORK/mkfixture.py" <<'PYEOF'
import hashlib, os, sys

work, piece_len, corrupt = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
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
torrent = {'announce': b'http://127.0.0.1:1/announce', 'info': info,
           'creation date': 1757000000, 'created by': b'test-repair.sh'}
open(os.path.join(work, 'test.torrent'), 'wb').write(benc(torrent))

# The hashes above are of the *original* payload. The store gets a damaged copy,
# so piece `corrupt` is exactly one piece that cannot verify.
damaged = bytearray(payload)
start = corrupt * piece_len
for i in range(start, start + piece_len):
    damaged[i] ^= 0xFF

# A bitfield claiming everything, including the piece that is now a lie.
bitfield = bytearray((len(pieces) + 7) // 8)
for i in range(len(pieces)):
    bitfield[i // 8] |= 0x80 >> (i % 8)

# Bit 0 of the trailing byte is the clean bit. The two arms differ here and
# nowhere else.
for name, flags in (('unclean', b'\x00'), ('clean', b'\x01')):
    with open(os.path.join(work, name, 'payload.bin.dhaar'), 'wb') as f:
        f.write(damaged)
        f.write(bitfield)
        f.write(hashlib.sha1(benc(info)).digest())
        f.write(flags)

print('%d %d' % (len(pieces), len(bitfield)))
PYEOF

cat > "$WORK/readbits.py" <<'PYEOF'
"""Prints the store's bitfield as a run of 0s and 1s, one per piece."""
import sys

store, payload_len, bitfield_len, piece_count = (
    sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
with open(store, 'rb') as f:
    f.seek(payload_len)
    bits = f.read(bitfield_len)
print(''.join('1' if bits[i // 8] & (0x80 >> (i % 8)) else '0'
              for i in range(piece_count)))
PYEOF

head -c "$PAYLOAD_SIZE" /dev/urandom > "$WORK/payload.bin"
read -r PIECES BITFIELD_LEN < <(python3 "$WORK/mkfixture.py" \
    "$WORK" "$PIECE_LENGTH" "$CORRUPT_PIECE")
echo "payload $PAYLOAD_SIZE bytes in $PIECES pieces, piece $CORRUPT_PIECE corrupted"

if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
    fail "port $PORT is already in use"
fi

# --- run one arm -------------------------------------------------------------
# $1 directory, $2 the piece count the status line must settle on.

run_arm() {
    local dir=$1 want=$2
    ( cd "$WORK/$dir" && RUST_LOG=dhaar_torrent=info exec "$BIN" \
        ../test.torrent -c ../empty.toml -l "$PORT" ) > "$WORK/$dir.log" 2>&1 &
    CLIENT_PID=$!

    for _ in $(seq $((TIMEOUT * 10))); do
        grep -q "| $want/$PIECES pieces" "$WORK/$dir.log" 2>/dev/null && break
        sleep 0.1
    done

    kill "$CLIENT_PID" 2>/dev/null || true
    wait "$CLIENT_PID" 2>/dev/null || true
    CLIENT_PID=""
}

bits_of() {
    python3 "$WORK/readbits.py" "$WORK/$1/payload.bin.dhaar" \
        "$PAYLOAD_SIZE" "$BITFIELD_LEN" "$PIECES"
}

# The bitfield each arm should leave behind: all ones, but for the unclean arm
# a hole where the damaged piece was disowned.
want_unclean=$(python3 -c "print(''.join('0' if i == $CORRUPT_PIECE else '1' \
    for i in range($PIECES)))")
want_clean=$(python3 -c "print('1' * $PIECES)")

echo "starting client over the unclean store"
run_arm unclean $((PIECES - 1))

echo "starting client over the clean store"
run_arm clean "$PIECES"

# --- assertions --------------------------------------------------------------

echo "checks:"

grep -q "| $((PIECES - 1))/$PIECES pieces" "$WORK/unclean.log" \
    || fail "unclean store did not settle at $((PIECES - 1))/$PIECES; \
it reported $(grep -o "| [0-9]*/$PIECES pieces" "$WORK/unclean.log" | tail -1)"
pass "unclean store re-hashed and disowned the damaged piece"

got=$(bits_of unclean)
[ "$got" = "$want_unclean" ] \
    || fail "unclean bitfield on disk is $got, want $want_unclean"
pass "the repaired bitfield was written back to the store"

grep -q "| $PIECES/$PIECES pieces" "$WORK/clean.log" \
    || fail "clean store did not start at $PIECES/$PIECES"
pass "clean store was taken at its word, damage and all"

got=$(bits_of clean)
[ "$got" = "$want_clean" ] \
    || fail "clean bitfield on disk is $got, want $want_clean"
pass "no repair ran behind the clean bit"

echo
echo "PASS"
