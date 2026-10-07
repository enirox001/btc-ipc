"""Small synthetic block fixtures for decoder and indexer tests."""

from __future__ import annotations

import struct

from bitcoin_ipc_client.types import hash256, wire_hash

def synthetic_block(previous_hash="00" * 32, nonce=0, *, witness=False):
    version = struct.pack("<I", 2)
    body = b"\x01" + bytes(32) + b"\xff" * 4 + b"\x01\x00" + b"\xff" * 4
    body += b"\x01" + struct.pack("<Q", 50 * 100_000_000) + b"\x01\x51"
    locktime = bytes(4)
    stripped = version + body + locktime
    tx = version + (b"\x00\x01" if witness else b"") + body
    tx += (b"\x01\x02\xaa\xbb" if witness else b"") + locktime
    header = struct.pack("<I", 1) + wire_hash(previous_hash) + hash256(stripped)
    header += struct.pack("<III", 1700000000, 0x207fffff, nonce)
    return header + b"\x01" + tx, hash256(stripped)[::-1].hex()
