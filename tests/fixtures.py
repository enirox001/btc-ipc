"""Small synthetic block fixtures for decoder and indexer tests."""

from __future__ import annotations

import struct

from bitcoin_ipc_client import BlockData, BlockRef, RPCError
from bitcoin_ipc_client.types import hash256, wire_hash
from reference_indexer.blocks import decode_block

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


class SyntheticNode:
    """Retains orphan blocks and provides immutable hash-anchored ancestry."""
    def __init__(self):
        self.active = []
        self.blocks = {}
        self.ancestry = {}
        self.nonce = 0
        self.experimental = self
        self.on_block = None
        self.extend(1)

    def extend(self, count):
        for _ in range(count):
            previous = self.active[-1] if self.active else "00" * 32
            raw, _ = synthetic_block(previous, self.nonce, witness=bool(self.nonce % 2))
            self.nonce += 1
            decoded = decode_block(raw)
            height = len(self.active)
            self.active.append(decoded.hash)
            self.blocks[decoded.hash] = BlockData(decoded.hash, height, decoded.time, True, raw)
            self.ancestry[decoded.hash] = tuple(self.active)

    def reorg(self, depth, replacements):
        if depth:
            del self.active[-depth:]
        self.extend(replacements)

    async def tip(self):
        return BlockRef(self.active[-1], len(self.active) - 1)

    async def ancestor(self, snapshot, height):
        return self.ancestry[snapshot.hash][height]

    async def block(self, block_hash, *, mode="auto"):
        if self.on_block is not None:
            hook, self.on_block = self.on_block, None
            hook()
        return self.blocks[block_hash]

    async def rpc(self, method, params):
        if method != "getblockhash":
            raise AssertionError(f"Unexpected RPC method: {method}")
        height = params[0]
        if not 0 <= height < len(self.active):
            raise RPCError({"code": -8, "message": "Block height out of range"})
        return self.active[height]
