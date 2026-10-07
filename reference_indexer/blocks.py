"""Decode block metadata and txids; this is not consensus validation."""

from __future__ import annotations

from dataclasses import dataclass
import struct

from bitcoin_ipc_client.types import hash256

class WireReader:
    def __init__(self, raw: bytes):
        self.raw = raw
        self.pos = 0

    def read(self, size: int) -> bytes:
        end = self.pos + size
        if size < 0 or end > len(self.raw):
            raise ValueError("truncated Bitcoin serialization")
        value = self.raw[self.pos:end]
        self.pos = end
        return value

    def compact(self) -> int:
        prefix = self.read(1)[0]
        if prefix < 253:
            return prefix
        size = {253: 2, 254: 4, 255: 8}[prefix]
        value = int.from_bytes(self.read(size), "little")
        minimum = {253: 253, 254: 65536, 255: 4294967296}[prefix]
        if value < minimum:
            raise ValueError("noncanonical CompactSize")
        if value > len(self.raw):
            raise ValueError("CompactSize exceeds available block data")
        return value

    def vector(self):
        self.read(self.compact())

@dataclass(frozen=True)
class DecodedBlock:
    hash: str
    previous_hash: str
    time: int
    txids: tuple[str, ...]

def decode_block(raw: bytes) -> DecodedBlock:
    reader = WireReader(raw)
    header = reader.read(80)
    count = reader.compact()
    if count == 0 or count > len(raw) // 10:
        raise ValueError("invalid transaction count")
    txids = []
    for _ in range(count):
        version = reader.read(4)
        witness = reader.raw[reader.pos:reader.pos + 1] == b"\x00"
        if witness and reader.read(2) != b"\x00\x01":
            raise ValueError("unsupported transaction witness flags")
        start = reader.pos
        inputs = reader.compact()
        if inputs == 0 or inputs > len(raw) // 41:
            raise ValueError("invalid transaction input count")
        for _ in range(inputs):
            reader.read(36)
            reader.vector()
            reader.read(4)
        outputs = reader.compact()
        if outputs > len(raw) // 9:
            raise ValueError("invalid transaction output count")
        for _ in range(outputs):
            reader.read(8)
            reader.vector()
        stripped = version + reader.raw[start:reader.pos]
        if witness:
            any_witness = False
            for _ in range(inputs):
                items = reader.compact()
                any_witness |= items != 0
                for _ in range(items):
                    reader.vector()
            if not any_witness:
                raise ValueError("superfluous witness serialization")
        stripped += reader.read(4)
        txids.append(hash256(stripped)[::-1].hex())
    if reader.pos != len(raw):
        raise ValueError("trailing block data")
    return DecodedBlock(hash256(header)[::-1].hex(), header[4:36][::-1].hex(),
                        struct.unpack_from("<I", header, 68)[0], tuple(txids))
