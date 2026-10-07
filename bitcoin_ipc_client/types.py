"""Owned results, chain events, and Bitcoin hash conversions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from .errors import IPCError

@dataclass(frozen=True)
class BlockRef:
    hash: str
    height: int

@dataclass(frozen=True)
class BlockData:
    hash: str
    height: int
    time: int
    in_active_chain: bool
    raw: bytes

@dataclass(frozen=True)
class ChainEvent:
    kind: str
    hash: str | None = None
    previous_hash: str | None = None
    height: int | None = None
    historical: bool = False
    validated: bool = False
    raw: bytes = b""
    reason: str | None = None

def wire_hash(value: str) -> bytes:
    if len(value) != 64:
        raise ValueError("block hash must contain 64 hexadecimal characters")
    try:
        result = bytes.fromhex(value)
    except ValueError as exc:
        raise ValueError("invalid hexadecimal block hash") from exc
    if len(result) != 32:
        raise ValueError("invalid block hash")
    return result[::-1]

def display_hash(value: Any) -> str:
    owned = bytes(value)
    if len(owned) != 32:
        raise IPCError(f"expected a 32-byte hash, got {len(owned)} bytes")
    return owned[::-1].hex()

def hash256(raw: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(raw).digest()).digest()
