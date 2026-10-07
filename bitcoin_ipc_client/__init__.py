"""Reusable external Bitcoin Core IPC client, independent of the indexer."""
from .client import BitcoinIPCClient
from .errors import BlockUnavailable, ConnectionLost, IPCError, RPCError, UnsupportedInterface
from .types import BlockData, BlockRef, ChainEvent

__all__ = [
    "BitcoinIPCClient", "BlockData", "BlockRef", "ChainEvent", "IPCError",
    "ConnectionLost", "UnsupportedInterface", "BlockUnavailable", "RPCError",
]
