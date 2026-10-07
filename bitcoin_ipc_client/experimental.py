"""Adapters for experimental Chain methods; these are not stability promises."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .errors import BlockUnavailable
from .types import BlockData, BlockRef, display_hash, wire_hash

if TYPE_CHECKING:
    from .client import BitcoinIPCClient

class ExperimentalChain:
    """Convenience adapter; successfully using a method is no stability promise."""
    def __init__(self, client: BitcoinIPCClient):
        self.client = client

    async def find_block(self, block_hash: str, *, data: bool = True) -> BlockData:
        client = self.client
        chain = await client.chain_capability()
        response = await client.call(lambda: chain.findBlock(
            context=client.ctx, hash=wire_hash(block_hash),
            block={"wantHash": True, "wantHeight": True, "wantTime": True,
                   "wantInActiveChain": True, "wantData": data},
        ))
        if not response.result or not response.block.found:
            raise BlockUnavailable(f"Unknown block: {block_hash}")
        block = response.block
        raw = bytes(block.data) if data else b""
        if data and (len(raw) <= 80 or raw[:80] == bytes(80)):
            raise BlockUnavailable(f"Block data unavailable (possibly pruned): {block_hash}")
        return BlockData(display_hash(block.hash), int(block.height), int(block.time), bool(block.inActiveChain), raw)

    async def ancestor(self, tip: BlockRef, height: int) -> str:
        if not 0 <= height <= tip.height:
            raise ValueError("ancestor height is outside the snapshot")
        client = self.client
        chain = await client.chain_capability()
        response = await client.call(lambda: chain.findAncestorByHeight(
            context=client.ctx, blockHash=wire_hash(tip.hash), ancestorHeight=height,
            ancestor={"wantHash": True},
        ))
        if not response.result or not response.ancestor.found:
            raise BlockUnavailable(f"Ancestor unavailable at height {height}")
        return display_hash(response.ancestor.hash)
