"""Historical synchronization and active-chain reconciliation after reorgs."""

from __future__ import annotations

import asyncio
import logging

from bitcoin_ipc_client import BitcoinIPCClient, BlockRef, IPCError, RPCError, UnsupportedInterface
from .blocks import decode_block
from .store import IndexStore

LOG = logging.getLogger("btc-ipc")

class SnapshotChanged(Exception):
    pass

async def reconcile(client: BitcoinIPCClient, store: IndexStore, *, mode: str = "auto") -> BlockRef:
    """Walk a hash-anchored snapshot, then repeat if the node's tip changed."""
    while True:
        snapshot = await client.tip()

        async def ancestor(height):
            if height == snapshot.height:
                return snapshot.hash
            if mode != "rpc":
                try:
                    return await client.experimental.ancestor(snapshot, height)
                except UnsupportedInterface:
                    if mode == "typed":
                        raise
            # RPC has no hash-anchored height query. Check that the tip still
            # matches before and after the height lookup; restart on change.
            if await client.tip() != snapshot:
                raise SnapshotChanged()
            try:
                value = await client.rpc("getblockhash", [height])
            except RPCError:
                # A reorg can shorten the chain between the pre-query check
                # and getblockhash, making a formerly valid height disappear.
                if await client.tip() != snapshot:
                    raise SnapshotChanged()
                raise
            if await client.tip() != snapshot:
                raise SnapshotChanged()
            return value

        try:
            genesis = await ancestor(0)
            store.bind_network(genesis)
            tip = store.tip()
            common = min(tip.height, snapshot.height) if tip else -1
            while common >= 0 and store.hash_at(common) != await ancestor(common):
                common -= 1
            if tip and common < tip.height:
                LOG.info("Rewind %d -> %d", tip.height, common)
                store.rewind(common)
            for height in range(common + 1, snapshot.height + 1):
                block_hash = await ancestor(height)
                block = await client.block(block_hash, mode=mode)
                decoded = await asyncio.to_thread(decode_block, block.raw)
                if decoded.hash != block_hash or block.height != height:
                    raise IPCError("Block identity disagrees with snapshot")
                store.append(decoded, height)
                if height % 100 == 0 or height == snapshot.height:
                    LOG.info("Indexed %d / %d %s", height, snapshot.height, block_hash)
            if await client.tip() == snapshot:
                return snapshot
        except SnapshotChanged:
            continue
