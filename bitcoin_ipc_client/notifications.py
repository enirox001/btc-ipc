"""Python capability servers for callbacks over the existing IPC connection."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .types import ChainEvent, display_hash

if TYPE_CHECKING:
    from .client import BitcoinIPCClient

LOG = logging.getLogger("btc-ipc")

def make_callback_servers(modules: dict, client: BitcoinIPCClient):
    class Thread(modules["proxy"].Thread.Server):
        def __init__(self, name):
            self.name = name

        async def getName(self, **kwargs):
            return self.name

    class ThreadMap(modules["proxy"].ThreadMap.Server):
        async def makeThread(self, name, **kwargs):
            return Thread(str(name))

        async def makePool(self, count, **kwargs):
            # All Python callback dispatch happens on this asyncio/KJ loop.
            return None

    class Notifications(modules["chain"].ChainNotifications.Server):
        async def destroy(self, context, **kwargs):
            return None

        async def transactionAddedToMempool(self, context, tx, **kwargs):
            return None

        async def transactionRemovedFromMempool(self, context, tx, reason, **kwargs):
            return None

        def emit_block(self, kind, block, role=None):
            try:
                client.emit(ChainEvent(
                    kind, display_hash(block.hash),
                    display_hash(block.prevHash) if len(block.prevHash) else None,
                    int(block.height),
                    bool(role.historical) if role is not None else False,
                    bool(role.validated) if role is not None else False,
                    bytes(block.data),
                ))
            except Exception as exc:
                client.interrupt_events(f"callback decoding failed: {exc}")
                LOG.exception("Callback decoding failed; reconciliation required")

        async def blockConnected(self, context, role, block, **kwargs):
            self.emit_block("connected", block, role)

        async def blockDisconnected(self, context, block, **kwargs):
            self.emit_block("disconnected", block)

        async def updatedBlockTip(self, context, **kwargs):
            client.emit(ChainEvent("tip"))

        async def chainStateFlushed(self, context, role, locator, **kwargs):
            return None

    return ThreadMap(), Thread("python-callback"), Notifications()
