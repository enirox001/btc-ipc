"""Client regression tests without node sockets or native dependencies."""
from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
import unittest

from bitcoin_ipc_client import BitcoinIPCClient, ChainEvent, BlockUnavailable, ConnectionLost, IPCError, UnsupportedInterface
from bitcoin_ipc_client.notifications import make_callback_servers
from bitcoin_ipc_client.types import display_hash, wire_hash
from reference_indexer.blocks import decode_block
from tests.fixtures import synthetic_block

LOG = logging.getLogger("btc-ipc")

class ClientTests(unittest.IsolatedAsyncioTestCase):
    def test_hash_roundtrip(self):
        value = "0123456789abcdef" * 4
        self.assertEqual(display_hash(wire_hash(value)), value)
        for invalid in ("ab", "z" * 64, " " * 64):
            with self.assertRaises(ValueError):
                wire_hash(invalid)

    def test_bounded_event_delivery(self):
        client = BitcoinIPCClient("unused", queue_size=1)
        client.emit(ChainEvent("tip"))
        client.emit(ChainEvent("connected"))
        self.assertEqual(client.events.get_nowait().kind, "interrupted")
        client.interrupt_events("failure")
        self.assertEqual(client.events.get_nowait().reason, "failure")

    async def test_call_serialization_and_timeout(self):
        client = BitcoinIPCClient("unused", timeout=1)
        client.connected = True
        active = 0
        maximum = 0

        async def request():
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(.001)
                return 42
            finally:
                active -= 1

        self.assertEqual(await asyncio.gather(*(client.call(request) for _ in range(4))), [42] * 4)
        self.assertEqual(maximum, 1)
        client.timeout = .01
        with self.assertRaises(ConnectionLost):
            await client.call(lambda: asyncio.sleep(1))
        self.assertFalse(client.connected)
        self.assertEqual(client.events.get_nowait().kind, "interrupted")

    async def test_rpc_fallback_is_selective(self):
        client = BitcoinIPCClient("unused")
        raw, _ = synthetic_block()
        decoded = decode_block(raw)
        calls = []

        async def unsupported(*args, **kwargs):
            raise UnsupportedInterface("unsupported")

        async def rpc(method, params):
            calls.append(method)
            if method == "getblockheader":
                return {"height": 0, "time": decoded.time, "confirmations": 1}
            return raw.hex()

        client.experimental.find_block = unsupported
        client.rpc = rpc
        self.assertEqual((await client.block(decoded.hash)).raw, raw)
        self.assertEqual(calls, ["getblockheader", "getblock"])
        with self.assertRaises(UnsupportedInterface):
            await client.block(decoded.hash, mode="typed")

        async def missing(*args, **kwargs):
            raise BlockUnavailable("pruned")

        client.experimental.find_block = missing
        calls.clear()
        with self.assertRaises(BlockUnavailable):
            await client.block(decoded.hash)
        self.assertEqual(calls, [])

    async def test_callback_failure_marks_interruption(self):
        module = SimpleNamespace(Thread=SimpleNamespace(Server=object),
                                 ThreadMap=SimpleNamespace(Server=object),
                                 ChainNotifications=SimpleNamespace(Server=object))
        client = BitcoinIPCClient("unused")
        _, _, notifications = make_callback_servers({"proxy": module, "chain": module}, client)
        with self.assertLogs(LOG, level="ERROR"):
            await notifications.blockDisconnected(None, SimpleNamespace(hash=b"bad"))
        event = client.events.get_nowait()
        self.assertEqual(event.kind, "interrupted")
        self.assertIn("callback decoding failed", event.reason)

    async def test_concurrent_subscription_is_single(self):
        counts = {"factories": 0, "subscriptions": 0}

        class Chain:
            async def handleNotifications(self, **kwargs):
                counts["subscriptions"] += 1
                await asyncio.sleep(0)
                return SimpleNamespace(result=object())

        class Init:
            async def makeChain(self, **kwargs):
                counts["factories"] += 1
                await asyncio.sleep(0)
                return SimpleNamespace(result=Chain())

        client = BitcoinIPCClient("unused")
        client.connected = True
        client.init = Init()
        client.callback_servers = (None, None, None)
        await asyncio.gather(*(client.subscribe() for _ in range(8)))
        self.assertEqual(counts, {"factories": 1, "subscriptions": 1})

    async def test_queued_call_does_not_cross_connection_generation(self):
        client = BitcoinIPCClient("unused")
        client.connected = True
        await client._lock.acquire()
        entered = asyncio.Event()
        sent = []

        async def request():
            sent.append(True)

        async def queued_call():
            entered.set()
            return await client.call(request)

        task = asyncio.create_task(queued_call())
        await entered.wait()
        client.generation += 1
        client._lock.release()
        with self.assertRaises(ConnectionLost):
            await task
        self.assertEqual(sent, [])
        self.assertTrue(client.connected, "A stale request must not disconnect the new connection")

    async def test_timeout_includes_queue_wait_without_closing_connection(self):
        client = BitcoinIPCClient("unused", timeout=.02)
        client.connected = True
        await client._lock.acquire()
        sent = []

        async def request():
            sent.append(True)

        task = asyncio.create_task(client.call(request))
        try:
            with self.assertRaisesRegex(IPCError, "request was not sent"):
                await asyncio.wait_for(task, 1)
            self.assertTrue(client.connected)
            self.assertEqual(sent, [])
        finally:
            client._lock.release()

    async def test_connection_change_during_response_is_detected(self):
        client = BitcoinIPCClient("unused")
        client.connected = True

        async def stale_response():
            client.generation += 1
            return "old connection result"

        with self.assertRaises(ConnectionLost):
            await client.call(stale_response)
        self.assertTrue(client.connected)
