"""Offline regression tests; no pycapnp installation or node required."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import sqlite3
import tempfile
import unittest

from bitcoin_ipc_client import (
    BitcoinIPCClient, BlockData, BlockRef, ChainEvent, BlockUnavailable,
    ConnectionLost, IPCError, UnsupportedInterface,
)
from bitcoin_ipc_client.notifications import make_callback_servers
from bitcoin_ipc_client.types import display_hash, hash256, wire_hash
from reference_indexer.blocks import WireReader, decode_block
from reference_indexer.store import IndexStore
from reference_indexer.sync import reconcile
from tests.fixtures import synthetic_block

LOG = logging.getLogger("btc-ipc")

class Tests(unittest.IsolatedAsyncioTestCase):
    def test_hash_roundtrip(self):
        value = "0123456789abcdef" * 4
        self.assertEqual(display_hash(wire_hash(value)), value)
        for invalid in ("ab", "z" * 64, " " * 64):
            with self.assertRaises(ValueError):
                wire_hash(invalid)

    def test_decoder(self):
        for witness in (False, True):
            raw, txid = synthetic_block(witness=witness)
            block = decode_block(raw)
            self.assertEqual(block.txids, (txid,))
            self.assertEqual(block.previous_hash, "00" * 32)
            self.assertEqual(block.hash, hash256(raw[:80])[::-1].hex())
            for malformed in (raw[:79], raw[:-1], raw + b"\x00", raw[:80] + b"\x00"):
                with self.assertRaises(ValueError):
                    decode_block(malformed)

    def test_compact_size(self):
        with self.assertRaises(ValueError):
            WireReader(b"\xfd\x01\x00").compact()
        with self.assertRaises(ValueError):
            WireReader(b"\xfe\xff\xff\xff\x7f").compact()

    def test_persistence_and_atomic_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "index.sqlite"
            first = decode_block(synthetic_block()[0])
            second = decode_block(synthetic_block(first.hash, 1)[0])
            store = IndexStore(path)
            store.bind_network(first.hash)
            store.append(first, 0)
            store.db.execute("CREATE TRIGGER fail_append BEFORE INSERT ON active_chain WHEN NEW.height=1 BEGIN SELECT RAISE(ABORT, 'test crash'); END")
            with self.assertRaises(sqlite3.IntegrityError):
                store.append(second, 1)
            self.assertEqual(store.tip(), BlockRef(first.hash, 0))
            self.assertEqual(store.db.execute("SELECT count(*) FROM blocks").fetchone()[0], 1)
            store.db.execute("DROP TRIGGER fail_append")
            store.append(second, 1)
            store.close()
            store = IndexStore(path)
            try:
                self.assertEqual(store.tip(), BlockRef(second.hash, 1))
                with self.assertRaises(IPCError):
                    store.bind_network("ff" * 32)
                store.rewind(0)
                store.append(second, 1)
                self.assertEqual(store.db.execute("SELECT count(*) FROM transactions").fetchone()[0], 2)
            finally:
                store.close()

    def test_bounded_event_delivery(self):
        client = BitcoinIPCClient("unused", queue_size=1)
        client.emit(ChainEvent("tip"))
        client.emit(ChainEvent("connected"))
        self.assertEqual(client.events.get_nowait().kind, "interrupted")
        client.interrupt_events("failure")
        self.assertEqual(client.events.get_nowait().reason, "failure")

    async def test_reconciliation_reorg(self):
        class FakeClient:
            def __init__(self):
                self.chain = []
                self.experimental = self

            async def tip(self):
                return BlockRef(decode_block(self.chain[-1]).hash, len(self.chain) - 1)

            async def ancestor(self, snapshot, height):
                return decode_block(self.chain[height]).hash

            async def block(self, block_hash, mode):
                for height, raw in enumerate(self.chain):
                    block = decode_block(raw)
                    if block.hash == block_hash:
                        return BlockData(block.hash, height, block.time, True, raw)
                raise BlockUnavailable(block_hash)

        client = FakeClient()
        previous = "00" * 32
        for nonce in range(5):
            raw, _ = synthetic_block(previous, nonce)
            client.chain.append(raw)
            previous = decode_block(raw).hash
        store = IndexStore(":memory:")
        try:
            await reconcile(client, store)
            self.assertEqual(store.tip(), await client.tip())
            client.chain = client.chain[:2]
            previous = decode_block(client.chain[-1]).hash
            for nonce in range(10, 14):
                raw, _ = synthetic_block(previous, nonce, witness=True)
                client.chain.append(raw)
                previous = decode_block(raw).hash
            await reconcile(client, store)
            self.assertEqual(store.tip(), await client.tip())
            for height, raw in enumerate(client.chain):
                self.assertEqual(store.hash_at(height), decode_block(raw).hash)
        finally:
            store.close()

    async def test_call_serialization_and_timeout(self):
        client = BitcoinIPCClient("unused", timeout=.01)
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
        from types import SimpleNamespace
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


def selftest():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(selftest())
