"""Block decoding, durable checkpoints, and reorg recovery regressions."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from bitcoin_ipc_client import BlockRef, IPCError, RPCError
from bitcoin_ipc_client.types import hash256
from reference_indexer.blocks import WireReader, decode_block
from reference_indexer.store import IndexStore
from reference_indexer.sync import reconcile
from tests.fixtures import SyntheticNode, synthetic_block

class IndexerTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_reorg_during_historical_typed_sync(self):
        node = SyntheticNode()
        node.extend(30)
        node.on_block = lambda: node.reorg(12, 15)
        store = IndexStore(":memory:")
        try:
            await reconcile(node, store, mode="typed")
            self.assert_active_chain(node, store)
        finally:
            store.close()

    async def test_rpc_height_query_reorg_race(self):
        node = SyntheticNode()
        node.extend(10)
        store = IndexStore(":memory:")
        try:
            await reconcile(node, store, mode="rpc")
            node.extend(1)
            # The first lookup is genesis, then the saved tip's height. Reorg
            # after the pre-query tip check, before getblockhash returns.
            original_rpc = node.rpc
            calls = 0

            async def racing_rpc(method, params):
                nonlocal calls
                calls += 1
                if calls == 2:
                    node.reorg(8, 0)
                return await original_rpc(method, params)

            node.rpc = racing_rpc
            await reconcile(node, store, mode="rpc")
            self.assert_active_chain(node, store)
        finally:
            store.close()

    async def test_rpc_failure_with_unchanged_tip_is_not_hidden(self):
        node = SyntheticNode()
        node.extend(3)
        store = IndexStore(":memory:")

        async def failing_rpc(method, params):
            raise RPCError({"code": -32603, "message": "real node failure"})

        node.rpc = failing_rpc
        try:
            with self.assertRaises(RPCError):
                await reconcile(node, store, mode="rpc")
        finally:
            store.close()

    async def test_reorgs_and_database_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            for mode in ("typed", "rpc", "auto"):
                with self.subTest(mode=mode):
                    node = SyntheticNode()
                    node.extend(12)
                    path = Path(directory) / f"{mode}.sqlite"
                    store = IndexStore(path)
                    try:
                        await reconcile(node, store, mode=mode)
                        self.assert_active_chain(node, store)
                        for depth, replacements in ((0, 3), (1, 2), (4, 6), (8, 1), (0, 4)):
                            node.reorg(depth, replacements)
                            await reconcile(node, store, mode=mode)
                            self.assert_active_chain(node, store)
                            store.close()
                            store = IndexStore(path)
                            self.assert_active_chain(node, store)
                        self.assertEqual(store.db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                        self.assertEqual(store.db.execute("PRAGMA foreign_key_check").fetchall(), [])
                    finally:
                        store.close()

    def assert_active_chain(self, node, store):
        rows = store.db.execute("SELECT hash FROM active_chain ORDER BY height").fetchall()
        self.assertEqual([row[0] for row in rows], node.active)
        checkpoint = json.loads(store.db.execute("SELECT value FROM metadata WHERE key='tip'").fetchone()[0])
        self.assertEqual(checkpoint, {"hash": node.active[-1], "height": len(node.active) - 1})
        for block_hash in node.active:
            expected = decode_block(node.blocks[block_hash].raw).txids
            actual = store.db.execute("SELECT txid FROM transactions WHERE block_hash=? ORDER BY position", (block_hash,)).fetchall()
            self.assertEqual(tuple(row[0] for row in actual), expected)
