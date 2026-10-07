"""Atomic local index state; SQLite is an indexer dependency only."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import sqlite3

from bitcoin_ipc_client.errors import IPCError
from bitcoin_ipc_client.types import BlockRef
from .blocks import DecodedBlock

class IndexStore:
    """SQLite is local indexer state only; :memory: uses identical semantics."""
    def __init__(self, path: str | Path):
        if str(path) != ":memory:":
            Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path))
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            self.db.close()
            raise IPCError(f"Unsupported index database version {version}")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS blocks(
                hash TEXT PRIMARY KEY, height INTEGER NOT NULL,
                previous_hash TEXT NOT NULL, time INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS transactions(
                block_hash TEXT NOT NULL REFERENCES blocks(hash),
                position INTEGER NOT NULL, txid TEXT NOT NULL,
                PRIMARY KEY(block_hash, position));
            CREATE TABLE IF NOT EXISTS active_chain(
                height INTEGER PRIMARY KEY, hash TEXT UNIQUE NOT NULL REFERENCES blocks(hash));
            PRAGMA user_version=1;
        """)

    def close(self):
        self.db.close()

    def bind_network(self, genesis: str):
        row = self.db.execute("SELECT value FROM metadata WHERE key='genesis'").fetchone()
        if row and row[0] != genesis:
            raise IPCError("Database belongs to a different network; use another --db")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('genesis', ?)", (genesis,))

    def tip(self) -> BlockRef | None:
        row = self.db.execute("SELECT hash, height FROM active_chain ORDER BY height DESC LIMIT 1").fetchone()
        return BlockRef(*row) if row else None

    def hash_at(self, height: int) -> str | None:
        row = self.db.execute("SELECT hash FROM active_chain WHERE height=?", (height,)).fetchone()
        return row[0] if row else None

    def rewind(self, height: int):
        with self.db:
            self.db.execute("DELETE FROM active_chain WHERE height > ?", (height,))
            self._checkpoint()

    def _checkpoint(self):
        tip = self.tip()
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('tip', ?)",
                        (json.dumps(asdict(tip)) if tip else "null",))

    def append(self, block: DecodedBlock, height: int):
        tip = self.tip()
        if height != (tip.height + 1 if tip else 0):
            raise IPCError("Noncontiguous index append")
        if block.previous_hash != (tip.hash if tip else "00" * 32):
            raise IPCError("Block does not extend indexed tip")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO blocks VALUES (?, ?, ?, ?)",
                            (block.hash, height, block.previous_hash, block.time))
            self.db.executemany("INSERT OR IGNORE INTO transactions VALUES (?, ?, ?)",
                                ((block.hash, position, txid) for position, txid in enumerate(block.txids)))
            self.db.execute("INSERT INTO active_chain VALUES (?, ?)", (height, block.hash))
            self._checkpoint()
