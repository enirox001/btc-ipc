"""Command-line commands and the reconnecting watch/index runner."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import logging
from pathlib import Path
import sqlite3

from bitcoin_ipc_client import BitcoinIPCClient, ChainEvent, IPCError, ConnectionLost, UnsupportedInterface
from bitcoin_ipc_client.schemas import DEFAULT_CORE, provenance, require_capnp
from reference_indexer.blocks import decode_block
from reference_indexer.store import IndexStore
from reference_indexer.sync import reconcile
from benchmarks import benchmark

LOG = logging.getLogger("btc-ipc")

def event_dict(event: ChainEvent) -> dict:
    value = asdict(event)
    value["raw_bytes"] = len(value.pop("raw"))
    return value

def print_json(value):
    print(json.dumps(value, sort_keys=True), flush=True)

async def follow(client: BitcoinIPCClient, args, store: IndexStore | None = None):
    delay = 0.5
    while True:
        try:
            if not client.connected:
                await client.reconnect()
            try:
                await client.subscribe()
            except UnsupportedInterface:
                if store is None or args.mode == "typed":
                    raise
                LOG.info("Notifications unavailable; polling and reconciling")
            if store is not None:
                tip = await reconcile(client, store, mode=args.mode)
                print_json({"kind": "synced", **asdict(tip)})
                if args.once:
                    return
            delay = 0.5
            while client.connected:
                try:
                    event = await asyncio.wait_for(client.events.get(), args.poll_interval)
                except asyncio.TimeoutError:
                    if store is not None:
                        await reconcile(client, store, mode=args.mode)
                    continue
                print_json(event_dict(event))
                if store is not None:
                    # Notifications (including historical chainstates) are hints,
                    # never authoritative active-chain writes.
                    await reconcile(client, store, mode=args.mode)
            raise ConnectionLost("IPC disconnected")
        except (ConnectionLost, OSError) as exc:
            LOG.warning("%s; reconnecting in %.1fs", exc, delay)
            await client.close()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)

def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--core-source", type=Path, default=DEFAULT_CORE)
    result.add_argument("--socket", type=Path, default=Path(".local/node.sock"))
    result.add_argument("--timeout", type=float, default=30)
    result.add_argument("--verbose", action="store_true")
    commands = result.add_subparsers(dest="command", required=True)
    echo = commands.add_parser("echo", help="Connect, echo, and close")
    echo.add_argument("text", nargs="?", default="hello bitcoin IPC")
    rpc = commands.add_parser("rpc", help="Execute RPC-over-IPC (calls are never automatically retried)")
    rpc.add_argument("method")
    rpc.add_argument("--params", default="[]", help="JSON array or object")
    block = commands.add_parser("block", help="Read and decode one block")
    block.add_argument("hash")
    block.add_argument("--mode", choices=("auto", "typed", "rpc"), default="auto")
    block.add_argument("--raw", action="store_true", help="Include serialized block hex")
    for command in ("watch", "index"):
        sub = commands.add_parser(command, help="Follow notifications" if command == "watch" else "Sync and follow the active chain")
        sub.add_argument("--poll-interval", type=float, default=5)
        sub.add_argument("--queue-size", type=int, default=256)
        sub.add_argument("--mode", choices=("auto", "typed", "rpc"), default="auto")
        if command == "index":
            sub.add_argument("--db", default=".local/index.sqlite", help="Use :memory: for an ephemeral index")
            sub.add_argument("--once", action="store_true", help="Exit after converging to the current tip")
    bench = commands.add_parser("benchmark", help="Compare typed block reads and RPC; storage excluded")
    bench.add_argument("--hash")
    bench.add_argument("--iterations", type=int, default=100)
    bench.add_argument("--http-url", help="Optional HTTP RPC baseline")
    bench.add_argument("--cookie", type=Path)
    commands.add_parser("provenance", help="Report Core revision and schema checksums")
    commands.add_parser("selftest", help="Run offline decoding, persistence, and reconciliation tests")
    integration = commands.add_parser("integration-test", help="Run against a disposable regtest node")
    integration.add_argument("--node-binary", type=Path, default=DEFAULT_CORE / "build/bin/bitcoin-node")
    return result

async def run_command(args):
    client = BitcoinIPCClient(args.socket, args.core_source, timeout=args.timeout,
                              queue_size=getattr(args, "queue_size", 256))
    try:
        if args.command in ("watch", "index"):
            if args.poll_interval <= 0:
                raise ValueError("poll interval must be positive")
            store = IndexStore(args.db) if args.command == "index" else None
            try:
                await follow(client, args, store)
            finally:
                if store is not None:
                    store.close()
            return
        async with client:
            if args.command == "echo":
                print_json({"echo": await client.echo(args.text)})
            elif args.command == "rpc":
                params = json.loads(args.params)
                if not isinstance(params, (list, dict)):
                    raise ValueError("RPC params must be a JSON array or object")
                print_json(await client.rpc(args.method, params))
            elif args.command == "block":
                block = await client.block(args.hash, mode=args.mode)
                decoded = await asyncio.to_thread(decode_block, block.raw)
                if decoded.hash != block.hash:
                    raise IPCError("Block payload hash mismatch")
                output = {**asdict(decoded), "height": block.height, "in_active_chain": block.in_active_chain,
                          "raw_bytes": len(block.raw)}
                if args.raw:
                    output["raw"] = block.raw.hex()
                print_json(output)
            elif args.command == "benchmark":
                if args.iterations <= 0:
                    raise ValueError("iterations must be positive")
                print_json(await benchmark(client, args))
    finally:
        await client.close()


def main():
    args = parser().parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "selftest":
            from tests.test_offline import selftest
            return selftest()
        if args.command == "provenance":
            print_json(provenance(args.core_source))
            return 0
        capnp = require_capnp()
        if args.command == "integration-test":
            from tests.integration import integration_test
            routine = integration_test(args)
        else:
            routine = run_command(args)
        asyncio.run(capnp.run(routine))
        return 0
    except KeyboardInterrupt:
        return 130
    except (IPCError, OSError, ValueError, sqlite3.Error) as exc:
        LOG.error("%s", exc)
        return 1
