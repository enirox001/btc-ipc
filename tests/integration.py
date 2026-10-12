"""End-to-end tests against a disposable real regtest node."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import logging
from pathlib import Path
import socket
import subprocess
import tempfile

from bitcoin_ipc_client import BitcoinIPCClient, IPCError, ConnectionLost, RPCError
from bitcoin_ipc_client.schemas import DEFAULT_CORE, provenance, require_capnp
from reference_indexer.blocks import decode_block
from reference_indexer.store import IndexStore
from reference_indexer.sync import reconcile

LOG = logging.getLogger("btc-ipc")

async def integration_test(args):
    """Disposable real-node test; never touches an existing node or datadir."""
    binary = args.node_binary.resolve()
    if not binary.is_file():
        raise ValueError(f"Node binary does not exist: {binary}")
    # Short temp path avoids Unix socket path length limits on macOS.
    with tempfile.TemporaryDirectory(prefix="btc-ipc-", dir="/tmp") as directory:
        root = Path(directory)
        ipc_socket = root / "node.sock"
        logfile = (root / "node.log").open("w+")
        process = None
        client = BitcoinIPCClient(ipc_socket, args.core_source, timeout=10, queue_size=32)
        store = IndexStore(root / "index.sqlite")
        stage = "node startup"
        mock_time = 1700000000
        crash_recovery = None

        def begin_stage(name):
            nonlocal stage
            stage = name
            LOG.info("Integration: %s", stage)

        async def generate(count, control=None):
            nonlocal mock_time
            control = control or client
            # Reusing a parent, payout, and timestamp after invalidateblock can
            # reproduce the invalidated block. Make each new branch distinct.
            mock_time += 60
            await control.rpc("setmocktime", [mock_time])
            hashes = await control.rpc("generatetoaddress", [count, "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn"])
            assert len(hashes) == count, f"Requested {count} blocks, generated {len(hashes)}"
            return hashes

        def start_node():
            # The OS can allocate port 0, but Core requires the resulting
            # nonzero port number. Select again on restart; IPC is our transport.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
                reservation.bind(("127.0.0.1", 0))
                rpc_port = reservation.getsockname()[1]
            return subprocess.Popen([str(binary), "-regtest", "-server", "-listen=0", "-dnsseed=0",
                                     "-discover=0", f"-rpcport={rpc_port}", "-rpcbind=127.0.0.1",
                                     "-rpcallowip=127.0.0.1", "-nosettings", "-printtoconsole",
                                     f"-datadir={root}", f"-ipcbind=unix:{ipc_socket}"], stdout=logfile, stderr=subprocess.STDOUT)

        async def ready():
            for _ in range(150):
                if process.poll() is not None:
                    logfile.flush()
                    raise IPCError("Test node exited: " + (root / "node.log").read_text()[-3000:])
                try:
                    if not client.connected:
                        await client.connect()
                    await client.rpc("getblockchaininfo")
                    return
                except (OSError, ConnectionLost, RPCError):
                    await client.close()
                    await asyncio.sleep(.1)
            raise IPCError("Test node did not become ready")

        async def next_connected():
            async def get():
                while True:
                    event = await client.events.get()
                    if event.kind == "connected":
                        return event
            return await asyncio.wait_for(get(), 10)

        async def flush_callbacks():
            chain = await client.chain_capability()
            await client.call(lambda: chain.waitForNotifications(context=client.ctx))

        try:
            begin_stage("node startup")
            process = start_node()
            await ready()
            begin_stage("echo and RPC error handling")
            assert await client.echo("integration") == "integration"
            try:
                await client.rpc("nonexistent_btc_ipc_test_method")
                raise AssertionError("Expected RPC error")
            except RPCError as exc:
                assert exc.code == -32601
            await client.subscribe()
            begin_stage("initial block generation and callbacks")
            hashes = await generate(5)
            event = await next_connected()
            assert event.hash in hashes
            begin_stage("typed/RPC block comparison and initial sync")
            typed = await client.block(hashes[-1], mode="typed")
            rpc = await client.block(hashes[-1], mode="rpc")
            assert typed.raw == rpc.raw
            decoded = decode_block(typed.raw)
            assert list(decoded.txids) == (await client.rpc("getblock", [typed.hash, 1]))["tx"]
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            # Shallow reorg, then a multi-block branch replacement.
            begin_stage("shallow reorg")
            await client.rpc("invalidateblock", [hashes[-1]])
            await generate(2)
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            begin_stage("multi-block reorg")
            await client.rpc("invalidateblock", [hashes[1]])
            await generate(6)
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            # Reconnect and local restart recover from the same database.
            begin_stage("client reconnect and database reopen")
            await client.close()
            async with BitcoinIPCClient(ipc_socket, args.core_source, timeout=10) as controller:
                await generate(2, controller)
            store.close()
            store = IndexStore(root / "index.sqlite")
            await client.connect()
            await client.subscribe()
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            begin_stage("node restart and recovery")
            await client.rpc("stop")
            await asyncio.to_thread(process.wait, timeout=15)
            await client.close()
            process = start_node()
            await ready()
            await client.subscribe()
            await generate(1)
            await next_connected()
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            begin_stage("abrupt node crash and recovery")
            crash_recovery = {"before_crash": asdict(await client.tip())}
            process.kill()
            await asyncio.to_thread(process.wait, timeout=15)
            await client.close()
            store.close()
            store = IndexStore(root / "index.sqlite")
            process = start_node()
            await ready()
            crash_recovery["after_restart"] = asdict(await client.tip())
            await client.subscribe()
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            await generate(1)
            await flush_callbacks()
            await reconcile(client, store)
            assert store.tip() == await client.tip()
            assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            crash_recovery["after_recovery"] = asdict(store.tip())
            print(json.dumps({"integration_test": "passed", "tip": asdict(store.tip()),
                        "crash_recovery": crash_recovery,
                        "provenance": provenance(args.core_source)}, sort_keys=True), flush=True)
        except Exception as exc:
            logfile.flush()
            node_log = (root / "node.log").read_text()
            failure_log = Path(__file__).resolve().parent.parent / ".local/integration-node.log"
            try:
                failure_log.parent.mkdir(parents=True, exist_ok=True)
                failure_log.write_text(node_log)
                LOG.error("Full node log saved to %s", failure_log)
            except OSError as log_error:
                LOG.warning("Could not save node log: %s", log_error)
            raise IPCError(f"Integration failed during {stage}: {exc}\nNode log tail:\n{node_log[-3000:]}") from exc
        finally:
            await client.close()
            store.close()
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    await asyncio.to_thread(process.wait, timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait)
            logfile.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-source", type=Path, default=DEFAULT_CORE)
    parser.add_argument("--node-binary", type=Path, default=DEFAULT_CORE / "build/bin/bitcoin-node")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        asyncio.run(require_capnp().run(integration_test(args)))
        return 0
    except KeyboardInterrupt:
        return 130
    except (IPCError, OSError, ValueError) as exc:
        LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
