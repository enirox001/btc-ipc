"""Compare typed IPC, RPC-over-IPC, and optional HTTP block retrieval."""

from __future__ import annotations

import asyncio
import argparse
import base64
import json
import math
import logging
from pathlib import Path
import statistics
import time
from typing import Any
import urllib.request

from bitcoin_ipc_client import BitcoinIPCClient, IPCError, RPCError
from bitcoin_ipc_client.schemas import DEFAULT_CORE, provenance, require_capnp
from reference_indexer.blocks import decode_block

def http_rpc(url: str, cookie: Path, method: str, params: list) -> Any:
    auth = base64.b64encode(cookie.read_text().strip().encode()).decode()
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(url, body, {"Content-Type": "application/json", "Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if payload.get("error") is not None:
        raise RPCError(payload["error"])
    return payload["result"]

async def benchmark(client: BitcoinIPCClient, args):
    target = args.hash or (await client.tip()).hash
    typed = await client.experimental.find_block(target)
    rpc_raw = bytes.fromhex(await client.rpc("getblock", [target, 0]))
    if typed.raw != rpc_raw:
        raise IPCError("Benchmark payloads differ")
    result = {"provenance": provenance(client.core_source), "block": target,
              "block_bytes": len(rpc_raw), "iterations": args.iterations,
              "note": "Sequential warm calls; typed lookup includes metadata. No persistence. JSON hex conversion included in RPC paths.",
              "measurements": {}}
    paths = ["typed-ipc", "rpc-over-ipc"]
    if args.http_url:
        if not args.cookie:
            raise ValueError("--http-url requires --cookie")
        paths.append("http-rpc")
    for path in paths:
        samples = []
        for _ in range(args.iterations):
            started = time.perf_counter()
            if path == "typed-ipc":
                raw = (await client.experimental.find_block(target)).raw
            elif path == "rpc-over-ipc":
                raw = bytes.fromhex(await client.rpc("getblock", [target, 0]))
            else:
                raw = bytes.fromhex(await asyncio.to_thread(http_rpc, args.http_url, args.cookie, "getblock", [target, 0]))
            samples.append(time.perf_counter() - started)
            if raw != rpc_raw:
                raise IPCError("Benchmark payload changed")
        ordered = sorted(samples)
        result["measurements"][path] = {
            "median_ms": statistics.median(samples) * 1000,
            "p95_ms": ordered[math.ceil(len(samples) * .95) - 1] * 1000,
            "calls_per_second": len(samples) / sum(samples),
        }
    samples = []
    for _ in range(args.iterations):
        started = time.perf_counter()
        decode_block(rpc_raw)
        samples.append(time.perf_counter() - started)
    result["decode_median_ms"] = statistics.median(samples) * 1000
    result["decode_process_cpu_note"] = "Wall time only; does not estimate serialization CPU or socket bytes."
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-source", type=Path, default=DEFAULT_CORE)
    parser.add_argument("--socket", type=Path, default=Path(".local/node.sock"))
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--hash")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--http-url")
    parser.add_argument("--cookie", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def run():
        async with BitcoinIPCClient(args.socket, args.core_source, timeout=args.timeout) as client:
            print(json.dumps(await benchmark(client, args), sort_keys=True), flush=True)

    try:
        if args.iterations <= 0:
            raise ValueError("iterations must be positive")
        asyncio.run(require_capnp().run(run()))
        return 0
    except KeyboardInterrupt:
        return 130
    except (IPCError, OSError, ValueError) as exc:
        logging.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
