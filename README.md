# Bitcoin Core IPC client

A reusable Python IPC client and a lightweight reference chain indexer. The
client provides typed experimental Chain calls, notifications, explicit
reconnection, and RPC-over-IPC fallback. The indexer stores block metadata,
ordered transaction IDs, and active-chain membership in SQLite.

The Chain schema is experimental and must match the node binary. This project
is not yet established as production ready; the current checks do not establish
large-chain performance or compatibility across Core versions.

## Structure

```text
btc_ipc.py                  CLI entrypoint
cli.py                      Client commands and reconnecting watch/index loop
bitcoin_ipc_client/         Reusable connection, RPC, callbacks, and typed adapters
reference_indexer/          Block decoding, SQLite state, and chain reconciliation
tests/
    fixtures.py             Shared synthetic blocks and chain model
    test_client.py          Lifecycle, subscriptions, callbacks, and fallback
    test_indexer.py         Decoding, atomic storage, and reorg recovery
    test_capnp.py           Native Python binding checks
    integration.py          Disposable live regtest test
benchmarks.py               Standalone API comparison tool
docs/api-evaluation.md      Current interface assessment
```

The client has no SQLite dependency. Development tests and benchmarks are not
imported by the CLI. Historical investigation files are archived locally under
`.local/cleanup-archive/`.

## Setup

Use Python 3.12 or newer:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

The default Core source checkout is `../bitcoin_ipc_chain`. It must expose
`Init.makeChain` and `Init.makeRpc`. Override it with `--core-source /path/to/core`.
Record the revision and actual schema checksums with:

```sh
python btc_ipc.py provenance
```

## Start a node

Build the matching IPC-enabled Core checkout, then run this from this project's
directory:

```sh
../bitcoin_ipc_chain/build/bin/bitcoin -m node \
  -regtest -server -listen=0 -nosettings \
  -ipcbind=unix -printtoconsole
```

Core creates its default data directory and listens at its network-specific
`node.sock`. On macOS, regtest uses
`~/Library/Application Support/Bitcoin/regtest/node.sock`; on Linux, it uses
`~/.bitcoin/regtest/node.sock`.

Keep it running while using the client in another terminal. `--socket unix`
selects Core's default socket; `--regtest` selects its regtest directory. The
default network is mainnet; use `--chain signet`, `--chain testnet4`, or `--chain
test` for other networks. `--datadir /path/to/core-data` selects a custom base data
directory, with the network suffix added automatically. The client does not read
`bitcoin.conf`; pass any configured network/data directory explicitly.

Plain `--socket /path/to/node.sock` paths keep their existing interpretation.
Core-style `--socket unix:/path/to/node.sock` is also supported; a relative
`unix:PATH` resolves inside Core's network data directory. Without `--socket`,
the client still uses `.local/node.sock`. Global options go before the command.

## Commands

```sh
python btc_ipc.py --socket unix --regtest echo
python btc_ipc.py --socket unix --regtest rpc getblockchaininfo
python btc_ipc.py --socket unix --regtest rpc generatetoaddress \
  --params '[5,"mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn"]'
python btc_ipc.py --socket unix --regtest block BLOCK_HASH --mode typed
python btc_ipc.py --socket unix --regtest block BLOCK_HASH --mode rpc
python btc_ipc.py --socket unix --regtest watch
python btc_ipc.py --socket unix --regtest index --once
python btc_ipc.py --socket unix --regtest index
```

Commands emit JSON. RPC parameters accept a JSON array or object. Mutating RPC
calls are never automatically replayed after a failure: their outcome may be
unknown.

The indexer defaults to `.local/index.sqlite`; choose another path with `--db`,
or use `--db :memory:` for an ephemeral index. `--mode auto` falls back to RPC
when a typed interface is unsupported. `--mode typed` requires the typed block
and ancestor operations. Tip snapshots use RPC-over-IPC in every mode.

Notifications trigger reconciliation against the active chain. A periodic check
(default five seconds, configurable with `--poll-interval`) repairs missed events.
Reorgs rewind active-chain membership, and each block plus its checkpoint commits
atomically. Reopening the database recovers from its checkpoint. The store is
bound to the genesis hash; missing/pruned block data produces an error.

## Python API

```python
import asyncio
import capnp
from bitcoin_ipc_client import BitcoinIPCClient

async def main():
    async with BitcoinIPCClient("unix", chain="regtest") as client:
        tip = await client.tip()
        block = await client.block(tip.hash)
        print(block.height, len(block.raw))

asyncio.run(capnp.run(main()))
```

Calls share a serialized execution context. Their timeout includes waiting for
that context. An unsent timeout preserves the connection; a timeout after dispatch
disconnects because the outcome may be unknown. Queued requests cannot cross a
connection generation. Reconnect explicitly with `await client.reconnect()`,
subscribe again, and reconcile application state. The watch/index commands do
this automatically.

## Development checks

```sh
python -m unittest discover -s tests -v
python -m tests.integration
python benchmarks.py --socket unix --regtest --iterations 100
```

Native binding tests require pycapnp and the matching Core schemas; otherwise
they are skipped. The live integration runner owns a disposable node and database.
It checks callbacks, typed/RPC payload equality, initial sync, shallow and deep
reorgs, missed blocks during a client disconnect, database reopen, and graceful
and abrupt node restarts. Use `--node-binary /path/to/bitcoin-node` and
`--core-source /path/to/core` to select a build. Failures save the node log to
`.local/integration-node.log`. A node crash can roll back unflushed node state;
the test checks that the index converges to the restarted node.

The benchmark compares typed IPC and RPC-over-IPC, optionally adding HTTP RPC
with `--http-url` and `--cookie`. It excludes index storage. See
[the API assessment](docs/api-evaluation.md) for scope and current findings.
