# Bitcoin Core IPC experiment

The code is separated into a reusable client, a reference indexer, benchmarks,
and tests. **`btc_ipc.py`** is the small command-line entrypoint.
The reference indexer persists block metadata, ordered transaction IDs, and active
chain membership. It does not index scripts, addresses, wallets, or UTXOs.

This project uses the experimental `Chain` interface as a source of capabilities.
Using an operation successfully does not imply that it should become stable.

## Where to start reading

```text
btc-ipc/
├── btc_ipc.py                     # Entry point; imports the CLI when executed
├── cli.py                         # Commands, JSON output, watch/reconnect loop
├── bitcoin_ipc_client/
│   ├── __init__.py                # Public imports for other applications
│   ├── client.py                  # Connect, initialize, call, subscribe, close
│   ├── schemas.py                 # Load Core schemas; record versions/checksums
│   ├── rpc.py                     # JSON RPC requests through the IPC capability
│   ├── experimental.py            # Typed block and ancestor lookup adapters
│   ├── notifications.py           # Python servers receiving Core callbacks
│   ├── types.py                   # Owned results/events and hash conversions
│   └── errors.py                  # Failures callers can distinguish
├── reference_indexer/
│   ├── blocks.py                  # Decode blocks and compute transaction IDs
│   ├── store.py                   # SQLite records and atomic checkpoints
│   └── sync.py                    # Historical sync and reorg reconciliation
├── benchmarks.py                 # Compare retrieval paths; exclude storage
└── tests/
    ├── fixtures.py               # Synthetic serialized blocks
    ├── test_offline.py           # Regression tests without a node
    └── integration.py            # Disposable real-node test
```

Read in this order:

1. **`btc_ipc.py` → `cli.py`**: choose a command and see which operation it runs.
2. **`bitcoin_ipc_client/client.py`**: follow `connect()`, `echo()`, and `close()`.
   `schemas.py` supplies the message/interface definitions; the connection then
   obtains Core's `Init` capability and creates its remote execution context.
3. **`rpc.py` and `experimental.py`**: compare sending a JSON RPC request with
   invoking a schema-defined method. Both travel over the same IPC connection.
4. **`notifications.py`**: see the direction reverse. Core calls Python interface
   servers, which copy message data and put events on the client's queue.
5. **`reference_indexer/sync.py`**: follow a chain snapshot, find a common ancestor,
   retrieve blocks, decode them, and commit local state through `store.py`.

The main paths are:

```text
echo:   CLI → client.echo() → Core Echo capability
rpc:    CLI → client.rpc() → execute_rpc() → Core Rpc capability
block:  CLI → client.block() → experimental.find_block() → Core Chain capability
watch:  Core callback → Notifications → event queue → CLI follow() → JSON output
index:  CLI follow() → reconcile() → client queries → decode_block() → IndexStore
```

The client package does not import the indexer or SQLite. Tests and benchmarks
live outside the client. Existing `python btc_ipc.py ...` commands are unchanged;
`from btc_ipc import BitcoinIPCClient` remains available for compatibility.

## Setup

Use Python 3.12 or newer. From this directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python btc_ipc.py selftest
```

`pycapnp` is the only third-party runtime dependency. Block decoding uses the
standard library, and SQLite is imported for the reference indexer only. Offline
tests and help work without `pycapnp` installed.

The default schema source is `../bitcoin_ipc_chain`. Override it with
`--core-source /absolute/path/to/core`. Use a checkout exposing `Init.makeChain`,
`Init.makeRpc`, and the associated schemas. Core's existing Python IPC tests are
the reference for initialization. This client does not import its test framework.

The initial development checkout is at commit
`c2dfc0c57f3a9d54aca0783cd9fc236c4b84c330`, with existing local changes in
`chain.capnp` and `chain-types.h`. Those changes are not modified by this project.
The binary must match the schemas in the selected checkout. Capture the actual
working-tree schema checksums, not just the commit:

```sh
python btc_ipc.py provenance
```

## Run a node

If a matching binary is not built, follow Core's platform build instructions and
then run this from the Core checkout:

```sh
cmake -B build -DENABLE_IPC=ON -DENABLE_WALLET=OFF -DBUILD_GUI=OFF
cmake --build build -j 4
```

From this project's directory, start a separate regtest node:

```sh
mkdir -p .local/node
../bitcoin_ipc_chain/build/bin/bitcoin -m node \
  -regtest -server -listen=0 -nosettings \
  -datadir="$PWD/.local/node" \
  -ipcbind="unix:$PWD/.local/node.sock" \
  -debug=ipc -printtoconsole
```

The default client socket is `.local/node.sock`. Use `--socket /path/to/socket`
for a different node. Keep global options **before** the command.

Keep that node running while using the CLI in a second terminal. The
`integration-test` command creates and removes its own temporary node; it does
not leave a node running for later `rpc`, `watch`, or `index` commands.

## First calls

```sh
python btc_ipc.py echo
python btc_ipc.py rpc getblockchaininfo
python btc_ipc.py rpc generatetoaddress \
  --params '[5,"mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn"]'
python btc_ipc.py block BLOCK_HASH --mode typed
python btc_ipc.py block BLOCK_HASH --mode rpc
python btc_ipc.py watch
```

Generate blocks in another terminal to see callbacks. `watch` emits JSON lines
for connected blocks, disconnected blocks, tip updates, and interruptions. It
reconnects after node disconnects. Callback payloads own their bytes; they expose
chainstate role information without assuming that historical callbacks represent
changes to the active chain.

RPC parameters may be a JSON array or object. RPC calls are never automatically
retried: if a connection drops, a mutating call's outcome may be unknown.

## Index the active chain

```sh
# Prove the workload without writing a database.
python btc_ipc.py index --db :memory: --once

# Persist and follow; Ctrl-C stops cleanly.
python btc_ipc.py index

# Reopen the same database and converge, then exit.
python btc_ipc.py index --once

# Exercise the RPC-over-IPC retrieval baseline.
python btc_ipc.py index --mode rpc --db .local/rpc-index.sqlite --once
```

The indexer subscribes before syncing, walks a hash-anchored chain snapshot, and
reconciles again if the tip changed. Notifications trigger reconciliation rather
than direct database writes. A periodic check (default five seconds) also repairs
missed notifications. `--mode auto` falls back to RPC for unsupported typed
operations, while `--mode typed` surfaces them as failures. The coherent tip
snapshot deliberately uses `getblockchaininfo` over IPC in every mode.

Each indexed block and checkpoint commits in one SQLite transaction. Reorgs
rewind active-chain membership to the common ancestor; stored block/transaction
records are retained and can be reused. Transaction occurrences are keyed by
block and position, so transaction IDs need not be globally unique. The database
is bound to the network's genesis hash. Required pruned/missing block data stops
indexing with an error instead of silently skipping history.

The database is entirely the indexer's local state. The IPC client has no storage
requirements. Decoding is not a second consensus validation engine.

## Use the client independently

```python
import asyncio
import capnp
from bitcoin_ipc_client import BitcoinIPCClient

async def main():
    async with BitcoinIPCClient(".local/node.sock") as client:
        print(await client.rpc("getblockchaininfo"))
        tip = await client.tip()
        block = await client.experimental.find_block(tip.hash)
        print(block.height, len(block.raw))

asyncio.run(capnp.run(main()))
```

The client owns the socket, remote thread context, and capability lifetimes.
Calls sharing that context are serialized. Reconnect explicitly with
`await client.reconnect()`, then subscribe again and reconcile application state.
The watch/index commands perform that lifecycle for their consumers.

## Verification

```sh
python btc_ipc.py selftest
python -m unittest discover -s tests -v
python btc_ipc.py integration-test
```

Offline tests cover legacy and witness transaction ID decoding, malformed data,
hash ordering, bounded event delivery, durable state, atomic rollback, network
mismatch, and multi-block reorg reconciliation.

The integration command starts its own disposable regtest node with an available
loopback RPC port and never touches an existing datadir. It checks echo, RPC errors, typed/RPC payload equality,
transaction IDs against RPC, callbacks, shallow/multi-block reorgs, client
reconnect, reopening the database, and node restart. Override the binary with
`integration-test --node-binary /path/to/bitcoin-node` if needed.

The harness advances regtest mock time before each mining batch so replacement
branches cannot recreate deliberately invalidated blocks. It prints each test
stage and saves the node log to `.local/integration-node.log` on failure.

Development verification: all nine offline tests pass, as do schema loading,
callback capability construction, and callback decoding checks with `pycapnp`
2.2.4. The live integration test also passed in the developer's terminal against
the Core checkout recorded above, including initial sync, shallow and multi-block
reorgs, client reconnect, database reopen, and node restart recovery. The execution
sandbox cannot bind the required sockets, so live integration must run from a
normal terminal. These results cover the exercised paths, not every Chain method.

## Measurements and API evaluation

```sh
python btc_ipc.py benchmark --iterations 100

# Add HTTP RPC to the comparison using this regtest node's cookie.
python btc_ipc.py benchmark --iterations 100 \
  --http-url http://127.0.0.1:18443 \
  --cookie .local/node/regtest/.cookie
```

The benchmark compares equivalent serialized block bytes with warm, sequential
typed IPC, RPC-over-IPC, and optional HTTP RPC reads. It reports median/p95 call
latency, calls per second, and separate decoding time. Typed lookup also returns
metadata; RPC paths include JSON hex conversion. HTTP timing includes Python
worker dispatch. SQLite is excluded. These are Python client measurements, not
isolated transport or serialization CPU measurements. Socket traffic, polling
versus callback latency, and node CPU impact need additional experiments.

Evaluate capabilities rather than declaring existing methods stable:

| Capability | Current route | Question to evaluate |
| --- | --- | --- |
| Coherent active tip | RPC-over-IPC | Is a small typed tip result worth maintaining? |
| Full block by hash | Experimental `findBlock`, RPC fallback | Does typed retrieval justify the current selection/result shape? |
| Ancestor lookup | Experimental `findAncestorByHeight` | Can its semantics form a useful external contract? |
| Chain notifications | Experimental `handleNotifications` | What ordering, backpressure, teardown, and chainstate semantics are needed? |
| Recovery after gaps | Client reconciliation | Does the node need a resume cursor or subscription snapshot? |
| Miscellaneous node control | Explicit RPC-over-IPC | Is RPC already sufficient? |

For each capability, record measured behavior, failure semantics, wider use
cases, and one conclusion: **Stable candidate**, **Unstable / needs redesign**,
**RPC is sufficient**, or **Missing capability**. No final classifications or
performance claims are asserted before live experiments provide evidence.

Still to establish experimentally: forced callback failure, a slow consumer,
reorg during historical sync, process crash around a commit, and actual schema/
binary mismatch. Offline tests cover some mechanisms but do not replace these
live scenarios. Mainnet-scale throughput and production operation are non-goals.
