# API assessment

The reference indexer exercises a small set of node capabilities. Classifications
below apply to external concepts and are provisional; successful calls alone do
not establish a stable contract.

| Operation | Current route | Assessment |
| --- | --- | --- |
| Coherent tip snapshot | RPC-over-IPC `getblockchaininfo` | RPC is sufficient for this workload. A typed hash/height result is a stable candidate concept; Mining already has `getTip`. |
| Block retrieval by hash | Experimental `findBlock`, RPC fallback | Stable candidate concept. The existing selection/result schema needs clearer availability and error semantics before stabilization. |
| Hash-anchored ancestry | Experimental `findAncestorByHeight` | Stable candidate concept for historical sync. Evaluate error and snapshot semantics independently of the current schema. |
| Connected/disconnected notifications | Experimental `handleNotifications` | Unstable / needs redesign: define ordering, callback failure policy, teardown, and chainstate roles. |
| Subscription starting snapshot and resume cursor | Query/reconcile in the client | Missing capability for resumable delivery. The current interface provides neither an atomic starting snapshot nor replay. |
| Node control and miscellaneous queries | Explicit RPC-over-IPC | RPC is sufficient. |

The client uses notifications as hints and reconciles against authoritative chain
queries after gaps. This recovers useful application state without establishing
exactly-once event delivery or a stable notification API.

Earlier raw probes on the experimental Core checkout found an incorrect ancestry
result for an unknown descendant, a node abort from a failing callback, and an
assertion when a reorg invalidated a height used by `getBlockHash`. These are
reasons to review the underlying implementation and external contract. The
indexer uses hash-anchored ancestry and does not call either `findAncestorByHash`
or `getBlockHash` directly. Investigation notes and reproductions are preserved
in the local cleanup archive.

`benchmarks.py` measures sequential warm block reads and separate decoding time.
Typed IPC includes metadata; RPC includes JSON hex conversion. The optional HTTP
path includes Python worker dispatch. These measurements do not isolate transport
CPU, serialization CPU, callback latency, or node load.

The earlier live integration and 20-round stress runs passed in the user's
terminal. The streamlined suite retains focused regressions for client lifecycle,
data integrity, and chain recovery. Production operation, pruned-node fixtures,
schema/version mismatch, and sustained large-chain load remain unverified.
