# MarketOMS — Functional Specification

Generated: 2026-06-05
Source: design/ docs + source as of master HEAD 0852a030813d88ad8486a2830547a8e252efafb4

> This document is derived from design/ and source. Do not edit directly.
> To update: change the relevant design/ file or source, then re-run
> skills/workflows/spec-generator/SKILL.md.

## Conflicts with design docs

**C-1 — SnapshotManager source comment says 80 bytes per parent record**
- `SnapshotManager.java` Javadoc says `[24 .. 24+orderCount*80]   parent order records (MESSAGE_SIZE each)`. The text is self-contradictory: `MESSAGE_SIZE = 128`, not 80. Source behavior and `design/failover.md` both use 128 bytes. Only the comment is stale. *Spec reflects source behavior (128 bytes).*
- Recommended fix: update the Javadoc in `SnapshotManager.java` line 26 to say 128 instead of 80.

**C-2 — resilience-review.md C1 fix description references an intermediate snapshot version**
- `design/resilience-review.md` C1 Fix text says: "SNAPSHOT_VERSION bumped to 2; MAX_SNAPSHOT_BYTES increased to ≈ 10.5 MB." Source shows `SNAPSHOT_VERSION = 3`, no static `MAX_SNAPSHOT_BYTES` constant (buffer is computed dynamically at ≈ 13.0 MB). The fix description describes an intermediate commit; the current state is version 3. *Spec reflects source.*
- Recommended fix: update resilience-review.md C1 Fix text to reflect the current state.

---

## Section 1 — System Overview

MarketOMS is a sell-side Order Management System designed for institutional electronic trading. Its core function is to receive parent orders from buy-side clients (via a FIX connectivity layer), validate them, route them to one or more execution venues using configurable algorithmic strategies, aggregate fills back to the originating client, and maintain a legally complete audit trail of every state transition. The system is built for deterministic sub-millisecond latency on the hot path and is designed to handle high-frequency institutional order flow without GC-induced latency spikes.

The system is operated by a sell-side trading desk or electronic market-making operation. The desk operator configures permitted symbols, per-order notional limits, venue routing, and cluster topology. Buy-side clients connect over a FIX connectivity bridge and interact with the system via standard FIX 4.x order messages. Venues are accessed through a downstream FIX connectivity engine that bridges the system's internal binary wire format to venue-specific FIX sessions.

In a sell-side trading infrastructure, MarketOMS occupies the layer between the client-facing FIX gateway and the venue-facing execution network. It is the system of record for all open orders and the single authority for child order creation, fills, cancels, and state transitions. It does not perform pre-trade risk (beyond its built-in per-order notional limit) and does not manage post-trade reporting — those functions are handled by upstream risk systems and downstream settlement infrastructure.

---

## Section 2 — Actors and External Interfaces

### 2.1 External Actor Table

| Actor | Interface | Protocol | Direction | Notes |
|-------|-----------|----------|-----------|-------|
| Buy-side client | Aeron Cluster ingress, UDP port 9000 | ClusterMessageType framing (129 bytes: 1-byte type + 128-byte OrderLayout) | Inbound | `AeronCluster.Context.ingressEndpoints` format: `nodeId=host:port` (e.g. `0=localhost:9000`). Env: `HARNESS_CLUSTER_INGRESS`. Message types: NEW_ORDER=1, CANCEL_ORDER=2, REPLACE_ORDER=3. |
| Buy-side client | Aeron Cluster egress | Same 129-byte ClusterMessageType framing | Outbound | Exec reports sent via `ClientSession.offer()` in `sendEgressExecReport()`. Spins up to 1000 attempts on back-pressure before logging WARN and dropping. |
| FIX connectivity engine (inbound ExecReports) | Aeron IPC stream 11 (`STREAM_FIX_TO_OMS`) | FIX Binary (76 bytes) | Inbound | Venue fills arrive here. Decoded by `FIXMessageDecoder.decodeExecReport()`. Channel: `aeron:ipc`. |
| FIX connectivity engine (outbound NOS/Cancel/Replace) | Aeron IPC stream 10 (`STREAM_OMS_TO_FIX`) | FIX Binary (76 bytes) | Outbound | Child NOS dispatched by `FIXMessageEncoder.sendNewOrderSingle()`. Cancel and replace via `sendCancelRequest()` / `sendCancelReplace()`. Non-blocking offer — back-pressure not retried. |
| Trading venue 1 | Aeron IPC stream 20 (`STREAM_VENUE_1_OUT`) | FIX Binary (76 bytes) | Outbound | Per-venue FIX engine publication. |
| Trading venue 2 | Aeron IPC stream 21 (`STREAM_VENUE_2_OUT`) | FIX Binary (76 bytes) | Outbound | Per-venue FIX engine publication. |
| Trading venue 3 | Aeron IPC stream 22 (`STREAM_VENUE_3_OUT`) | FIX Binary (76 bytes) | Outbound | Per-venue FIX engine publication. |
| algo-sor process (parent orders) | Aeron IPC stream 30 (`STREAM_OMS_TO_ALGO`) | ClusterMessageType framing (129 bytes, type=NEW_ORDER) | Outbound | Validated parent orders published to `AlgoSorAgent`. Channel: `aeron:ipc`. |
| algo-sor process (child intents) | Aeron IPC stream 12 (`STREAM_CHILD_INTENTS`) | 8-byte header + 56-byte `ChildOrderIntentFlyweight` = 64 bytes | Inbound | `intentSub` has `availableImageHandler` (INFO log) and `unavailableImageHandler` (WARN log) for disconnect detection. |
| Cluster operator | Environment variables / `OMS_CONFIG_FILE` properties file | OS env / `.properties` | Configuration | `OmsConfig` resolves and validates all parameters at startup. See Section 7 for full list. |

### 2.2 Internal IPC Channel Summary

All channels use `aeron:ipc` (shared memory, ~100–300 ns). The Aeron Cluster ingress and inter-node Raft replication use UDP. `ExclusivePublication` is used for all single-writer channels.

---

## Section 3 — Order Lifecycle

### 3.1 New Order Ingress

A buy-side client submits a new order by sending a 129-byte message to the Aeron Cluster ingress endpoint (UDP port 9000 by default). The message is framed with a `ClusterMessageType.NEW_ORDER` byte followed by a 128-byte `OrderLayout` payload containing account ID, client-assigned order ID (`clOrdId`), symbol (packed as a long), price (fixed-point × 10,000), quantity, side, and time-in-force. The message is replicated across the 3-node Raft cluster before `OmsClusteredService.onSessionMessage()` fires on the leader.

Inside `onSessionMessage()`, the service first drains any pending `ChildOrderIntent` messages from algo-sor (via `pollIntents()`), then dispatches to `handleNewOrderSingle()`. A slot is allocated from `OrderBook`. The system assigns a monotonically incrementing `orderId` (`nextOrderId++`), sets the initial state to `NEW`, and runs the 7-rule `ValidationEngine.validateNewOrder()`. If validation fails, the order is set to `REJECTED` and an exec report is sent back to the client via cluster egress; the slot is freed. If validation passes, the order is indexed in `OrderBook` and registered in the `seenClOrdIds` dedup map. An exec report confirming `NEW` state is sent to the client.

### 3.2 Routing to algo-sor

After a parent order passes validation and is recorded in `OrderBook`, it is published to `algo-sor` via Aeron IPC stream 30 as a 129-byte `ClusterMessageType.NEW_ORDER` message. The algo-sor process (running `AlgoSorAgent` in a dedicated `AgentRunner` thread) receives the order and routes it via `SmartOrderRouter`, `IcebergAlgoEngine`, or `TwapAlgoEngine`. Each routing decision produces one or more `ChildOrderIntent` messages published back to `oms-core` on stream 12.

### 3.3 Child Order Creation

When a `ChildOrderIntent` arrives on stream 12, `OmsClusteredService.onChildOrderIntent()` executes on the Raft commit thread. The flow is:

1. Wrap intent view over the inbound buffer (zero-copy).
2. Look up parent in `OrderBook` by `parentOrderId`.
3. Compute `liveChildQty` by walking the parent's child linked list in `ChildOrderRegistry.computeLiveChildQty()`.
4. Run all 7 rules of `ChildOrderIntentValidator`. If any rule fails, the intent is silently dropped (logged at DEBUG).
5. Assign `childOrderId = nextOrderId++`.
6. Call `ChildOrderRegistry.createChild()`, which allocates a slot from the off-heap store, populates child fields, and prepends the child to the parent's linked list.
7. If the parent is in state `NEW`, transition it to `ROUTING` via `OrderStateMachine.transitionToRouting()`.
8. Dispatch a FIX Binary NOS (76 bytes) to the FIX connectivity engine on stream 10 via `FIXMessageEncoder.sendNewOrderSingle()`.

The child `clOrdId` is computed deterministically as `parentOrderId × 10,000 + (sliceIndex & 0xFF)`, making it globally unique, self-describing, and FIX-safe.

### 3.4 Fill Receipt and Aggregation

Venue fill reports arrive as 76-byte FIX Binary `ExecutionReport` (MsgType=8) messages on stream 11. `OmsClusteredService.handleExecReport()` decodes the `execType` byte and maps it to an `OrderEvent`. If the `clOrdId` matches a child order, `ChildOrderRegistry.applyFillAndAggregate()` applies the fill to the child and propagates the quantity delta to the parent. When the child's `leavesQty` reaches zero it is marked `FILLED`; the parent's state transitions to `PARTIALLY_FILLED` if `filledQty > 0` and `leavesQty > 0`, or `FILLED` if `leavesQty == 0`. A parent exec report is sent to the originating cluster client via cluster egress.

### 3.5 Terminal States

An order reaches a terminal state (`FILLED`, `CANCELED`, `REJECTED`, or `EXPIRED`) via the `OrderStateMachine`. Terminal orders are unindexed from `OrderBook` (both clOrdId and orderId maps) and their slot is returned to the free-slot stack. Terminal child orders are removed from `ChildOrderRegistry` indexes via `removeTerminal()`.

### 3.6 Failover Recovery

On leader failure, the Raft cluster elects a new leader (typically < 1 second). The new leader calls `OmsClusteredService.onStart()` with the most recent snapshot image. `ParentOrderState.registerTransitions()` is called first (required before restoring any ROUTING-state orders). The snapshot is deserialized: `OrderBook` is reset and re-hydrated order-by-order, `ValidationEngine.seenClOrdIds` is rebuilt entry-by-entry, and `ChildOrderRegistry` is restored in bulk. `recomputeNextOrderId()` scans both stores for the maximum orderId. Finally, `republishRoutingOrders()` re-sends all `NEW`, `ROUTING`, and `PARTIALLY_FILLED` parent orders to algo-sor, which re-routes them. `ChildOrderIntentValidator` rule 7 (over-allocation guard) prevents any duplicate child creation for quantity already in the restored `ChildOrderRegistry`.

---

## Section 4 — Child Order Lifecycle

### 4.1 Intent-Based Slicing Protocol

`algo-sor` is a pure computation engine. It never allocates child order records or sends orders directly to venues. Instead, for each parent order it receives, it runs a routing computation (SOR, Iceberg, or TWAP) and publishes one or more `ChildOrderIntent` messages back to `oms-core`. Each intent contains: `parentOrderId`, `parentClOrdId`, `sliceQty`, `limitPrice` (fixed-point), `venueId`, `algoType` (1=SOR, 2=ICEBERG, 3=TWAP), `sliceIndex` (0-based byte), and `intentTimestampNanos`. The intent is 56 bytes, transmitted as a 64-byte message (8-byte header + 56-byte payload) on stream 12.

### 4.2 Intent Validation Gate

All 7 rules of `ChildOrderIntentValidator.validate()` must return `PASS (= 0)` before `ChildOrderRegistry.createChild()` is called. See Section 5.2 for the complete rule table. The critical constraint is rule 7 (over-allocation guard): `liveChildQty + sliceQty ≤ parent.leavesQty`. `computeLiveChildQty()` walks the parent's child linked list immediately before each intent is validated, so the check is current for the cycle.

### 4.3 Child Order Identity

The child `clOrdId` formula guarantees global uniqueness and is self-describing:

```
childClOrdId = parentOrderId × 10,000 + (sliceIndex & 0xFF)
```

Dividing by 10,000 recovers the `parentOrderId`. The formula supports at most **256 unique children per parent order** (sliceIndex is a byte; values 0–255). `IcebergAlgoEngine` enforces this limit with a WARN log and break at `MAX_SLICES_PER_PARENT = 256`. At default settings (TWAP: 12 slices, Iceberg: ~10 peaks at `peakFraction=10`) this limit is not approached in normal operation.

### 4.4 Child-Parent Linked List

Children are linked to their parent via a singly-linked list stored entirely in the `ChildOrderRegistry` off-heap buffer:
- `parent.parentOrFirstChildId()` → `orderId` of the first (most recent) child (0 = no children).
- `child.nextSiblingSlot()` → slot index of the next sibling (-1 = end of list).
- `child.parentOrFirstChildId()` → `orderId` of its parent.

New children are prepended (O(1) insert). `cancelAllChildren()` and `computeLiveChildQty()` walk this list in O(n) where n = live child count (typically ≤ 20).

### 4.5 Post-Failover Behaviour

`IcebergAlgoEngine` and `TwapAlgoEngine` hold per-order state (remaining qty, slice schedules) in the `algo-sor` process. This state is lost on crash because only `oms-core` is snapshotted. On restore, `republishRoutingOrders()` re-sends active parent orders to algo-sor, which re-routes from scratch. The over-allocation guard (rule 7) prevents any slice that would exceed `parent.leavesQty - liveChildQty`, blocking double-slicing for child qty already committed in the restored `ChildOrderRegistry`.

---

## Section 5 — Validation Rules

### 5.1 New Order Validation (parent orders)

Source: `ValidationEngine.validateNewOrder()`. Rules are evaluated cheapest-first; the first failure returns without checking subsequent rules.

| Rule ID | Condition | Error Code | Business Meaning |
|---------|-----------|------------|-----------------|
| V-1 | `seenClOrdIds.get(clOrdId) != DEDUP_MISSING` | `ERR_DUPLICATE_CL_ORD_ID = 1` | A prior order with this client ID was already accepted. Protects against duplicate submission and Raft log replay creating phantom orders. |
| V-2 | `!Side.isValid(side)` — side not in {BUY=1, SELL=2, SELL_SHORT=5} | `ERR_INVALID_SIDE = 2` | Order must specify a valid FIX tag 54 side byte. |
| V-3 | `!TimeInForce.isValid(tif)` — tif not in {DAY=0, GTC=1, IOC=3, FOK=4, GTD=6} | `ERR_INVALID_TIF = 3` | Order must specify a supported FIX tag 59 time-in-force value. |
| V-4 | `qty <= 0` | `ERR_INVALID_QTY = 4` | Order quantity must be a positive integer. |
| V-5 | `price < 0` | `ERR_INVALID_PRICE = 5` | Price may be 0 (market orders) but must not be negative. Price is stored as fixed-point × 10,000. |
| V-6 | `!permittedSymbols.contains(symbol)` | `ERR_SYMBOL_NOT_WHITELISTED = 6` | The encoded symbol long is not in the operator-configured whitelist (`OMS_SYMBOLS`). |
| V-7 | `price > (maxNotional × PRICE_MULTIPLIER) / qty` (when price > 0) | `ERR_NOTIONAL_LIMIT = 7` | Order notional value (price × qty / 10,000) exceeds the per-order limit (`OMS_MAX_NOTIONAL`). Check uses integer division to avoid overflow. |

**Cancel request validation** uses `validateCancelRequest()`: new `clOrdId` must be fresh (V-1 equivalent), and `origClOrdId` must reference a previously accepted order (`ERR_INVALID_ACCOUNT = 8` reused as "order not found").

### 5.2 Intent Validation (child order creation)

Source: `ChildOrderIntentValidator.validate()`. All 7 rules must return `PASS (= 0)` before a child order is created. A failed intent is silently dropped (logged at DEBUG).

| Rule ID | Condition | Result Code | Business Meaning |
|---------|-----------|-------------|-----------------|
| I-1 | `parent == null` (parentOrderId not found in OrderBook) | `REJECT_PARENT_NOT_FOUND = 20` | The routing instruction references a parent that does not exist or has already been freed. |
| I-2 | `!isRoutableState(parentState)` — state not in {NEW=1, ROUTING=10, PARTIALLY_FILLED=2} | `REJECT_PARENT_NOT_ROUTABLE = 21` | A terminal, cancelled, or pending-cancel parent cannot accept new children. |
| I-3 | `sliceQty <= 0` | `REJECT_INVALID_SLICE_QTY = 22` | The child must carry a positive quantity. |
| I-4 | `sliceQty > parent.leavesQty` | `REJECT_SLICE_EXCEEDS_LEAVES = 23` | The child quantity cannot exceed the parent's remaining open quantity. |
| I-5 | `limitPrice <= 0` | `REJECT_INVALID_PRICE = 24` | Child order must have a positive limit price. |
| I-6 | `venueId <= 0` | `REJECT_INVALID_VENUE = 25` | A valid positive venue identifier is required to dispatch the NOS. |
| I-7 | `liveChildQty + sliceQty > parent.leavesQty` | `REJECT_QTY_OVERALLOCATION = 26` | Cumulative qty of all live children plus this slice would exceed the parent's remaining open quantity. This is the over-allocation guard, evaluated using the live child qty computed immediately before this validation call. |

---

## Section 6 — Non-Functional Requirements

### 6.1 Latency

**NFR-001 [SHALL]** The system targets deterministic sub-millisecond end-to-end order processing latency on the hot path.

This target is enforced by the following design invariants:
- **Zero object allocation** on the hot path (`onSessionMessage()`, `validateNewOrder()`, `validate()`, `createChild()`, `onFragment()`). All state uses pre-allocated flyweights (`OrderFlyweight`, `ChildOrderIntentFlyweight`) wrapping off-heap `UnsafeBuffer` stores. Agrona `Long2LongHashMap` and `LongHashSet` are used in place of Java standard collections to eliminate boxing.
- **Fixed-point arithmetic** for all prices (long × 10,000). No `double`, `float`, or `BigDecimal` on the hot path.
- **Busy-spin idle strategies** (`BusySpinIdleStrategy`) for all Aeron MediaDriver threads (conductor, sender, receiver) and for the `AlgoSorAgent` `AgentRunner`.
- **Single-threaded execution** per component. `OmsClusteredService` runs on the Raft commit thread. `AlgoSorAgent` runs in a dedicated `AgentRunner` thread. No `synchronized`, `AtomicReference`, or locks on the hot path.
- **Off-heap storage**: `ChildOrderRegistry` uses `UnsafeBuffer` backed by `ByteBuffer.allocateDirect()`. `OrderBook` uses an on-heap `byte[]` (deferred migration to off-heap; see Section 9.2).

**NFR-002 [SHOULD]** The performance harness (`oms-harness/perf`) measures p99 round-trip latency via `LatencyHistogram` (100,000 × 1 µs buckets, zero-allocation at record time). A p99 threshold is configurable via `PERF_P99_LIMIT_US` (default 100,000 µs).

### 6.2 Availability and Fault Tolerance

**NFR-003 [SHALL]** MarketOMS SHALL survive the failure of any single cluster node without interrupting order processing.

The system runs as a 3-node Aeron Cluster (Raft consensus). One node is the leader; two are followers. Every committed Raft log entry is replicated to and applied by all three nodes identically, keeping their in-memory state current at all times. On leader failure, the two remaining nodes hold a Raft election. The election is bounded by `leaderHeartbeatTimeoutNs` (default 2 seconds in source) and `startupCanvassTimeoutNs` (default 5 seconds). After election, the new leader calls `OmsClusteredService.onStart()` to restore state and begin accepting client sessions.

**NFR-004 [SHALL]** Recovery time objective (RTO) SHALL be bounded to a maximum of 5 minutes of Raft log replay.

A periodic snapshot timer (`SNAPSHOT_TIMER_ID`) fires every `SNAPSHOT_INTERVAL_NS = 5 minutes` (source: `OmsClusteredService.java`). On each fire, `ClusterControl.ToggleState.SNAPSHOT.toggle()` signals `ConsensusModule` to take a snapshot. The Raft log is truncated to the snapshot position; after a failure the new leader replays at most 5 minutes of committed log entries before reaching a current state.

**NFR-005 [SHALL]** `OMS_ARCHIVE_DIR` SHALL point to a durable filesystem (not `/tmp`). Default: `${user.home}/oms-archive-{nodeId}`. Both the archive directory and the cluster sub-directory (`archiveDir/cluster`) must be wiped together via `OMS_ARCHIVE_DELETE_ON_START=true` if a clean start is required — deleting only one leaves inconsistent Raft state.

### 6.3 Consistency

**NFR-006 [SHALL]** All order state mutations SHALL execute inside `onSessionMessage()` or `onLoadSnapshot()`, which fire only after Raft quorum commit, ensuring all replicas apply identical mutations in identical order.

The `OrderStateMachine.TRANSITION_TABLE` is a static `byte[16][10]` array initialized at class load time, identical on every JVM in the cluster. `ValidationEngine.seenClOrdIds` is snapshotted and restored, ensuring that Raft log replay after failover does not re-accept duplicate orders.

**NFR-007 [SHALL]** The `seenClOrdIds` dedup map SHALL prevent duplicate order creation during Raft log replay. Every accepted order's `clOrdId` is written to `seenClOrdIds` before `onSessionMessage()` returns; replayed identical messages are rejected by rule V-1 (`ERR_DUPLICATE_CL_ORD_ID`).

### 6.4 Durability

**NFR-008 [SHALL]** The snapshot SHALL include the complete order state: `OrderBook` (all parent order records), `ValidationEngine.seenClOrdIds` (dedup map), and `ChildOrderRegistry` (all child order records). Snapshot version 3 header embeds the capacity values (`snapshotMaxOrders`, `snapshotMaxChildren`) at which the snapshot was taken; restore rejects any configuration that would reduce capacity below the snapshot's values.

**NFR-009 [SHOULD]** If the leader crashes between `ChildOrderRegistry.createChild()` and `FIXMessageEncoder.sendNewOrderSingle()`, the child order exists in the registry (and will be in the next snapshot) but no NOS was sent to the venue. Reconciliation is recommended via FIX `OrderStatusRequest (35=H)` on reconnect. This reconciliation is not currently implemented (see Section 9.1).

**NFR-010 [SHALL]** algo-sor in-process state (`IcebergAlgoEngine` remaining qty, `TwapAlgoEngine` handle arrays) is ephemeral and is NOT snapshotted. After failover, `republishRoutingOrders()` re-bootstraps algo-sor by re-sending all active parent orders. The over-allocation guard (I-7) prevents double-slicing.

---

## Section 7 — Configuration Reference

All parameters are resolved at startup by `OmsConfig.load(ConfigSource)`. When `OMS_CONFIG_FILE` is set, file values take priority over environment variables (`ChainedConfigSource`). Every resolved value is logged at INFO with its source name. Invalid values (negative, non-integer, blank symbol list) cause `IllegalArgumentException` before any Aeron infrastructure starts.

### 7.1 Capacity and Tuning Parameters (OmsConfig)

| Env Var | Default | Component | Effect | Constraint |
|---------|---------|-----------|--------|------------|
| `OMS_MAX_ORDERS` | 65,536 | `OrderBook` | Maximum simultaneously open parent orders. Pre-allocates backing array and two `Long2LongHashMap` indexes. | Changing after a snapshot requires archive wipe (`OMS_ARCHIVE_DELETE_ON_START=true` for one restart). Snapshot header validates current ≥ snapshot value. |
| `OMS_MAX_CHILDREN` | 32,768 | `ChildOrderRegistry` | Maximum simultaneously open child orders. Pre-allocates off-heap `UnsafeBuffer` store and indexes. | Same archive-wipe requirement as `OMS_MAX_ORDERS`. |
| `OMS_INTENT_FRAGMENT_LIMIT` | 20 | `OmsClusteredService` | Maximum `ChildOrderIntent` fragments polled per `onSessionMessage()` / `onTimerEvent()` call. Bounds intent processing per Raft commit cycle. | Must be ≥ 1. |
| `OMS_ALGO_FRAGMENT_LIMIT` | 10 | `AlgoSorAgent` | Maximum parent-order fragments polled per `doWork()` cycle. Bounds parent order processing per agent duty cycle. | Must be ≥ 1. |
| `OMS_MAX_VENUES` | 10 | `SmartOrderRouter`, `AlgoSorAgent` | Maximum venues `SmartOrderRouter` can distribute across. Venue arrays pre-allocated at this size. | Must be ≥ 1. |
| `OMS_MAX_NOTIONAL` | 10,000,000 | `ValidationEngine` | Per-order notional limit in base currency units (not fixed-point). E.g. 10,000,000 = £10 million. Enforced by rule V-7. | Must be ≥ 1. |
| `OMS_SYMBOLS` | `AAPL,MSFT,GOOG,AMZN` | `ValidationEngine` | Comma-separated ASCII symbol whitelist. Each symbol (up to 8 chars) is encoded as a `long` at startup and stored in `LongHashSet`. | Must not be blank. Symbols > 8 chars are silently truncated. |
| `OMS_MAX_ICEBERG_ORDERS` | 4,096 | `IcebergAlgoEngine` | Maximum concurrently tracked iceberg orders. Maps pre-sized to `maxConcurrent × 2` at load factor 0.65. | Must be ≥ 1. Exceeding causes hot-path rehash. |
| `OMS_MAX_TWAP_ORDERS` | 512 | `TwapAlgoEngine` | Maximum concurrently tracked TWAP orders. All per-handle arrays pre-allocated at this size. | Must be ≥ 1. Exhaustion falls back to single-slice dispatch with WARN log. |
| `OMS_CONFIG_FILE` | (none) | `OmsNode`, `OmsLauncher` | Optional path to a `.properties` config file. File values take priority over env vars when set. | No validation — file must be readable at startup or an exception is thrown. |

### 7.2 Infrastructure Parameters (OmsNode — not in OmsConfig)

| Env Var | Default | Component | Effect |
|---------|---------|-----------|--------|
| `OMS_NODE_ID` | 0 | `OmsNode` | Integer node ID (0, 1, or 2) identifying this node within `OMS_CLUSTER_MEMBERS`. |
| `OMS_AERON_DIR` | `/dev/shm/oms-aeron-{nodeId}` | MediaDriver, Aeron client | Shared memory directory for the embedded Aeron MediaDriver. Wiped on start and shutdown. |
| `OMS_ARCHIVE_DIR` | `${user.home}/oms-archive-{nodeId}` | Archive, ConsensusModule | Persistent directory for the Raft log archive. Must be on a durable filesystem. Never `/tmp`. |
| `OMS_ARCHIVE_DELETE_ON_START` | `false` | Archive, ConsensusModule | When `true`, wipes both the archive directory and the cluster sub-directory on every restart. Set `true` for the test harness; set `false` for production. |
| `OMS_CLUSTER_MEMBERS` | `0,localhost:9000,...,localhost:8010` | ConsensusModule | Pipe-separated Raft member descriptors: `nodeId,clientPort,memberPort,logPort,transferPort,archivePort`. For multi-node, replace `localhost` with actual IPs. |
| `OMS_CLUSTER_INGRESS_CHANNEL` | `aeron:udp?endpoint=localhost:9000` | ConsensusModule | Ingress channel where this node listens for cluster client sessions. Must match the client port in `OMS_CLUSTER_MEMBERS`. |
| `OMS_ARCHIVE_CONTROL_CHANNEL` | `aeron:udp?endpoint=localhost:8010` | Archive | UDP channel the Archive binds for control requests. |
| `OMS_ARCHIVE_LOCAL_CONTROL_CHANNEL` | `aeron:ipc` | AeronArchive.Context | Channel used by ConsensusModule and ClusteredServiceContainer to reach the co-located archive. Must be `aeron:ipc` when archive is embedded in-process. |
| `OMS_ARCHIVE_LOCAL_RESPONSE_CHANNEL` | `aeron:ipc` | AeronArchive.Context | Response channel for archive control requests. Must be `aeron:ipc` for embedded archive. |
| `OMS_ARCHIVE_REPLICATION_CHANNEL` | `aeron:udp?endpoint=localhost:0` | Archive, ConsensusModule | Channel for cross-node log replication. Use a fixed port (e.g. `:8020`) in multi-node deployments. |

### 7.3 Test Harness Parameters

| Env Var | Default | Component | Effect |
|---------|---------|-----------|--------|
| `HARNESS_CLUSTER_INGRESS` | `0=localhost:9000` | `HarnessLauncher`, `PerfHarnessLauncher` | Cluster ingress endpoint in `nodeId=host:port` format. |
| `HARNESS_AERON_DIR` | `/tmp/oms-aeron-harness` | `HarnessLauncher` | Aeron dir for the harness process. Different from the OMS node dir. |
| `HARNESS_TIMEOUT_MS` | 8,000 | `HarnessLauncher` | Per-scenario wait timeout in milliseconds. |

---

## Section 8 — Hard Limits and Ceilings

Source is authoritative for all values below.

| Limit | Current Value | Location | What Happens When Reached | Configurable? |
|-------|---------------|----------|--------------------------|---------------|
| Maximum open parent orders | 65,536 | `OrderBook.MAX_ORDERS` | `allocateSlot()` returns -1; `handleNewOrderSingle()` sends REJECTED exec report to client | Yes — `OMS_MAX_ORDERS` |
| Maximum open child orders | 32,768 | `ChildOrderRegistry.DEFAULT_CAPACITY` | `createChild()` returns null; intent is dropped with WARN log | Yes — `OMS_MAX_CHILDREN` |
| Maximum child orders per parent | 256 | `IcebergAlgoEngine.MAX_SLICES_PER_PARENT` | `IcebergAlgoEngine.onSlice()` logs WARN and stops dispatching; remaining qty is silently unrouted | No — hard limit of the `childClOrdId` formula (sliceIndex is a byte) |
| Maximum concurrent iceberg orders | 4,096 | `IcebergAlgoEngine` constructor default | Maps would rehash on the hot path if exceeded | Yes — `OMS_MAX_ICEBERG_ORDERS` |
| Maximum concurrent TWAP orders | 512 | `TwapAlgoEngine.MAX_TWAP_ORDERS` | `acquireHandle()` returns -1; `onSlice()` logs WARN and falls back to single-slice dispatch | Yes — `OMS_MAX_TWAP_ORDERS` |
| Maximum venues for routing | 10 | `SmartOrderRouter.MAX_VENUES` | Venue arrays in `AlgoSorAgent` are pre-allocated at this size; excess venues are ignored | Yes — `OMS_MAX_VENUES` |
| Order state table dimensions | 16 states × 10 events | `OrderState.NUM_STATES`, `OrderEvent.NUM_EVENTS` | Adding a state ≥ 16 causes `ArrayIndexOutOfBoundsException` in `OrderStateMachine.transition()` | No — requires source change |
| OrderLayout record size | 128 bytes | `OrderLayout.BLOCK_LENGTH` / `MESSAGE_SIZE` | Static assert fires at JVM startup if a field is added past offset 120 | No — requires wire format change |
| ChildOrderIntentFlyweight record size | 56 bytes | `ChildOrderIntentFlyweight.BLOCK_LENGTH` | Layout is fixed; adding fields requires protocol version bump | No — requires wire format change |
| Cluster ingress/egress message size | 129 bytes | `ClusterMessageType.IPC_MESSAGE_SIZE` | Fixed (1-byte type + 128-byte OrderLayout) | No — tied to OrderLayout.MESSAGE_SIZE |
| FIX Binary message size | 76 bytes | `FIXMessageDecoder.FIX_BINARY_SIZE` | Fixed binary format between OMS and FIX connectivity engine | No |
| Snapshot version | 3 | `SnapshotManager.SNAPSHOT_VERSION` | Version mismatch at restore throws `IllegalStateException`; wipe archive with `OMS_ARCHIVE_DELETE_ON_START=true` | No — requires source change |
| Snapshot header size | 24 bytes | `SnapshotManager.HEADER_SIZE` | First 24 bytes reserved for version, counts, and capacity metadata | No |
| Snapshot buffer size at defaults | ≈ 13.0 MB | Computed in `SnapshotManager(int, int)` constructor | Buffer computed as `24 + maxOrders×128 + maxOrders×16 + 4 + maxChildren×128` at construction; not a static constant | Indirectly — changes with `OMS_MAX_ORDERS` / `OMS_MAX_CHILDREN` |

> ⚠ Conflict C-1: `SnapshotManager.java` source comment says 80 bytes per parent record. Source behavior and design docs use 128 bytes (`OrderLayout.MESSAGE_SIZE`). Spec reflects source behavior.

---

## Section 9 — Out of Scope and Known Limitations

### 9.1 Not Implemented (Planned)

**P-1 — Horizontal Sharding (Symbol Partitioning)**
MarketOMS currently runs as a single shard: all symbols are processed by one Aeron Cluster (3-node Raft group). `decisions.md` Section 10.2–10.3 documents the horizontal scaling path via symbol-partitioned sharding. When a shard is added, `OmsLauncher.java` would need new channel wiring, `OmsNode.java` would need a new env var for shard routing, and `AeronTransport` would need per-shard stream ID constants. Cross-shard account-level notional limits are explicitly noted as not yet implemented. No code changes have been made toward this.
*Source: `design/decisions.md` Sections 10.2–10.4.*

**P-2 — Live Market Data Feed for AlgoSorAgent**
`AlgoSorAgent` currently uses a static mock market depth (`loadDefaultDepth()`) that routes all quantity to venue 1 at a hardcoded benchmark price (£100.0000). In production, a market data handler would update `venueIds`, `venuePrices`, and `venueQtys` arrays dynamically. The `SmartOrderRouter.route()` method already accepts these arrays as parameters; the feed wiring is the missing piece.
*Source: `AlgoSorAgent.java:180–189`, comment "Mock market depth — updated by a market data handler in production".*

**P-3 — Per-Strategy Routing Selection in AlgoSorAgent**
`AlgoSorAgent.onFragment()` currently always routes via `SmartOrderRouter`. The code contains commented-out stubs for `icebergEngine.onSlice()` and `twapEngine.onSlice()` guarded on a strategy byte: "For algo orders, call the appropriate engine based on strategy byte (future work)". `IcebergAlgoEngine` and `TwapAlgoEngine` are wired and pre-allocated but are not dispatched from the live fragment handler.
*Source: `AlgoSorAgent.java:143–145`.*

**P-4 — FIX OrderStatusRequest (35=H) Reconciliation**
If the leader crashes between `ChildOrderRegistry.createChild()` and `FIXMessageEncoder.sendNewOrderSingle()`, a child order exists in the registry but no NOS was sent to the venue. The source code comments this condition and recommends reconciliation via FIX `OrderStatusRequest (35=H)` on FIX session reconnect. No reconciliation handler is implemented.
*Source: `OmsClusteredService.java:324–326`.*

### 9.2 Known Gaps (Deferred)

**D-1 — Single-Node Default Provides No Fault Tolerance (MEDIUM)**
The default `OMS_CLUSTER_MEMBERS` value configures a single node (`0,localhost:9000,...`). A single-node deployment has no Raft quorum tolerance: any restart causes a full leader election with only one candidate, and the system is unavailable during the restart window. Production deployments MUST override `OMS_CLUSTER_MEMBERS` with a 3-node configuration. No code change is required — this is an operational configuration requirement.
*Source: `design/resilience-review.md` M1. Status: DOCUMENTED (operational requirement).*

**D-2 — OrderBook Backing Array Is On-Heap (LOW)**
`OrderBook` uses a `byte[]` backing array (`new byte[maxOrders × 128]`). At the default 65,536 orders this is an 8 MB on-heap allocation. While it is promoted to the old generation immediately (not a hot-path GC concern), it is inconsistent with `ChildOrderRegistry` which uses `ByteBuffer.allocateDirect()`. Replacement with `UnsafeBuffer` over a direct `ByteBuffer` has been deferred. `OMS_MAX_ORDERS` is configurable, which partially mitigates the concern by allowing right-sizing.
*Source: `design/resilience-review.md` L1. Status: DOCUMENTED (deferred). `OrderBook.java:76`: `this.backingArray = new byte[maxOrders * OrderLayout.MESSAGE_SIZE]`.*

> ⚠ Conflict C-2: `design/resilience-review.md` C1 Fix description references `SNAPSHOT_VERSION = 2` and `MAX_SNAPSHOT_BYTES ≈ 10.5 MB`. Current source: `SNAPSHOT_VERSION = 3`, no static constant, buffer ≈ 13.0 MB. Spec reflects source.
