# MarketOMS — Functional Specification

Generated: 2026-06-05
Source: design/ docs + source as of master HEAD aa387fb

> This document is derived from design/ and source. Do not edit directly.
> To update: change the relevant design/ file or source, then re-run
> skills/workflows/spec-generator/SKILL.md.

## Conflicts with design docs

| ID | Design Doc Location | Design Doc States | Source Shows | Impact |
|----|--------------------|--------------------|--------------|--------|
| C-3 | `design/failover.md`, recovery sequence step 8 | "`OrderBook.restoreOrder()` copies 80 bytes" | `OrderBook.java:245` copies `OrderLayout.MESSAGE_SIZE = 128` bytes | Section 6.4, Section 8 |

Recommended: Update `design/failover.md` step 8 — replace "80 bytes" with
"128 bytes (`OrderLayout.MESSAGE_SIZE`)". Also update `OrderBook.java` class-level
Javadoc (lines 17–20) and line 244 comment which carry the same stale slot
arithmetic (source-internal stale comments — runtime behaviour is correct).

---

## Section 1 — System Overview

MarketOMS is a sell-side Order Management System that receives parent equity
orders from buy-side clients, validates them, routes them to execution venues via
algorithmic slicing, and aggregates venue execution reports back into the parent
order record. It is designed for a single-shard, single-symbol-universe trading
operation where deterministic sub-millisecond latency and zero data loss under
node failure are primary constraints.

The system runs as a three-node Aeron Cluster (Raft consensus) so that any single
node failure — including the current leader — causes automatic failover without
manual intervention and without losing any committed order state. All mutable
state (parent orders, child orders, deduplication table) is snapshotted every five
minutes and is fully restored after failover.

The system is built in Java 21 on Aeron 1.47.0 and Agrona 2.0.1. The core design
constraint is zero object allocation on every hot-path operation: no `new`, no
boxing, no standard Java collections on any code path that fires during order
processing. All mutable state lives off-heap in pre-allocated `UnsafeBuffer` stores;
all message parsing uses flyweights (`OrderFlyweight`, `ChildOrderIntentFlyweight`)
that re-wrap the same buffer on each message rather than constructing new objects.

---

## Section 2 — Actors and External Interfaces

### 2.1 External Interface Table

| Actor | Interface | Protocol | Stream / Port | Direction | Owning Class |
|-------|-----------|----------|--------------|-----------|--------------|
| Buy-side client (order submitter) | Aeron Cluster ingress | `ClusterMessageType` framing (1-byte type + 128-byte `OrderLayout`) | UDP port 9000 (default; `OMS_CLUSTER_INGRESS_CHANNEL`) | Inbound | `OmsClusteredService` via `ConsensusModule` |
| Buy-side client | Aeron Cluster egress | `ClusterMessageType` framing (129-byte response) | Cluster egress session | Outbound | `OmsClusteredService.sendEgressExecReport()` |
| FIX connectivity engine | Aeron IPC | FIX Binary (76-byte NOS / Cancel / Replace) | Stream 10 (`STREAM_OMS_TO_FIX`) | Outbound | `FIXMessageEncoder` |
| FIX connectivity engine | Aeron IPC | FIX Binary (76-byte ExecReport) | Stream 11 (`STREAM_FIX_TO_OMS`) | Inbound | `OmsClusteredService.handleExecReport()` |
| `algo-sor` process | Aeron IPC | `NEW_ORDER (1)` + `OrderLayout` (129 bytes) | Stream 30 (`STREAM_OMS_TO_ALGO`) | Outbound | `OmsClusteredService.algoSorPublication` |
| `algo-sor` process | Aeron IPC | `CHILD_ORDER_INTENT (40)` (8-byte header + 56-byte intent) | Stream 12 (`STREAM_CHILD_INTENTS`) | Inbound | `OmsClusteredService.intentSub` |
| Cluster operator | Environment variables / properties file | `OmsConfig` (`OMS_CONFIG_FILE`) | Process launch | Config | `OmsNode.loadOmsConfig()`, `OmsLauncher.loadOmsConfig()` |

### 2.2 Message Type Constants

| Constant | Value | Direction | Meaning |
|----------|-------|-----------|---------|
| `ClusterMessageType.NEW_ORDER` | 1 | Client → cluster; cluster → algo-sor | New Order Single |
| `ClusterMessageType.CANCEL_ORDER` | 2 | Client → cluster | Cancel Request |
| `ClusterMessageType.REPLACE_ORDER` | 3 | Client → cluster | Cancel/Replace |
| `ClusterMessageType.CHILD_ORDER_INTENT` | 40 | algo-sor → cluster | Slice routing instruction |

### 2.3 Image Availability (SHALL)

The `intentSub` subscription (stream 12) MUST be created with
`availableImageHandler` and `unavailableImageHandler`. An algo-sor disconnect
logs WARN; absence of the handlers would leave the disconnect invisible and
parent orders stalled in `ROUTING` state indefinitely.

---

## Section 3 — Order Lifecycle

### 3.1 Order Ingress

A new order arrives at `OmsClusteredService.onSessionMessage()` after Raft quorum
commit — meaning all three replica nodes execute the same call in the same
position in the log. The message is a 129-byte `ClusterMessageType`-framed
`OrderLayout` record. The service dispatches on the `msgType` byte:

- `NEW_ORDER (1)` → `handleNewOrder()`
- `CANCEL_ORDER (2)` → `handleCancelRequest()`
- `REPLACE_ORDER (3)` → `handleCancelReplace()`

Before any order processing, `pollIntents()` drains pending `ChildOrderIntent`
messages from the algo-sor IPC subscription (up to `OMS_INTENT_FRAGMENT_LIMIT`
fragments per cycle).

### 3.2 New Order Validation

`handleNewOrder()` wraps the inbound buffer in `decodeFlyweight` (pre-allocated
`OrderFlyweight`) and calls `ValidationEngine.validateNewOrder()`. Seven rules
are evaluated cheapest-first (see Section 5.1). On any failure, an egress exec
report with the reject code is sent via `sendEgressExecReport()` and processing
stops. No state is mutated on a reject.

### 3.3 Slot Allocation and Initial State

On `VALID`, an `OrderBook` slot is allocated via `allocateSlot()`. The flyweight
is initialised: `leavesQty` set to `qty`, `orderState` set to `NEW (1)`.
`ValidationEngine.registerAccepted(clOrdId, orderId)` records the order in the
dedup map. `nextOrderId` is incremented.

The order is then published to `algo-sor` on stream 30 as a 129-byte
`ClusterMessageType + OrderLayout` message.

### 3.4 Routing via algo-sor

`AlgoSorAgent.doWork()` polls stream 30 (up to `OMS_ALGO_FRAGMENT_LIMIT`
fragments per cycle). For each `NEW_ORDER` fragment it calls
`SmartOrderRouter.route()`, which computes venue slices from mock market depth
and calls `publishIntent()` once per venue via the `ChildIntentSink` callback.
Each intent is published on stream 12 as a 64-byte frame (8-byte header +
56-byte `ChildOrderIntentFlyweight`).

TWAP and Iceberg algorithm dispatch is **[PLANNED]** — see Section 9.1.

### 3.5 Child Order Creation

Back in `OmsClusteredService.pollIntents()`, each inbound intent is validated by
`ChildOrderIntentValidator` (7 rules, Section 5.2). On `PASS`:

1. `ChildOrderRegistry.createChild()` allocates an off-heap slot and sets
   `childClOrdId = parentOrderId × 10,000 + (sliceIndex & 0xFF)`.
2. The child is linked to the parent via a singly-linked list:
   `parent.parentOrFirstChildId` holds the first child's orderId;
   `child.nextSiblingSlot` chains subsequent siblings.
3. `OrderStateMachine.transitionToRouting()` moves the parent from `NEW` to
   `ROUTING (10)`.
4. `FIXMessageEncoder.sendNewOrderSingle()` publishes the child NOS to the FIX
   connectivity engine on stream 10 (76-byte FIX Binary format).

### 3.6 Fill Receipt and Aggregation

Execution reports arrive on stream 11 as 76-byte FIX Binary messages.
`FIXMessageDecoder.decodeExecReport()` reads the `execType` byte and maps it to
an `OrderEvent` constant via `mapExecTypeToEvent()`.

`ChildOrderRegistry.applyFillAndAggregate()` locates the child by `clOrdId`,
applies the partial or full fill to `filledQty` / `leavesQty`, and returns the
parent flyweight (`secondFlyweight`) for the parent to be updated.
`OrderStateMachine.applyTransition()` moves the parent state accordingly
(e.g. `ROUTING → PARTIALLY_FILLED` on a partial fill).

A retransmitted fill for an already-terminal child returns null from
`applyFillAndAggregate()` and is discarded.

### 3.7 Terminal States

| Terminal State | Byte | Reached When |
|---------------|------|--------------|
| `FILLED` | 6 | `leavesQty == 0` after a full fill |
| `CANCELED` | 7 | Cancel confirmed by venue (`EXEC_CANCELED`) |
| `REJECTED` | 8 | NOS or replace rejected by venue |
| `EXPIRED` | 9 | DAY or GTD order expired |

On any terminal state, `OrderBook.unindex()` removes the order from both lookup
maps and `freeSlot()` returns the slot to the free stack.

### 3.8 Failover Mid-Lifecycle

If the leader crashes while orders are in `NEW`, `ROUTING`, or
`PARTIALLY_FILLED` state, the new leader restores `OrderBook` and
`ChildOrderRegistry` from the most recent snapshot, replays the Raft log to the
current position, and then calls `republishRoutingOrders()` to re-send all
non-terminal parent orders to `algo-sor`. `ChildOrderIntentValidator` rule 7
(over-allocation guard) prevents double-slicing for qty already held in live
child orders.

---

## Section 4 — Child Order Lifecycle

### 4.1 Intent-Based Slicing Protocol

`algo-sor` is a stateless computation engine. It receives a parent order, runs
routing arithmetic, and publishes one or more `ChildOrderIntent` records back to
`oms-core`. It never writes to `OrderBook`, `ChildOrderRegistry`, or
`FIXMessageEncoder`. All child order records are born exclusively inside
`oms-core`.

### 4.2 ChildOrderIntent Contents

A `ChildOrderIntentFlyweight` (`BLOCK_LENGTH = 56` bytes) carries:

| Field | Type | Meaning |
|-------|------|---------|
| `parentOrderId` | long | Identifies the parent in `OrderBook` |
| `parentClOrdId` | long | Parent's client-assigned order ID |
| `sliceQty` | long | Quantity to allocate to this child |
| `limitPrice` | long | Fixed-point limit price (× 10,000) |
| `venueId` | int | Target venue (maps to an Aeron publication stream) |
| `algoType` | byte | 1=SOR, 2=ICEBERG, 3=TWAP |
| `sliceIndex` | byte | 0-based slice counter for clOrdId uniqueness |
| `intentTimestampNanos` | long | Intent creation timestamp |

### 4.3 Child clOrdId Uniqueness

```
childClOrdId = parentOrderId × 10,000 + (sliceIndex & 0xFF)
```

This formula is self-describing (divide by 10,000 to recover `parentOrderId`),
globally unique within a session, and FIX-safe (fits in a long). The `& 0xFF`
limits `sliceIndex` to 0–255, capping uniquely addressable children per parent
at **256**. `IcebergAlgoEngine` enforces this with a WARN + break at
`MAX_SLICES_PER_PARENT = 256`.

### 4.4 Seven Intent Validation Rules

See Section 5.2 for the full rule table. The critical rule is rule 7
(over-allocation guard): `liveChildQty + sliceQty ≤ parent.leavesQty`.
`computeLiveChildQty()` walks the child linked list immediately before each
intent is processed to give an accurate current count.

### 4.5 Child Orders on Failover

`ChildOrderRegistry` is fully serialized in every Aeron Cluster snapshot
(snapshot version 3, see Section 6.4). After failover and restore, all in-flight
child orders are present in the registry. When `republishRoutingOrders()`
re-routes parent orders to `algo-sor`, rule 7 in `ChildOrderIntentValidator`
uses the restored `liveChildQty` to block any slice that would over-commit
the already-outstanding child qty.

---

## Section 5 — Validation Rules

### 5.1 New Order Validation (parent orders)

Source: `ValidationEngine.validateNewOrder()`. Rules evaluated in order;
first failure returns immediately (no further checks).

| Rule | Condition Checked | Error Code | Business Meaning |
|------|-------------------|------------|-----------------|
| V-1 | `seenClOrdIds.get(clOrdId) != DEDUP_MISSING` | `ERR_DUPLICATE_CL_ORD_ID` (1) | Duplicate ClOrdID — order already accepted in this session |
| V-2 | `Side.isValid(side)` | `ERR_INVALID_SIDE` (2) | Side byte must be a recognised Buy/Sell constant |
| V-3 | `TimeInForce.isValid(timeInForce)` | `ERR_INVALID_TIF` (3) | TIF byte must be a recognised DAY/GTC/IOC/GTD constant |
| V-4 | `qty > 0` | `ERR_INVALID_QTY` (4) | Order quantity must be positive |
| V-5 | `price >= 0` | `ERR_INVALID_PRICE` (5) | Price must be zero or positive; zero is permitted for market orders |
| V-6 | `permittedSymbols.contains(symbol)` | `ERR_SYMBOL_NOT_WHITELISTED` (6) | Instrument not in the configured symbol whitelist (`OMS_SYMBOLS`) |
| V-7 | `price > (maxNotional × PRICE_MULTIPLIER) / qty` | `ERR_NOTIONAL_LIMIT` (7) | Order value exceeds the per-order notional limit (`OMS_MAX_NOTIONAL`) |

`ERR_INVALID_ACCOUNT (8)` is reserved for cancel validation
(`validateCancelRequest()`): the new ClOrdID on a cancel must be fresh, and
`origClOrdId` must refer to a live order. It is reused as an "order not found"
sentinel in the cancel path.

### 5.2 Intent Validation (child order creation)

Source: `ChildOrderIntentValidator.validate()`. All 7 rules must return `PASS`
before `ChildOrderRegistry.createChild()` is called.

| Rule | Condition Checked | Result Code | Business Meaning |
|------|-------------------|-------------|-----------------|
| I-1 | `parent != null` (found in `OrderBook`) | `REJECT_PARENT_NOT_FOUND` (20) | Intent references a parent that does not exist |
| I-2 | `parentState ∈ {NEW, ROUTING, PARTIALLY_FILLED}` | `REJECT_PARENT_NOT_ROUTABLE` (21) | Parent is terminal or pending cancel/replace — cannot accept new child |
| I-3 | `sliceQty > 0` | `REJECT_INVALID_SLICE_QTY` (22) | Slice quantity must be positive |
| I-4 | `sliceQty ≤ parent.leavesQty` | `REJECT_SLICE_EXCEEDS_LEAVES` (23) | Slice would exceed remaining unfilled quantity |
| I-5 | `limitPrice > 0` | `REJECT_INVALID_PRICE` (24) | Child order must have a positive limit price |
| I-6 | `venueId > 0` | `REJECT_INVALID_VENUE` (25) | Venue ID must be a positive routing target |
| I-7 | `liveChildQty + sliceQty ≤ parent.leavesQty` | `REJECT_QTY_OVERALLOCATION` (26) | Over-allocation guard: sum of all live child qty plus this slice must not exceed parent leaves |

---

## Section 6 — Non-Functional Requirements

### 6.1 Latency

**NFR-001 (SHALL)** The system MUST produce zero GC pressure on the hot path.
No `new` expressions are permitted in `onSessionMessage()`,
`validateNewOrder()`, `validate()`, `createChild()`, or `onFragment()`.
Enforced by flyweight patterns over pre-allocated `UnsafeBuffer` stores and
Agrona primitive collections (`Long2LongHashMap`, `LongHashSet`).

**NFR-002 (SHALL)** All components use busy-spin idle strategies
(`BusySpinIdleStrategy`). Back-pressure in `publishIntent()` spins with
`Thread.onSpinWait()` — never `Thread.sleep()`.

**NFR-003 (SHALL)** All prices are stored as fixed-point `long` values
multiplied by `PRICE_MULTIPLIER = 10,000`. No `double`, `float`, or
`BigDecimal` on the hot path. This eliminates non-deterministic floating-point
rounding differences across Raft replicas.

**NFR-004 (SHALL)** All capacity structures are pre-allocated at construction.
`OrderBook` (`byte[]`), `ChildOrderRegistry` (`UnsafeBuffer`), and all Agrona
map indexes (`Long2LongHashMap`, `LongHashSet`) are pre-sized to `2 × capacity`
at load factor 0.65f. No rehash allocations occur on the hot path.

### 6.2 Availability and Fault Tolerance

**NFR-005 (SHALL)** The system MUST tolerate any single node failure without
data loss. Three Aeron Cluster nodes maintain a Raft log; any two form a quorum.
Leader failover is transparent to clients once the new leader completes
`OmsClusteredService.onStart()`.

**NFR-006 (SHALL)** A periodic snapshot timer (`SNAPSHOT_TIMER_ID`,
`SNAPSHOT_INTERVAL_NS = 5 minutes`) schedules Aeron Cluster snapshots via
`ClusterControl.ToggleState.SNAPSHOT.toggle()`. This bounds the Raft log replay
window to at most 5 minutes of transactions on recovery.

**NFR-007 (SHALL)** After every snapshot restore, `republishRoutingOrders()`
MUST re-publish all `NEW`, `ROUTING`, and `PARTIALLY_FILLED` parent orders to
`algo-sor` on stream 30. `algo-sor` holds no durable state; this re-seeds its
computation cycle.

**NFR-008 (SHALL)** The `intentSub` Aeron subscription (stream 12) MUST have
`availableImageHandler` and `unavailableImageHandler`. A disconnect is logged at
WARN level.

**M1 (DOCUMENTED — operational)** The default `OMS_CLUSTER_MEMBERS` value is a
single-node configuration. Production deployments MUST set this to a 3-node
configuration to achieve the fault-tolerance guarantee in NFR-005.

### 6.3 Consistency

**NFR-009 (SHALL)** All order state mutations (`OrderBook`, `ChildOrderRegistry`)
MUST execute inside `OmsClusteredService.onSessionMessage()` or
`onLoadSnapshot()`, which fire only after Raft quorum commit. This guarantees
identical mutation sequences across all three replicas.

**NFR-010 (SHALL)** `ParentOrderState.registerTransitions()` MUST be called
before any `AgentRunner` starts and before snapshot load in `onStart()`.
This populates `OrderStateMachine.TRANSITION_TABLE[10][*]` for the `ROUTING`
state. Absence causes silent fill drops against ROUTING parent orders.

**NFR-011 (SHALL)** `ValidationEngine.seenClOrdIds` provides idempotent
validation. Every accepted ClOrdID is registered before `validateNewOrder()`
returns. Raft log replay after failover re-submits committed messages; each
replayed NOS is rejected by `ERR_DUPLICATE_CL_ORD_ID` before state mutation.

### 6.4 Durability

**NFR-012 (SHALL)** The following components MUST be serialized in
`SnapshotManager.takeSnapshot()` and restored in `loadSnapshot()`:
`OrderBook`, `ValidationEngine.seenClOrdIds`, and `ChildOrderRegistry`.

Snapshot format (version 3 — source: `SnapshotManager.java`):

```
[0-3]   version (int) = 3
[4-7]   orderCount (int)
[8-11]  dedupCount (int)
[12-15] reserved (int) = 0
[16-19] snapshotMaxOrders (int) — OMS_MAX_ORDERS at snapshot time
[20-23] snapshotMaxChildren (int) — OMS_MAX_CHILDREN at snapshot time
[24 .. 24+orderCount×128]  parent order records (128 bytes each — OrderLayout.MESSAGE_SIZE)
[.. + dedupCount×16]       dedup entries (clOrdId:8 + orderId:8 = 16 bytes)
[.. + 4 + childCount×128]  child registry (int count + BLOCK_LENGTH records)
```

> ⚠ Conflict C-3: `design/failover.md` recovery sequence step 8 states
> `restoreOrder()` copies "80 bytes." Source (`OrderBook.java:245`) copies
> `OrderLayout.MESSAGE_SIZE = 128 bytes`. Spec reflects source truth.

Buffer size: `24 + maxOrders×128 + maxOrders×16 + 4 + maxChildren×128`,
computed at construction — approximately 13.0 MB at defaults.
Restore validates: current `OMS_MAX_ORDERS` ≥ `snapshotMaxOrders` and
current `OMS_MAX_CHILDREN` ≥ `snapshotMaxChildren`. A capacity downsize
requires wiping the archive first (`OMS_ARCHIVE_DELETE_ON_START=true` for one
restart, then revert).

**NFR-013 (SHALL)** `OMS_ARCHIVE_DIR` MUST point to a durable filesystem that
survives host reboots. Default: `${user.home}/oms-archive-{nodeId}`.
`/tmp` MUST NOT be used in production. Both archive dir and cluster dir
(`archiveDir/cluster`) must be wiped together when a clean-start is needed.

**NFR-014 (SHALL)** `recomputeNextOrderId()` MUST scan both `OrderBook` and
`ChildOrderRegistry` after snapshot restore to find the maximum orderId and set
`nextOrderId = max + 1`. Both parent and child orders share the counter.

**NFR-015 (SHOULD)** `OrderBook` backing array is allocated on-heap
(`new byte[maxOrders × 128]`). This is promoted to old-gen immediately and
does not violate the zero-GC hot-path invariant but is inconsistent with
`ChildOrderRegistry` (off-heap). Deferred — see Section 9.2.

---

## Section 7 — Configuration Reference

### 7.1 OmsConfig Parameters (resolved by `oms-config` module)

All fields are validated at startup via `OmsConfig.load(ConfigSource)`. Any
invalid value throws `IllegalArgumentException` before Aeron infrastructure
starts. Each field is logged at INFO with its source name (`env`, `file`, or
`default`). When `OMS_CONFIG_FILE` is set, file values take priority over env
vars (`ChainedConfigSource`).

| Env Var | Default | Component | Effect | Constraint |
|---------|---------|-----------|--------|-----------|
| `OMS_MAX_ORDERS` | 65,536 | `OrderBook` | Pre-allocated off-heap slots for parent orders | ≥ 1; reducing below a snapshot's `snapshotMaxOrders` requires wiping the archive |
| `OMS_MAX_CHILDREN` | 32,768 | `ChildOrderRegistry` | Pre-allocated off-heap slots for child orders | ≥ 1; same wipe requirement as above |
| `OMS_INTENT_FRAGMENT_LIMIT` | 20 | `OmsClusteredService` | Max `ChildOrderIntent` fragments polled per Raft work cycle | ≥ 1 |
| `OMS_ALGO_FRAGMENT_LIMIT` | 10 | `AlgoSorAgent` | Max parent-order fragments polled per `doWork()` cycle | ≥ 1 |
| `OMS_MAX_VENUES` | 10 | `SmartOrderRouter`, `AlgoSorAgent` | Pre-allocated venue arrays size | ≥ 1; increase requires restart |
| `OMS_MAX_NOTIONAL` | 10,000,000 | `ValidationEngine` | Per-order notional ceiling in base currency units | ≥ 1; used in V-7 check |
| `OMS_SYMBOLS` | `AAPL,MSFT,GOOG,AMZN` | `ValidationEngine` | Comma-separated whitelist of permitted symbols | Must not be blank |
| `OMS_MAX_ICEBERG_ORDERS` | 4,096 | `IcebergAlgoEngine` | Max concurrently tracked iceberg orders | ≥ 1; exceeding causes hot-path map rehash |
| `OMS_MAX_TWAP_ORDERS` | 512 | `TwapAlgoEngine` | Max concurrently tracked TWAP orders | ≥ 1; exceeding causes silent single-slice fallback with WARN |
| `OMS_CONFIG_FILE` | _(unset)_ | Entry points | Path to `.properties` config file; file values override env vars | Optional |

### 7.2 Cluster and Archive Parameters (OmsNode — not in OmsConfig)

| Env Var | Default | Purpose |
|---------|---------|---------|
| `OMS_NODE_ID` | `0` | Integer node identifier (0/1/2 in a 3-node cluster) |
| `OMS_AERON_DIR` | `/dev/shm/oms-aeron-{nodeId}` | Aeron shared memory directory (IPC transport) |
| `OMS_ARCHIVE_DIR` | `${user.home}/oms-archive-{nodeId}` | Durable archive directory. Must NOT be `/tmp`. |
| `OMS_CLUSTER_MEMBERS` | Single-node default | Raft member list: `nodeId,client:port,member:port,log:port,transfer:port,archive:port` per node, pipe-separated for multi-node |
| `OMS_CLUSTER_INGRESS_CHANNEL` | `aeron:udp?endpoint=localhost:9000` | Cluster ingress UDP endpoint for `ConsensusModule` |
| `OMS_ARCHIVE_CONTROL_CHANNEL` | `aeron:udp?endpoint=localhost:8010` | Archive control channel |
| `OMS_ARCHIVE_LOCAL_CONTROL_CHANNEL` | `aeron:ipc` | Archive client request channel (must be `aeron:ipc` for embedded archive) |
| `OMS_ARCHIVE_LOCAL_RESPONSE_CHANNEL` | `aeron:ipc` | Archive client response channel (must be `aeron:ipc` for embedded archive) |
| `OMS_ARCHIVE_REPLICATION_CHANNEL` | `aeron:udp?endpoint=localhost:0` | Replication channel; multi-node must use a fixed per-node endpoint |
| `OMS_ARCHIVE_DELETE_ON_START` | `false` | `true` wipes archive + cluster dir on every restart. Use `true` in test harness only. |

### 7.3 Test Harness

| Env Var | Default | Purpose |
|---------|---------|---------|
| `HARNESS_CLUSTER_INGRESS` | `0=localhost:9000` | Cluster ingress endpoint in `nodeId=host:port` format (required by `AeronCluster.Context.ingressEndpoints`) |

---

## Section 8 — Hard Limits and Ceilings

> ⚠ Conflict C-3 (see Section 6.4): `design/failover.md` step 8 states
> "80 bytes" per parent record during restore. Source uses
> `OrderLayout.MESSAGE_SIZE = 128`. All values below reflect source truth.

| Limit | Value | Location | What Happens When Reached | Configurable? |
|-------|-------|----------|--------------------------|---------------|
| Parent order book capacity | 65,536 | `OmsConfig.DEFAULT_MAX_ORDERS` | `OrderBook.allocateSlot()` returns -1; new order is rejected | Yes — `OMS_MAX_ORDERS` |
| Child order registry capacity | 32,768 | `OmsConfig.DEFAULT_MAX_CHILDREN` | `ChildOrderRegistry.createChild()` returns null; intent rejected | Yes — `OMS_MAX_CHILDREN` |
| Max venues per SOR route | 10 | `OmsConfig.DEFAULT_MAX_VENUES` | Venue arrays pre-allocated at this size; extra venues silently ignored | Yes — `OMS_MAX_VENUES` |
| Max concurrent iceberg orders | 4,096 | `OmsConfig.DEFAULT_MAX_ICEBERG_ORDERS` | Map rehash on hot path (GC allocation) | Yes — `OMS_MAX_ICEBERG_ORDERS` |
| Max concurrent TWAP orders | 512 | `OmsConfig.DEFAULT_MAX_TWAP_ORDERS` | `acquireHandle()` returns -1; TWAP falls back to single-slice with WARN | Yes — `OMS_MAX_TWAP_ORDERS` |
| Max slices per parent | 256 | `IcebergAlgoEngine.MAX_SLICES_PER_PARENT` | WARN logged + slicing stops; parent stalls on remaining qty | No (hard; `sliceIndex` is a byte; clOrdId wraps at 256) |
| Intent fragment limit per cycle | 20 | `OmsConfig.DEFAULT_INTENT_FRAGMENT_LIMIT` | Remaining intents deferred to next cycle | Yes — `OMS_INTENT_FRAGMENT_LIMIT` |
| Algo fragment limit per cycle | 10 | `OmsConfig.DEFAULT_ALGO_FRAGMENT_LIMIT` | Remaining parent orders deferred to next `doWork()` cycle | Yes — `OMS_ALGO_FRAGMENT_LIMIT` |
| State machine dimensions | 16 × 10 | `OrderState.NUM_STATES`, `OrderState.NUM_EVENTS` | `ArrayIndexOutOfBoundsException` if state byte ≥ 16 | No (compile-time constant) |
| OrderLayout record size | 128 bytes | `OrderLayout.BLOCK_LENGTH` / `MESSAGE_SIZE` | Static assert fires as `AssertionError` at JVM startup if a field is added past offset 120 | No |
| Cluster ingress/egress frame | 129 bytes | `ClusterMessageType.IPC_MESSAGE_SIZE` | Fixed wire format | No |
| FIX Binary message size | 76 bytes | `FIXMessageDecoder.FIX_BINARY_SIZE` | Fixed wire format | No |
| ChildOrderIntent frame | 64 bytes | `HEADER_LENGTH (8)` + `BLOCK_LENGTH (56)` | Fixed wire format | No |
| Snapshot interval | 5 minutes | `OmsClusteredService.SNAPSHOT_INTERVAL_NS` | Longer interval increases RTO on failover | No (requires source change + explicit sign-off) |

---

## Section 9 — Out of Scope and Known Limitations

### 9.1 Not Implemented — Planned

**[PLANNED] Algo engine dispatch routing**
`AlgoSorAgent.onFragment()` always routes via `SmartOrderRouter` (basic SOR).
The dispatch path for `IcebergAlgoEngine` and `TwapAlgoEngine` is marked as
"future work" with example code commented out. Until wired, all parent orders
receive SOR treatment regardless of the `algoType` byte in the intent.
Source: `AlgoSorAgent.java`, `onFragment()` comment block.

**[PLANNED] Live venue market depth**
`AlgoSorAgent` calls `loadDefaultDepth()`, which pre-loads a single mock venue
with full order quantity. No real-time market data feed is connected.
Production requires a market data handler to update `venueIds`, `venuePrices`,
and `venueQtys` before each routing cycle.
Source: `AlgoSorAgent.java`, `loadDefaultDepth()`.

**[PLANNED] Horizontal sharding by symbol**
The current topology is a single Aeron Cluster shard for all symbols.
Symbol-partitioned sharding is documented in `design/decisions.md` §10.2–10.4
but not implemented. When added, `OmsLauncher.java`, `OmsNode.java`, and
`AeronTransport` will require changes. `OrderStateMachine`, `ValidationEngine`,
`ChildOrderRegistry`, and `ChildOrderIntentValidator` will not change.

**[PLANNED] Account-level cross-symbol notional limit**
`ValidationEngine` enforces per-order notional (`V-7`) but has no cross-symbol
account-level check. Noted as a cross-shard concern in `design/decisions.md`
§10.4.

### 9.2 Known Gaps — Deferred

**M1 — Single-node default cluster configuration (MEDIUM, DOCUMENTED)**
The default `OMS_CLUSTER_MEMBERS` value is single-node. A restart leaves no
quorum available during the restart window; there is no fault tolerance.
No code change required. Production MUST override `OMS_CLUSTER_MEMBERS` with a
3-node configuration.
Source: `design/resilience-review.md` M1; `OmsNode.java`.

**L1 — OrderBook backing array on-heap (LOW, DOCUMENTED)**
`OrderBook` allocates its backing store as `new byte[maxOrders × 128]` on the
Java heap. It is long-lived, promoted to old-gen immediately, and does not
violate the zero-GC hot-path invariant. However, it is inconsistent with
`ChildOrderRegistry` (off-heap `ByteBuffer.allocateDirect()`). The fix is
deferred as non-urgent.
Source: `design/resilience-review.md` L1; `OrderBook.java` constructor.
