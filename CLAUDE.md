## Mandatory: Keep This File Current

After completing **any** task that changes the system design, architecture, or
module structure, you must update this file before closing the task. This is
not optional and is not a separate follow-up task — it is part of the definition
of done for every change.

### What triggers an update

Update this file when any of the following occur:

- A new module is added or an existing module is removed
- A dependency between modules is added, removed, or redirected
- An Aeron IPC stream is added, removed, or changes direction
- A new message type is added to the wire protocol
- A class is moved between modules
- A new component is introduced (e.g. a new Agent, a new Registry, a new Validator)
- A state machine state or transition is added or modified
- The binary layout of any flyweight changes (new field, offset change, size change)
- The build steps change (new Gradle task, new boundary check, new JVM arg)
- A constraint is relaxed or tightened (e.g. a new zero-allocation rule, a new
  module boundary enforcement check)

### What to update

When a trigger above applies, update the relevant section(s) of this file:

- **Module map** — reflect any added, removed, or renamed modules
- **Component interaction diagram** — reflect any new or changed IPC channels,
  message types, or component relationships
- **Aeron IPC channel map** — update stream IDs, directions, and content descriptions
- **Order state model diagram** — update if states or transitions changed
- **Binary layout table** — update if any flyweight field was added, moved, or resized
- **Build steps** — update if the build sequence, boundary checks, or JVM args changed
- **Common build failures table** — add any new failure mode encountered during the task

### Format rules

- Keep diagrams as ASCII — no external image references
- Keep the IPC channel map as a Markdown table
- Keep the binary layout as a Markdown table with Offset, Size, Type, Field, and
  Semantics columns
- Do not summarise changes in prose where a table or diagram is already present —
  update the artefact directly

### How to do it

At the end of every task, before reporting completion, run this self-check:

1. Read through the list of triggers above
2. For each trigger that applies to the work just completed, identify the
   corresponding section in this file
3. Update that section to reflect the current state of the system
4. If a trigger applies but no corresponding section exists, add a new section

Do not add a change log or "last updated" timestamp — keep the file describing
the current state only, not the history of how it got there. Git history
serves that purpose.

# Engineering Rules
- Strictly Zero-GC in core packages.
- No object allocation, no boxing/unboxing, no standard Java collections on the hot path.
- Use Agrona primitive collections and DirectBuffers exclusively.
- Use fixed-point math (long) for prices.

---

# Module Structure

Gradle multi-module monorepo. Four bounded modules + launcher:

```
market-oms/
  oms-codec/      codec/, fix/         — shared binary contract, Agrona only (no Aeron)
  oms-core/       statemachine/, validation/, cluster/, common/
  algo-sor/       algo/, algoagent/
  oms-launcher/   launcher/
```

**Hard boundary:** `algo-sor` must NOT depend on `oms-core`. The only shared module is `oms-codec`. All inter-module communication is via Aeron IPC at runtime.

**No Spring, no CDI, no reflection.** All wiring is explicit constructor injection in `OmsLauncher`.

**Java 21 only.** `sealed` interfaces or `records` are permitted only where they introduce no hot-path allocation.

---

# Aeron IPC Channel Map

| Stream ID | Direction              | Purpose                                      |
|-----------|------------------------|----------------------------------------------|
| 10        | oms-core → algo-sor    | Accepted parent orders                       |
| 12        | algo-sor → oms-core    | `ChildOrderIntent` routing instructions      |

Stream 11 (direct child NOS from algo-sor to FIX bridge) is **removed**. `algo-sor` never writes to the FIX bridge.

---

# Golden-Source State Architecture

`oms-core` is the SINGLE golden source of all order state — parent and child.

**`algo-sor` is a pure computation engine.** It receives parent orders, runs routing/slicing arithmetic, and publishes `ChildOrderIntent` messages back to `oms-core`. It creates no order state, touches no order book, and dispatches nothing to the FIX bridge. It is stateless between work cycles.

**`oms-core` owns child order lifecycle:**
1. Receives `ChildOrderIntent` from algo-sor over stream 12
2. Validates intent via `ChildOrderIntentValidator` (7 rules — must PASS before any child is created)
3. Creates child in `ChildOrderRegistry` (the only place child order records are born)
4. Links child → parent in `OrderBook`
5. Transitions parent state via `OrderStateMachine`
6. Dispatches child NOS to FIX bridge via `AeronEgressPublisher`

All Aeron Cluster replication, snapshotting, and recovery operates only on `oms-core` state.

```
oms-core ──[parent order]──► algo-sor ──[ChildOrderIntent]──► oms-core
                                                                   │
                                                       validates intent
                                                       creates child in ChildOrderRegistry
                                                       links child → parent in OrderBook
                                                       transitions parent → ROUTING
                                                           │
                                                           └──[child NOS]──► FIX bridge ──► Venue
```

---

# Order State Model

Base states (`OrderState.java`, byte constants 0–7): `NEW`, `PENDING_NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `PENDING_CANCEL`, `REPLACED`, `REJECTED`.

Parent-only extension (`ParentOrderState.java`, values 10+):
- `ROUTING = 10` — parent accepted, one or more children dispatched, awaiting fills.

`OrderState.NUM_STATES = 16` (was 8) to accommodate parent-only states at index 10. `ParentOrderState.registerTransitions()` must be called in `OmsLauncher.main()` before any `AgentRunner` is started.

ROUTING lifecycle:
- `NEW → ROUTING` — first `ChildOrderIntent` accepted
- `ROUTING → PARTIALLY_FILLED` — first child fill aggregated
- `ROUTING → CANCELED` — all children canceled before any fill
- `ROUTING → FILLED` — all qty filled via children

---

# Binary Layout — `OrderFields.java`

Block length: **128 bytes** (fixed, never increase).

Reserved region (offsets 112–127) is consumed by parent-child linkage fields:

| Offset | Size | Type | Constant                          | Semantics                                                      |
|--------|------|------|-----------------------------------|----------------------------------------------------------------|
| 112    | 4    | int  | `OFFSET_CHILD_COUNT`              | Parent: number of live children. 0 on child orders.           |
| 116    | 4    | int  | `OFFSET_NEXT_SIBLING_SLOT`        | Child: slot index of next sibling. 0 = end of list.           |
| 120    | 8    | long | `OFFSET_PARENT_OR_FIRST_CHILD_ID` | Parent: first child orderId. Child: parent orderId.           |

Static assert: `OFFSET_PARENT_OR_FIRST_CHILD_ID + Long.BYTES == BLOCK_LENGTH`.

---

# Binary Layout — `ChildOrderIntentFlyweight.java`

56 bytes total (fits in one cache line). Package: `com.sellside.oms.codec`.

| Offset | Size | Type  | Field                 |
|--------|------|-------|-----------------------|
| 0      | 8    | long  | parentOrderId         |
| 8      | 8    | long  | parentClOrdId         |
| 16     | 8    | long  | sliceQty              |
| 24     | 8    | long  | limitPrice (fixed-pt) |
| 32     | 4    | int   | venueId               |
| 36     | 1    | byte  | algoType (1=SOR, 2=ICEBERG, 3=TWAP) |
| 37     | 1    | byte  | sliceIndex            |
| 38     | 2    | short | _pad                  |
| 40     | 8    | long  | intentTimestampNanos  |
| 48     | 8    | long  | _reserved             |

---

# Child Order ID Convention

```
childClOrdId = parentOrderId * 10_000L + (sliceIndex & 0xFF)
```

Self-describing (divide by 10_000 to recover parentOrderId), globally unique, FIX-safe.

---

# `ChildOrderRegistry` Design

Off-heap store (`UnsafeBuffer`) for all child orders. Two pre-allocated flyweights (`sharedFlyweight`, `secondFlyweight`) to avoid clobbering when fill + parent lookup occur in the same work cycle. Indexes: `Long2LongHashMap` for orderId→slot and clOrdId→orderId. Free-slot stack for O(1) alloc/free.

Children are linked to parents via a singly-linked list using `OFFSET_PARENT_OR_FIRST_CHILD_ID` (list head on parent) and `OFFSET_NEXT_SIBLING_SLOT` (next pointer on child).

Snapshotted and restored with `oms-core`'s Aeron Cluster snapshot.

---

# `ChildOrderIntentValidator` Rules

All 7 rules must pass (result `PASS = 0`) before `createChild()` is called:

1. Parent exists in `OrderBook`
2. Parent state is routable: `NEW`, `ROUTING`, or `PARTIALLY_FILLED`
3. `sliceQty > 0`
4. `sliceQty ≤ parent.leavesQty`
5. `limitPrice > 0`
6. `venueId > 0`
7. `liveChildQty + sliceQty ≤ parent.leavesQty` (over-allocation guard)

---

# MediaDriver Configuration (low-latency)

```java
new MediaDriver.Context()
    .threadingMode(ThreadingMode.DEDICATED)
    .conductorIdleStrategy(new BusySpinIdleStrategy())
    .senderIdleStrategy(new BusySpinIdleStrategy())
    .receiverIdleStrategy(new BusySpinIdleStrategy())
    .dirDeleteOnStart(true)
    .dirDeleteOnShutdown(true)
```

`AgentRunner` uses `BusySpinIdleStrategy`. Back-pressure in `publishIntent()` spins with `Thread.onSpinWait()` — never `Thread.sleep()`.

---

# Aeron Cluster Node Configuration

All channels are env-var driven — nothing hardcoded. Every field below is **required**; omitting any one throws a `ConfigurationException` at launch.

## Archive channels (`OmsNode.java` → `Archive.Context` + `AeronArchive.Context`)

| Env Var | Field | Local default | Multi-node |
|---------|-------|---------------|------------|
| `OMS_ARCHIVE_CONTROL_CHANNEL` | `Archive.controlChannel` | `aeron:udp?endpoint=localhost:8010` | Same port, per-node host/IP |
| `OMS_ARCHIVE_LOCAL_CONTROL_CHANNEL` | `AeronArchive.controlRequestChannel` (ConsensusModule + ClusteredServiceContainer) | `aeron:ipc` | `aeron:ipc` (embedded) or UDP (external archive) |
| `OMS_ARCHIVE_LOCAL_RESPONSE_CHANNEL` | `AeronArchive.controlResponseChannel` | `aeron:ipc` | `aeron:ipc` (embedded) |
| `OMS_ARCHIVE_REPLICATION_CHANNEL` | `Archive.replicationChannel` + `ConsensusModule.replicationChannel` | `aeron:udp?endpoint=localhost:0` | `aeron:udp?endpoint=<this-node-ip>:8020` (fixed, per node) |

**Embedded-vs-external archive rule:** When the archive runs inside the same process (via `ClusteredMediaDriver`), all `AeronArchive.Context` clients **must** use `aeron:ipc` for both `controlRequestChannel` and `controlResponseChannel`. Using UDP for an embedded archive throws `ERROR - local archive control must be IPC`.

**`replicationChannel`** must be set on both `Archive.Context` and `ConsensusModule.Context`. Port `0` is fine for single-node local dev; for multi-node it must be a fixed per-node endpoint peers can reach.

## Consensus + Ingress channels

| Env Var | Field | Local default | Multi-node |
|---------|-------|---------------|------------|
| `OMS_CLUSTER_INGRESS_CHANNEL` | `ConsensusModule.ingressChannel` | `aeron:udp?endpoint=localhost:9000` | Per-node host/IP, same port |
| `OMS_CLUSTER_MEMBERS` | `ConsensusModule.clusterMembers` | see below | Per-node entries |

**`clusterMembers` format** (Aeron 1.40+, comma-separated endpoint pairs per node):
```
<nodeId>,<clientHost:port>,<memberHost:port>,<logHost:port>,<transferHost:port>,<archiveControlHost:port>
```
Single-node default: `0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010`
Multi-node: pipe-separate (`|`) entries, one per node, with actual host IPs.

## Archive state and clean-start

| Env Var | Default | Purpose |
|---------|---------|---------|
| `OMS_ARCHIVE_DELETE_ON_START` | `false` | `true` = wipe archive + cluster dir on every restart. Use `true` in the test harness; `false` in production (preserves snapshots). |

**State persistence:** Aeron Cluster stores two separate state trees:
- **Archive dir** (`OMS_ARCHIVE_DIR`) — Raft log recordings (`.rec` files), archive catalog. Wiped by `Archive.Context.deleteArchiveOnStart(true)`.
- **Cluster dir** (`$OMS_ARCHIVE_DIR/cluster`) — `node-state.dat`, `recording.log`, `cluster-mark*.dat`. Wiped by `ConsensusModule.Context.deleteDirOnStart(true)`. **Both must be wiped together.** Deleting only the archive leaves the cluster dir intact; the ConsensusModule replays the Raft log from `recording.log`, restoring all prior `onSessionMessage` calls (including `ValidationEngine.seenClOrdIds`) even with no snapshot.

`ClusteredServiceContainer.Context` does **not** have `deleteDirOnStart` — it shares the same `clusterDir` as `ConsensusModule`, so deleting it from `ConsensusModule` is sufficient.

## Harness client ingress format

`AeronCluster.Context.ingressEndpoints` requires `nodeId=host:port` format (e.g. `0=localhost:9000`), not bare `host:port`. Env var: `HARNESS_CLUSTER_INGRESS`, default `0=localhost:9000`.

---

# Cluster Ingress Wire Format

**All messages between cluster clients and `OmsClusteredService` use `ClusterMessageType` framing, not FIX Binary.**

```
[0]      msgType  : byte         — ClusterMessageType constant (1=NEW_ORDER, 2=CANCEL_ORDER, 3=REPLACE_ORDER)
[1..128] payload  : OrderLayout  — 128-byte order record (OrderLayout field offsets)
```
Total: 129 bytes (`ClusterMessageType.IPC_MESSAGE_SIZE`).

`FIXMessageDecoder` is used **only** for the FIX bridge → oms-core IPC channel (stream 10 / FIX engine path), where the upstream FIX engine sends pre-parsed FIX Binary format. It must never be used to decode cluster ingress messages.

`OmsClusteredService.onSessionMessage` dispatches on `ClusterMessageType` constants and reads payload fields via `OrderLayout` offsets directly from the `DirectBuffer`. `FIXMessageDecoder` is still used inside `handleExecReport` (for FIX bridge exec reports arriving via the legacy IPC path).

**Egress responses** use the same framing. `sendEgressExecReport` writes `ClusterMessageType` + `OrderLayout` into a pre-allocated `egressBuffer` and delivers it via `ClientSession.offer()` using the cluster's `idleStrategy`.

---

# `--add-opens` JVM Flags

All modules that use Agrona (including `oms-harness`) must include all three flags in `applicationDefaultJvmArgs`:

```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
```

Omitting `jdk.internal.misc` causes `IllegalAccessError` from `UnsafeApi` at startup even if the other two are present. Set in each module's `build.gradle` under `application { applicationDefaultJvmArgs = [...] }`.

---

