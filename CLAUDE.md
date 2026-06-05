# Mandatory Behaviour for Every Task

These rules apply to every task without exception.
A task is not complete until all four rules are satisfied.

---

## Rule 1 — Load Design Before Coding

At the start of every task:
1. Read `design/INDEX.md`
2. Identify your task type from the Task → Files table
3. Load only the listed files — do not load the full design/ folder
4. Work through `design/checklist.md` before touching any source file

Do not skip this for "small" changes. A one-line change to
`ChildOrderRegistry.createChild()` can violate the golden-source invariant.

---

## Rule 2 — Update Design After Every Change

After completing any task that modifies the system:
1. Consult the File Ownership table in `design/INDEX.md`
2. Update the file that owns the changed fact — directly, in-place
3. Never add the same fact to two design files
4. Run the post-change verification from `design/INDEX.md`
5. If any design file exceeds 250 lines after the update, report it

---

## Rule 3 — Enforce Principles on Every Change

Read `design/principles.md` before coding. For each principle listed there,
run its verification command against the changed files before reporting done.

---

## Rule 4 — Self-Check Before Reporting Done

- [ ] `./gradlew clean build` passes
- [ ] All applicable `design/principles.md` verification commands pass
- [ ] `design/INDEX.md` File Ownership table consulted and correct file updated
- [ ] No design file exceeds 250 lines
- [ ] No fact duplicated across two design files
- [ ] `design/INDEX.md` post-change verification commands pass

---

## What Goes in CLAUDE.md vs design/

These two locations have different ownership. Never add the same content
to both.

**Update `CLAUDE.md` when:**
- A behavioural rule changes (how Claude Code must act, what it must check)
- An operational constraint changes (JVM flags, env vars, cluster config)
- The definition of done changes
- A new principle is added that governs how the system must be built
  (e.g. "oms-harness must never import ChildOrderRegistry")

**Update the relevant `design/` file when:**
- A fact changes (a constant value, a field offset, a stream ID)
- A component contract changes (class name, thread model, invariant)
- A state or transition changes
- A data flow changes
- A design decision is made or reversed
- A new term enters the codebase

**The test:** if the content answers "how should I behave?" it belongs in
`CLAUDE.md`. If it answers "what is the system?" it belongs in `design/`.

When in doubt, put facts in `design/` and rules in `CLAUDE.md`. A fact
in `CLAUDE.md` creates a second source of truth that will drift.

**`CLAUDE.md` must be updated** whenever:
- A new Engineering Rule is added
- A new mandatory check is added to any Rule block
- The Aeron cluster configuration changes (env vars, port layout)
- The `--add-opens` JVM flags change
- The cluster ingress wire format changes
- A new self-check item is added to Rule 4

---

# Resilience Principles

These principles apply to every change that touches stateful components, snapshot paths,
or inter-process channels. Violating any one can cause silent data loss on failover.

**P1 — Snapshot completeness:** Every component that holds durable order state MUST be
serialized in `SnapshotManager.takeSnapshot()` and restored in `loadSnapshot()`. Currently:
`OrderBook`, `ValidationEngine.seenClOrdIds`, `ChildOrderRegistry`. Adding a new stateful
component requires updating both methods and `MAX_SNAPSHOT_BYTES`.

**P2 — Archive durability:** `OMS_ARCHIVE_DIR` must point to a durable filesystem that
survives host reboots. The default `${user.home}/oms-archive-{nodeId}` is the minimum;
production must use a dedicated persistent mount. Never use `/tmp`.

**P3 — Snapshot frequency:** `ConsensusModule.Context.snapshotIntervalNs` is set to
5 minutes. Do not remove or increase this without updating `failover.md` and getting
explicit sign-off — longer intervals increase RTO.

**P4 — Post-restore re-routing:** `OmsClusteredService.onStart()` MUST call
`republishRoutingOrders()` after every snapshot restore. This re-publishes all
NEW / ROUTING / PARTIALLY_FILLED orders to algo-sor, whose in-process state is lost on
crash. `ChildOrderIntentValidator` rule 7 prevents over-slicing.

**P5 — orderId counter correctness:** `recomputeNextOrderId()` MUST scan both
`OrderBook` and `ChildOrderRegistry` for the max orderId after restore. Both parent
and child orders share the `nextOrderId` counter.

**P6 — Channel image handlers:** Every Aeron subscription on a channel where a disconnect
constitutes a SPOF MUST have `availableImageHandler` and `unavailableImageHandler` attached.
Currently required: `intentSub` (stream 12, algo-sor → oms-core). Log WARN on disconnect.

---

# Engineering Rules
- Strictly Zero-GC in core packages.
- No object allocation, no boxing/unboxing, no standard Java collections on the hot path.
- Use Agrona primitive collections and DirectBuffers exclusively.
- Use fixed-point math (long) for prices.

---

# Module Structure

See `design/principles.md` — authoritative source for module boundaries and dependency rules.

Summary (do not use for precise values — use design/principles.md):
- `oms-codec` — binary contract, Agrona only, no Aeron Cluster
- `oms-core`  — golden source for all order state, Aeron Cluster
- `algo-sor`  — computation engine only, no dependency on `oms-core`
- `oms-launcher` — wiring only
- `oms-harness`  — black-box test client, no dependency on `oms-core`

Hard boundary: `algo-sor` must NOT depend on `oms-core`. Enforced by:
  `./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core`
  → must return no output on every build.

**No Spring, no CDI, no reflection.** All wiring is explicit constructor injection in `OmsLauncher`.

**Java 17.** `sealed` interfaces or `records` are permitted only where they introduce no hot-path allocation.

---

# Aeron IPC Channel Map

See `design/principles.md` — authoritative source for all stream IDs,
directions, publishers, subscribers, and payload sizes.

Do not add, remove, or renumber a stream without updating `design/principles.md`
Section 3.2 first and re-running the stream ID verification command.

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
6. Dispatches child NOS to FIX bridge via `FIXMessageEncoder`

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

See `design/state-model.md` — authoritative source for all state values and transitions.

---

# Binary Layout

See `design/wire-formats.md` — authoritative source for all field offsets.

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

`FIXMessageDecoder` is used **only** for the FIX bridge → oms-core IPC channel (stream 11 / FIX engine path), where the upstream FIX engine sends pre-parsed FIX Binary format. It must never be used to decode cluster ingress messages.

`OmsClusteredService.onSessionMessage` dispatches on `ClusterMessageType` constants and reads payload fields via `OrderLayout` offsets directly from the `DirectBuffer`. `FIXMessageDecoder` is used inside `handleExecReport` (for FIX bridge exec reports arriving via the IPC path).

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

## Skills

All reusable agent workflows live in `skills/workflows/` at the repo root.
When a task matches a description below, read the listed file in full
and follow the workflow it describes before doing any other work.

**Scaling and resilience audit**
When asked about scaling, capacity, hard-coded limits, throughput walls,
configurable constants, snapshot gaps, resilience, SPOF, data loss,
recovery, or failover — read the file
`skills/workflows/scaling-audit/SKILL.md` in full and follow the workflow
it describes from Step 0 through Phase 4.

**Functional specification generation**
When asked to generate a functional specification, functional requirements
document, or a spec derived from the codebase — read the file
`skills/workflows/spec-generator/SKILL.md` in full and follow the workflow
it describes from Step 0 through Step 4.

**Design doc sync**
When asked to sync design docs to source, update design docs from code,
fix stale design documentation, or bring design docs up to date — read
the file `skills/workflows/design-sync/SKILL.md` in full and follow
the workflow it describes from Step 0 through Step 7.


---
## Git Commit Policy

When making git commits, follow these rules without exception:

- Use conventional commit format: type(scope): description
- Do NOT include any of the following in commit messages or commit bodies:
    - "Co-authored-by: Claude"
    - "Generated by Claude"
    - "Authored by Claude"
    - "AI-generated"
    - Any reference to Claude, Anthropic, or AI tooling
- Do NOT add any trailers, footers, or co-author lines to any commit
- Commit message body (if used) must contain only technical context
  relevant to the change — no tooling attribution of any kind

Example of a correct commit:
feat(order-book): make MAX_ORDERS configurable via OMS_MAX_ORDERS

Example of an incorrect commit:
feat(order-book): make MAX_ORDERS configurable via OMS_MAX_ORDERS

Co-authored-by: Claude <claude@anthropic.com>