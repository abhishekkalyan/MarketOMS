# Mandatory Behaviour for Every Task

These rules apply to every task without exception.
A task is not complete until all four rules are satisfied.

---

## Rule 1 — Check Design Before Coding

Before writing, editing, or deleting any Java source file:

1. Read `DESIGN.md` Section 2 (Core Design Principles)
2. Read the Component Contract in Section 4 for every class you intend to touch
3. Work through the Design Analysis Checklist (Section 12) question by question
4. If any checklist answer is NO, stop and explain the conflict before proceeding

Do not skip this even for "small" changes. A one-line change to
`ChildOrderRegistry.createChild()` can violate the golden-source invariant.
A new import in `algo-sor` can silently break the module boundary.

---

## Rule 2 — Update DESIGN.md After Every Change

After completing any task that changes the system, update `DESIGN.md` before
reporting done. This is part of the definition of done, not a follow-up task.

### What triggers a DESIGN.md update

| Trigger | DESIGN.md section(s) to update |
|---------|--------------------------------|
| New module added or removed | 1, 3.1, 3.3 |
| Module dependency added, removed, or changed | 3.1, 3.2 |
| Aeron IPC stream added, removed, or direction changed | 3.2 |
| New message type in `ClusterMessageType` | 3.2, 6, Glossary |
| Class moved between modules | 3.1, 3.3, 4 |
| New component introduced | 3.3, 4, 11, Glossary |
| `OrderLayout` field added, moved, or resized | 7.1 |
| `ChildOrderIntentFlyweight` field changed | 7.2 |
| State byte constant added or changed | 6.1, 6.2, 6.3, Glossary |
| Transition added or removed | 6.2, 6.3 |
| `registerTransitions()` call changed | 6.2, 9 |
| Snapshot format changed | 9 |
| New environment variable in `OmsNode` | 9 footnote |
| Build steps changed | 3.1 boundary check commands |
| New design decision made | 11 |

### How to update

1. Work through the trigger table above for everything changed in this task
2. Edit the relevant DESIGN.md section directly — update tables, diagrams,
   and constant values in-place
3. Do not add a changelog or "what changed" paragraph — DESIGN.md describes
   current state only; git history is the changelog
4. After updating, re-run all DESIGN.md Step 3 verification commands:
   - All 13 section grep checks still pass
   - Byte constants still match source (`grep "public static final byte" ...`)
   - Stream IDs still match source
   - `wc -l DESIGN.md` is still within 600–1200 lines
   - No placeholder text found

---

## Rule 3 — Enforce Principles on Every Change

The principles in `DESIGN.md` Section 2 are invariants, not guidelines.
Before committing any change, verify each applicable principle:

| Principle | Verification command |
|-----------|---------------------|
| Zero-GC on hot path | `grep -n " new " [changed file] \| grep -v "//\|test\|Test"` — no hits in hot-path methods |
| Single-threaded execution | `grep -n "synchronized\|AtomicReference\|ReentrantLock" [changed file]` — no hits |
| `oms-core` golden source | `grep -rn "ChildOrderRegistry\|OrderBook" algo-sor/src/` — no hits |
| `algo-sor` intent-only | `grep -rn "FIXMessageEncoder" algo-sor/src/` — no hits |
| `ChildOrderIntentValidator` before `createChild()` | Read `OmsClusteredService.onChildOrderIntent()` — validator called first |
| Module boundaries | `./gradlew :algo-sor:dependencies --configuration compileClasspath \| grep oms-core` — no output |
| Fixed-point prices | `grep -n "double\|float\|BigDecimal" [changed file]` — no hits |
| Raft-replicated mutations | All `OrderBook`/`ChildOrderRegistry` writes inside `onSessionMessage()` or `onLoadSnapshot()` |

---

## Rule 4 — Self-Check Before Reporting Done

Run this checklist internally before every task completion report:

- [ ] `./gradlew clean build` passes with zero compilation errors
- [ ] `:algo-sor:dependencies | grep oms-core` returns no output
- [ ] `:oms-codec:dependencies | grep aeron-cluster` returns no output
- [ ] No `new` expression added to any hot-path method
      (`onSessionMessage`, `onFragment`, `validateNewOrder`, `validate`, `createChild`)
- [ ] `DESIGN.md` updated for every trigger in Rule 2 that applies to this task
- [ ] All DESIGN.md verification commands pass after the update

---

# Engineering Rules
- Strictly Zero-GC in core packages.
- No object allocation, no boxing/unboxing, no standard Java collections on the hot path.
- Use Agrona primitive collections and DirectBuffers exclusively.
- Use fixed-point math (long) for prices.

---

# Module Structure

See `DESIGN.md` Section 3 for the authoritative module dependency graph,
compile-time boundary rules, and runtime Aeron IPC channel map.

Summary (do not use for precise values — use DESIGN.md Section 3):
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

See `DESIGN.md` Section 3.2 — authoritative source for all stream IDs,
directions, publishers, subscribers, and payload sizes.

Do not add, remove, or renumber a stream without updating DESIGN.md Section 3.2
first and re-running the stream ID verification command (DESIGN.md Step 3, Check 3).

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

See `DESIGN.md` Section 6 — authoritative source for all state byte values,
the complete legal transition table, and state invariants.

Key operational facts (do not use for precise values — use DESIGN.md Section 6):
- Base states: `PENDING_NEW(0)` through `EXPIRED(9)` in `com.cobain.oms.model.OrderState`
- Parent-only extension: `ROUTING(10)` in `com.cobain.oms.core.ParentOrderState`
- `NUM_STATES = 16` in `OrderState`
- `ParentOrderState.registerTransitions()` must be called in both `OmsLauncher.main()`
  and `OmsClusteredService.onStart()` before any order processing begins —
  without this, ROUTING transitions return `INVALID_TRANSITION`
- Terminal check: `OrderState.isTerminal(state)` = `state >= FILLED` (>= 6)

---

# Binary Layout

See `DESIGN.md` Section 7 — authoritative source for:
- Complete `OrderLayout` 128-byte field table with all offsets and types
- Complete `ChildOrderIntentFlyweight` 56-byte field table
- Price encoding (`PRICE_MULTIPLIER = 10_000L`) with worked example
- Symbol encoding algorithm (`OrderFlyweight.encodeSymbol()`)
- FIX Binary inbound format (76-byte `FIXMessageDecoder` layout)

Key constraint: `OrderLayout.BLOCK_LENGTH = 128` is fixed. Never increase it.
The static assert `OFFSET_PARENT_OR_FIRST_CHILD_ID + Long.BYTES == BLOCK_LENGTH`
enforces this at class-load time.

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
