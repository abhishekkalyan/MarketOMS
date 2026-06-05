# Resilience Review — Zero Data Loss, SPOF Elimination, Automatic Recovery
<!-- One-time structured review; update if findings change or new gaps are found. -->

## Executive Summary

MarketOMS uses Aeron Cluster (3-node Raft) for replication and has a
well-structured snapshot/restore path. Six gaps were found and fixed:
two CRITICAL (data loss on failover; volatile archive directory), four HIGH
(no periodic snapshots; algo-sor disconnect undetected; orderId collision after
restore; routing orders not re-published post-crash). All six are resolved in
the same commit as this document.

---

## Requirements

| ID | Requirement |
|----|-------------|
| R1 | Zero data loss under any single-node failure |
| R2 | No single point of failure that halts order processing |
| R3 | Automatic state recovery without human intervention |

---

## Findings

### CRITICAL

**C1 — ChildOrderRegistry not included in snapshot**
- **Component:** `SnapshotManager.takeSnapshot()`, `OmsClusteredService.onTakeSnapshot()`
- **Gap:** `SnapshotManager` serialized `OrderBook` and `ValidationEngine.seenClOrdIds` but
  never called `childRegistry.snapshot()`. After failover + snapshot restore, all in-flight
  child orders vanish from `ChildOrderRegistry`. Subsequent fills against those children are
  silently dropped. `ChildOrderRegistry` already had working `snapshot()` / `restore()` methods.
- **Fix:** `SnapshotManager.takeSnapshot()` now accepts `ChildOrderRegistry` and appends child
  records after the dedup section. `loadSnapshot()` restores them in `handleSnapshotFragment()`.
  `SNAPSHOT_VERSION` bumped to 2; `MAX_SNAPSHOT_BYTES` increased to ≈ 10.5 MB.
- **Status:** FIXED

**C2 — OMS_ARCHIVE_DIR defaults to /tmp**
- **Component:** `OmsNode.main()` line: `env("OMS_ARCHIVE_DIR", "/tmp/oms-archive-" + nodeId)`
- **Gap:** `/tmp` is ephemeral on Linux (cleared on reboot, tmpfs on many systems). A host reboot
  wipes the entire Raft log and all snapshots; the node restarts with no state, diverges from
  peers, and cannot join the cluster without a fresh peer sync.
- **Fix:** Default changed to `System.getProperty("user.home") + "/oms-archive-" + nodeId`,
  which survives reboots. Production deployments must override with a durable mount path.
- **Status:** FIXED

---

### HIGH

**H1 — No periodic snapshot interval configured**
- **Component:** `OmsNode` `ConsensusModule.Context`, `OmsClusteredService.onTimerEvent()`
- **Gap:** No periodic snapshots → Raft log grows unbounded → long replay on recovery.
  Aeron 1.47.0 `ConsensusModule.Context` has no `snapshotIntervalNs()` API.
- **Fix:** `OmsClusteredService` schedules a Raft timer at startup (`SNAPSHOT_TIMER_ID`
  every 5 minutes). In `onTimerEvent()` it calls `ClusterControl.ToggleState.SNAPSHOT.toggle()`
  via the cluster counters reader, which signals ConsensusModule to take a snapshot. Timer
  reschedules itself on each fire.
- **Status:** FIXED

**H2 — intentSub has no image availability handlers**
- **Component:** `OmsNode.main()`, `AeronTransport.createIpcSubscription()`
- **Gap:** `intentSub` (stream 12, algo-sor → oms-core) was created without
  `availableImageHandler` / `unavailableImageHandler`. An algo-sor crash or IPC disconnect
  is invisible to `OmsClusteredService`; orders accumulate in state NEW/ROUTING indefinitely
  with no operator alert.
- **Fix:** `intentSub` in `OmsNode` now created directly via `aeron.addSubscription()` with
  handlers that log INFO on connect and WARN on disconnect.
- **Status:** FIXED

**H3 — recomputeNextOrderId() ignores child registry**
- **Component:** `OmsClusteredService.recomputeNextOrderId()`
- **Gap:** Both parent and child orders share the `nextOrderId` counter (children are assigned
  `nextOrderId++` in `onChildOrderIntent()`). After snapshot restore, `recomputeNextOrderId()`
  only scanned `OrderBook`. If child orderIds exceeded parent orderIds, `nextOrderId` would be
  set too low and subsequent assignments would collide with existing child records.
- **Fix:** `recomputeNextOrderId()` now also calls `childRegistry.maxOrderId()` and takes the
  maximum of parent and child max orderIds.
- **Status:** FIXED

**H4 — ROUTING orders not re-published to algo-sor after restore**
- **Component:** `OmsClusteredService.onStart()`
- **Gap:** `IcebergAlgoEngine` and `TwapAlgoEngine` hold per-order state (`remainingQty`,
  `peakQty`, `handleActive[]`, `slicesFired[]`) in the algo-sor process. That state is lost on
  crash. After failover + snapshot restore, `OrderBook` contains orders in state NEW / ROUTING /
  PARTIALLY_FILLED, but algo-sor has no knowledge of them. Those orders are permanently stalled —
  no further ChildOrderIntents arrive. `ChildOrderIntentValidator` rule 7 (over-allocation guard)
  prevents double-slicing for qty already committed in the restored `ChildOrderRegistry`.
- **Fix:** `republishRoutingOrders()` added to `OmsClusteredService`, called from `onStart()`
  after snapshot restore. It re-publishes all NEW / ROUTING / PARTIALLY_FILLED parent orders to
  algo-sor on stream 30. algo-sor re-routes them; the over-allocation guard blocks any slice that
  would exceed `leavesQty - liveChildQty`.
- **Status:** FIXED

---

### MEDIUM

**M1 — OMS_CLUSTER_MEMBERS defaults to single node**
- **Component:** `OmsNode.main()`
- **Gap:** The single-node default provides no fault tolerance. Any restart causes a full
  leader election with only one candidate — no quorum available during restart window.
- **Recommendation:** Production deployments must set `OMS_CLUSTER_MEMBERS` to a 3-node
  configuration. No code change required — this is an operational configuration requirement.
- **Status:** DOCUMENTED (no code change — operational requirement)

---

### LOW

**L1 — OrderBook backing array is on-heap**
- **Component:** `OrderBook` constructor: `new byte[maxOrders * OrderLayout.MESSAGE_SIZE]`
- **Gap:** Minor GC pressure from an 8 MB on-heap array. Not on the hot path for GC (long-lived,
  promoted to old gen immediately); does not violate zero-GC invariants on the hot path.
- **Recommendation:** Replace with `ByteBuffer.allocateDirect()` wrapped in `UnsafeBuffer` for
  consistency with `ChildOrderRegistry`. Not urgent; deferred.
- **Status:** DOCUMENTED (deferred). OrderBook capacity is now configurable via OMS_MAX_ORDERS.

---

## Satisfied Invariants

| ID | Finding | Status |
|----|---------|--------|
| S1 | `ValidationEngine.seenClOrdIds` serialized in snapshot | SATISFIED |
| S2 | `OrderBook.reset()` + `restoreOrder()` correctly rebuild clOrdId/orderId indexes | SATISFIED |
| S3 | `ChildOrderIntentValidator` rule 7 prevents over-slicing on re-route after restore | SATISFIED |
| S4 | Raft log replay provides idempotency: replayed NOS messages rejected by dedup | SATISFIED |
| S5 | `OMS_ARCHIVE_DELETE_ON_START` defaults to `false` — snapshots survive planned restarts | SATISFIED |
| S6 | Archive dir and cluster dir are co-located — both wiped together by `deleteArchiveOnStart` | SATISFIED |

---

## Snapshot Format (version 3)

```
[0-3]   version (int) = 3
[4-7]   orderCount (int)
[8-11]  dedupCount (int)
[12-15] reserved (int) = 0
[16-19] snapshotMaxOrders (int)   — OMS_MAX_ORDERS at snapshot time
[20-23] snapshotMaxChildren (int) — OMS_MAX_CHILDREN at snapshot time
[24 .. 24+orderCount*80]          parent order records (MESSAGE_SIZE=80 bytes each)
[.. + dedupCount*16]              dedup entries (clOrdId:8 + orderId:8)
[.. + 4 + childCount*128]         child registry (self-describing: int count + records)
```

Buffer size is computed at runtime in `SnapshotManager(int maxOrders, int maxChildren)`:
`24 + maxOrders×80 + maxOrders×16 + 4 + maxChildren×128`
At defaults: ≈ 10.5 MB.

**Migration from version 2:** version 2 snapshots are rejected at load time (`IllegalStateException`).
Wipe the archive directory once: set `OMS_ARCHIVE_DELETE_ON_START=true` for one restart, then revert.

**Migration when changing OMS_MAX_ORDERS or OMS_MAX_CHILDREN:** restore validates that
current capacity ≥ snapshot capacity. Reducing capacity below a snapshot's values requires
wiping the archive first.

## Satisfied Invariants (added by configurable-capacities)

| ID | Finding | Status |
|----|---------|--------|
| S7 | `OrderBook` capacity driven by `OMS_MAX_ORDERS`; no-arg constructor uses `MAX_ORDERS` default | SATISFIED |
| S8 | `ChildOrderRegistry` capacity driven by `OMS_MAX_CHILDREN`; no-arg constructor uses `DEFAULT_CAPACITY` default | SATISFIED |
| S9 | `SnapshotManager` buffer allocated at runtime from `maxOrders + maxChildren`; no static `MAX_SNAPSHOT_BYTES` | SATISFIED |
| S10 | Snapshot version 3 header embeds `snapshotMaxOrders` + `snapshotMaxChildren`; restore rejects incompatible downsizes | SATISFIED |
| S11 | `ValidationEngine.seenClOrdIds` pre-sized to `maxOrders × 2` at load factor 0.65 — no rehash allocations on hot path | SATISFIED |

## Satisfied Invariants (added by scaling-audit-2026-06-05)

| ID | Finding | Status |
|----|---------|--------|
| S12 | `OrderBook.clOrdIdToSlot` and `orderIdToSlot` pre-sized to `maxOrders × 2` at 0.65f — no hot-path rehash in `allocateSlot()` / `index()` | SATISFIED |
| S13 | `ChildOrderRegistry.orderIdToSlot` and `clOrdIdToOrderId` pre-sized to `capacity × 2` at 0.65f — no hot-path rehash in `createChild()` | SATISFIED |
| S14 | `IcebergAlgoEngine.remainingQty` and `peakQty` pre-sized to `maxConcurrent × 2` at 0.65f — no hot-path rehash in `onSlice()`; wired via `OMS_MAX_ICEBERG_ORDERS` (default 4_096) | SATISFIED |
| S15 | `TwapAlgoEngine` all per-handle arrays sized at runtime from `OMS_MAX_TWAP_ORDERS` (default 512); capacity fallback logs WARN — no silent degradation | SATISFIED |
| S16 | `IcebergAlgoEngine.onSlice()` guards against sliceIndex byte wrap-around with `MAX_SLICES_PER_PARENT = 256` — clOrdId collisions in `ChildOrderRegistry` are prevented | SATISFIED |
