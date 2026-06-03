# Failover and Recovery
<!-- Load when: touching SnapshotManager, OmsClusteredService.onTakeSnapshot,
     OmsClusteredService.onStart, or OrderBook.reset(). -->

## Section 9 — Failover and Recovery Sequence

**Leader failure sequence:**

1. Leader node fails. Remaining two nodes detect missed heartbeats.
2. Raft election: one follower becomes candidate, wins majority (2 of 2 remaining nodes).
3. New leader calls `OmsClusteredService.onNewLeadershipTermEvent()`.
4. New leader calls `OmsClusteredService.onStart()` with the most recent snapshot image.
5. `ParentOrderState.registerTransitions()` called in `onStart()` — **must happen before snapshot load**.
6. `SnapshotManager.loadSnapshot()` polls the snapshot image via `snapshotFragmentHandler`.
7. `OrderBook.reset()` clears all slots and indices.
8. For each serialized parent order record: `OrderBook.restoreOrder()` copies 80 bytes and rebuilds `clOrdIdToSlot` + `orderIdToSlot`.
9. For each dedup entry: `ValidationEngine.restoreEntry(clOrdId, orderId)` rebuilds `seenClOrdIds`.
10. `ChildOrderRegistry.restore()` reads int childCount + childCount × 128-byte records; rebuilds `orderIdToSlot` + `clOrdIdToOrderId`.
11. `recomputeNextOrderId()` scans both `OrderBook` and `ChildOrderRegistry` for max orderId; sets `nextOrderId = max + 1`.
12. `republishRoutingOrders()` re-publishes all NEW / ROUTING / PARTIALLY_FILLED parent orders to algo-sor (stream 30). `ChildOrderIntentValidator` rule 7 prevents over-slicing.
13. Raft log replay begins from the snapshot position — committed messages after the snapshot replay through `onSessionMessage()`.
14. New leader begins accepting client sessions.

**Snapshot format** (version 3, from `SnapshotManager`):
- `[0-3]`   version : int = 3
- `[4-7]`   orderCount : int
- `[8-11]`  dedupCount : int
- `[12-15]` reserved : int = 0
- `[16-19]` snapshotMaxOrders : int — `OMS_MAX_ORDERS` value at snapshot time
- `[20-23]` snapshotMaxChildren : int — `OMS_MAX_CHILDREN` value at snapshot time
- `[24 .. 24+orderCount×80]` parent order records (MESSAGE_SIZE = 80 bytes each)
- `[.. + dedupCount×16]` dedup entries (clOrdId:8 + orderId:8 = 16 bytes each)
- `[.. + 4 + childCount×128]` child registry (self-describing: int count + BLOCK_LENGTH records)

Buffer size = `24 + maxOrders×80 + maxOrders×16 + 4 + maxChildren×128` (computed at construction, ≈ 10.5 MB at defaults).
Restore validates `current OMS_MAX_ORDERS ≥ snapshotMaxOrders` and `current OMS_MAX_CHILDREN ≥ snapshotMaxChildren`.

**Snapshot frequency:** Driven by a Raft timer in `OmsClusteredService` (`SNAPSHOT_TIMER_ID`, `SNAPSHOT_INTERVAL_NS = 5 minutes`). On each timer fire, `ClusterControl.ToggleState.SNAPSHOT.toggle()` is called, signalling ConsensusModule to take a snapshot. Limits log replay window to at most 5 minutes on recovery. Note: Aeron 1.47.0 `ConsensusModule.Context` has no `snapshotIntervalNs` API; the timer approach achieves equivalent behaviour.

**`ParentOrderState.registerTransitions()` requirement:**
Must be called BEFORE snapshot load because `onStart()` restores order states including `ROUTING (= 10)`. If `NUM_STATES = 16` is configured but index 10 has no transitions registered, any subsequent event against a ROUTING parent returns `INVALID_TRANSITION` and the fill is silently dropped.

**Both `OmsNode` and `OmsLauncher` register transitions:**
- `OmsClusteredService.onStart()` calls `ParentOrderState.registerTransitions()` before snapshot load
- `OmsLauncher.main()` calls `ParentOrderState.registerTransitions()` before `AgentRunner.startOnThread()`

**Archive durability requirement:**
`OMS_ARCHIVE_DIR` must point to a durable filesystem (not `/tmp`). Default: `${user.home}/oms-archive-{nodeId}`. Production must override with a persistent mount. Both archive dir and cluster dir (`archiveDir/cluster`) must be wiped together — deleting only one leaves the other with inconsistent Raft state.

**Version migration:** version 1 snapshots are rejected at load. On upgrade: set `OMS_ARCHIVE_DELETE_ON_START=true` for one restart, then revert to `false`.
