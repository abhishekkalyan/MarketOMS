# Failover and Recovery
<!-- Load when: touching SnapshotManager, OmsClusteredService.onTakeSnapshot,
     OmsClusteredService.onLoadSnapshot, or OrderBook.reset(). -->

## Section 9 — Failover and Recovery Sequence

**Leader failure sequence:**

1. Leader node fails. Remaining two nodes detect missed heartbeats.
2. Raft election: one follower becomes candidate, wins majority (2 of 2 remaining nodes).
3. New leader calls `OmsClusteredService.onNewLeadershipTermEvent()`.
4. New leader calls `OmsClusteredService.onStart()` with the most recent snapshot image.
5. `ParentOrderState.registerTransitions()` called in `onStart()` — **must happen before snapshot load**.
6. `SnapshotManager.loadSnapshot()` polls the snapshot image via `snapshotFragmentHandler`.
7. `OrderBook.reset()` clears all slots and indices.
8. For each serialized order record: `OrderBook.restoreOrder()` copies 128 bytes and rebuilds `clOrdIdToSlot` + `orderIdToSlot`.
9. For each dedup entry: `ValidationEngine.restoreEntry(clOrdId, orderId)` rebuilds `seenClOrdIds`.
10. Raft log replay begins from the snapshot position — committed messages after the snapshot replay through `onSessionMessage()`.
11. New leader begins accepting client sessions.

**Snapshot format** (from `SnapshotManager`):
- `[0]` version : int (4 bytes) = 1
- `[4]` orderCount : int (4 bytes)
- `[8]` dedupCount : int (4 bytes)
- `[12]` reserved : int (4 bytes)
- `[16..]` order records (128 bytes each)
- `[16 + orderCount×128..]` dedup entries (clOrdId:8 + orderId:8 = 16 bytes each)

**`ParentOrderState.registerTransitions()` requirement:**
Must be called BEFORE snapshot load because `onLoadSnapshot()` restores order states including `ROUTING (= 10)`. If `NUM_STATES = 16` is configured but index 10 has no transitions registered, any subsequent event against a ROUTING parent returns `INVALID_TRANSITION` and the fill is silently dropped.

**Both `OmsNode` and `OmsLauncher` register transitions:**
- `OmsClusteredService.onStart()` calls `ParentOrderState.registerTransitions()` before snapshot load
- `OmsLauncher.main()` calls `ParentOrderState.registerTransitions()` before `AgentRunner.startOnThread()`
