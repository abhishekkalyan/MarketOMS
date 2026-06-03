# Sequencing and Idempotency Model
<!-- Load when: touching ValidationEngine, ChildOrderIntentValidator,
     SequenceTracker, or any dedup/replay mechanism. -->

## Section 8 — Sequencing and Idempotency Model

### 8.1 Client → FIX layer

FIX MsgSeqNum (tag 34), per-session monotonic counter managed by the FIX connectivity engine. Gap detection and ResendRequest are the FIX layer's concern; by the time a message reaches `OmsClusteredService` it has been validated for sequence.

### 8.2 FIX layer → OmsClusteredService (Aeron Cluster ingress)

Aeron Cluster log position is a monotonically increasing `long`. After failover, the new leader replays the log. `ValidationEngine.seenClOrdIds` is a `Long2LongHashMap` (key=clOrdId, value=orderId, miss sentinel=`Long.MIN_VALUE`). Replayed orders are rejected by `ERR_DUPLICATE_CL_ORD_ID` before state mutation. The dedup map is serialized in `SnapshotManager.takeSnapshot()`.

### 8.3 Within OmsClusteredService (Raft commit total order)

`onSessionMessage()` fires only after Raft quorum commit. Three Raft replicas execute identical call sequences in identical order. `TRANSITION_TABLE` is a static array — same on every JVM. `ParentOrderState.registerTransitions()` extends it identically on every node at startup. Single-threaded: `OrderBook.flyweight` is shared across calls — correctness depends on the single-thread guarantee.

### 8.4 algo-sor → oms-core (ChildOrderIntent)

Intents carry no sequence number. Idempotency is provided by `ChildOrderIntentValidator.REJECT_QTY_OVERALLOCATION` (rule 7): `liveChildQty + sliceQty > parentLeavesQty` catches duplicate intents that would over-commit. `computeLiveChildQty()` walks the child linked list immediately before each intent is processed.

### 8.5 Venue → oms-core (execution reports)

Fill reports arrive on stream 11. A retransmitted fill for an already-terminal child returns null from `ChildOrderRegistry.applyFillAndAggregate()` (child not found in index) and is silently discarded. If applied twice before terminal: `OrderFlyweight.filledQty()` would exceed `qty()` and `leavesQty()` would go negative — clamped to 0 by `Math.max(0L, newLeavesQty)` in `applyFill()`.

### 8.6 Recovery — algo-sor state after failover

`IcebergAlgoEngine` and `TwapAlgoEngine` hold per-order state in-process (not snapshotted). After a leader failover, `OmsClusteredService.onStart()` calls `republishRoutingOrders()` to re-send all NEW / ROUTING / PARTIALLY_FILLED parent orders to algo-sor on stream 30. algo-sor re-routes each order; `ChildOrderIntentValidator` rule 7 (over-allocation guard) blocks any slice that would exceed `parent.leavesQty - liveChildQty`, preventing double-slicing for qty already committed in the restored `ChildOrderRegistry`.
