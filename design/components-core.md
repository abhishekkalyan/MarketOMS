# Component Contracts — oms-core + algo-sor
<!-- Load when: touching any oms-core or algo-sor class listed below, or
     introducing a new stateful or computation component. Update the entry
     after any contract change. -->

## oms-core Components

Components in this section own the stateful, Raft-replicated runtime of MarketOMS.

---

#### AeronTransport

**Class:** `com.cobain.oms.transport.AeronTransport`
**Module:** `oms-core`
**Thread model:** static factory class — no instances, no threads
**Zero-allocation contract:** Factory methods; the objects they create (`ExclusivePublication`, `Subscription`) are Aeron infrastructure objects allocated at startup
**Inputs:** `aeronDirectory` string, stream ID constants
**Outputs:** `ExclusivePublication` (IPC or UDP) and `Subscription` instances for wiring
**Invariants:** `STREAM_OMS_TO_ALGO = 30`, `STREAM_CHILD_INTENTS = 12`, `STREAM_OMS_TO_FIX = 10`, `STREAM_FIX_TO_OMS = 11`; `IPC_CHANNEL = "aeron:ipc"`
**Failure mode:** Mismatched stream IDs between publisher and subscriber cause the Aeron subscription to never receive messages (silent starvation, not an exception)

---

#### ParentOrderState

**Class:** `com.cobain.oms.core.ParentOrderState`
**Module:** `oms-core`
**Thread model:** static class; `registerTransitions()` must be called on startup thread before any `AgentRunner` starts
**Zero-allocation contract:** `ROUTING = 10` is a `public static final byte`; `registerTransitions()` mutates the existing static `TRANSITION_TABLE` array — zero allocation
**Inputs:** `registerTransitions()` call from `OmsLauncher.main()` and `OmsClusteredService.onStart()`
**Outputs:** registers 4 transitions into `OrderStateMachine.TRANSITION_TABLE[10][*]`
**Invariants:** Must be called before any order with state `ROUTING` is processed; `ROUTING = 10` is within `NUM_STATES = 16`
**Failure mode:** If not called before `AgentRunner` starts, any fill against a ROUTING parent returns `INVALID_TRANSITION` and the fill is silently dropped

---

#### OrderStateMachine

**Class:** `com.cobain.oms.core.OrderStateMachine`
**Module:** `oms-core`
**Thread model:** static utility; `TRANSITION_TABLE` is a static `byte[16][10]` array; all operations are read-only except `registerTransitions()` at startup
**Zero-allocation contract:** `transition()` is a single array read; `applyTransition()` adds one byte write to the flyweight; `transitionToRouting()` adds one byte write and one `System.nanoTime()` call
**Inputs:** current state byte + event byte → `TRANSITION_TABLE[state & 0xFF][event & 0xFF]`
**Outputs:** next-state byte or `INVALID_TRANSITION (-1)`
**Invariants:** Terminal states (FILLED, CANCELED, REJECTED, EXPIRED) have no outbound transitions; all `TRANSITION_TABLE` cells are initialized to `INVALID_TRANSITION` in the static block; `ParentOrderState.registerTransitions()` must be called before ROUTING transitions are used
**Failure mode:** Calling `transition()` with an out-of-range state (≥ 16) throws `ArrayIndexOutOfBoundsException`; a state mutation outside `onSessionMessage()` diverges Raft replicas

---

#### ValidationEngine

**Class:** `com.cobain.oms.core.ValidationEngine`
**Module:** `oms-core`
**Thread model:** single-threaded (Raft commit thread); all state owned by `OmsClusteredService`
**Zero-allocation contract:** All checks are primitive comparisons or `Long2LongHashMap.get()` / `LongHashSet.contains()` calls; result codes are `int` constants; no exceptions thrown
**Inputs:** `validateNewOrder(OrderFlyweight)` on the hot path; `registerAccepted(long, long)` after every accepted order
**Outputs:** `int` result code (`VALID = 0`, or `ERR_*` constants 1–8)
**Invariants:** `seenClOrdIds.get(clOrdId) != DEDUP_MISSING` is the dedup check; `DEDUP_MISSING = Long.MIN_VALUE`; validation order is cheapest-first (duplicate → side → TIF → qty → price → symbol → notional)
**Failure mode:** `reset()` clears `seenClOrdIds`; must be followed by `restoreEntry()` calls for each snapshot entry, or replayed orders pass the dedup check and create duplicates

---

#### ChildOrderIntentValidator

**Class:** `com.cobain.oms.core.ChildOrderIntentValidator`
**Module:** `oms-core`
**Thread model:** single-threaded (Raft commit thread)
**Zero-allocation contract:** Returns a `byte` result code; all 7 checks are primitive comparisons; never throws
**Inputs:** `validate(ChildOrderIntentFlyweight, OrderFlyweight, long liveChildQty)`
**Outputs:** `PASS = 0` or `REJECT_* (20–26)`
**Invariants:** Rule 7 (over-allocation guard) requires `computeLiveChildQty()` to be called immediately before `validate()` so the `liveChildQty` is accurate for the current work cycle
**Failure mode:** Calling `validate()` with a stale `liveChildQty` allows concurrent child creation to exceed `parent.leavesQty()`; this corrupts the parent's fill accounting

---

#### ChildOrderRegistry

**Class:** `com.cobain.oms.core.ChildOrderRegistry`
**Module:** `oms-core`
**Thread model:** single-threaded (Raft commit thread); two flyweights (`sharedFlyweight`, `secondFlyweight`) prevent clobbering when child and parent are accessed in the same cycle
**Zero-allocation contract:** All state pre-allocated: `UnsafeBuffer` store (`capacity × 128 bytes`), two `Long2LongHashMap` indexes pre-sized to `capacity × 2` at load factor 0.65f (no hot-path rehashing), `int[]` free-slot stack, two `OrderFlyweight` instances
**Inputs:** `createChild(ChildOrderIntentFlyweight, OrderFlyweight, long childOrderId)`, `applyFillAndAggregate()`, `computeLiveChildQty()`, `cancelAllChildren()`
**Outputs:** `createChild()` returns `sharedFlyweight` or null if full; `applyFillAndAggregate()` returns `secondFlyweight` (parent) or null if child not found
**Invariants:** `DEFAULT_CAPACITY = 32_768`; `MISSING = Long.MIN_VALUE`; `NO_SIBLING = -1`; child linked list: `parent.parentOrFirstChildId()` = first child orderId; `child.nextSiblingSlot()` = next sibling slot or -1; child clOrdId formula: `parentOrderId * 10_000L + (sliceIndex & 0xFF)`
**Failure mode:** Retaining `sharedFlyweight` reference past the next `createChild()` call gives stale data; `computeLiveChildQty()` is O(n) where n = live children — must be called before `validate()` in the same cycle

---

#### SnapshotManager

**Class:** `com.cobain.oms.cluster.SnapshotManager`
**Module:** `oms-core`
**Thread model:** called from the Raft commit thread during `onTakeSnapshot()` and `onStart()`; not called during normal order processing
**Zero-allocation contract:** `snapshotBuffer` is a pre-allocated `UnsafeBuffer` backed by `ByteBuffer.allocateDirect()` in `SnapshotManager(int maxOrders, int maxChildren)` — no runtime allocation during snapshot/restore
**Inputs:** `SnapshotManager(int maxOrders, int maxChildren)` (no-arg delegates to defaults); `takeSnapshot(...)`, `loadSnapshot(...)`
**Outputs:** serialized snapshot offered to `snapshotPublication`; `OrderBook.restoreOrder()`, `ValidationEngine.restoreEntry()`, and `ChildOrderRegistry.restore()` called during load
**Invariants:** `SNAPSHOT_VERSION = 3`; `HEADER_SIZE = 24`; `DEDUP_ENTRY_SIZE = 16`; buffer size = `24 + maxOrders×128 + maxOrders×16 + 4 + maxChildren×128`; header includes `snapshotMaxOrders` at [16] and `snapshotMaxChildren` at [20]; restore validates current capacity ≥ snapshot capacity
**Failure mode:** Version mismatch or capacity downsize throws `IllegalStateException` — wipe archive and restart with `OMS_ARCHIVE_DELETE_ON_START=true`

---

#### OmsClusteredService

**Class:** `com.cobain.oms.cluster.OmsClusteredService`
**Module:** `oms-core`
**Thread model:** single-threaded Raft commit thread (Aeron Cluster `ClusteredService` contract); `intentFragmentHandler` is called from within `onSessionMessage()` and `onTimerEvent()` — same thread
**Zero-allocation contract:** All components pre-allocated in constructor; `algoOutbound` (129 bytes), `egressBuffer` (129 bytes), `intentView`, `decodeFlyweight` all pre-allocated; `intentFragmentHandler` is a pre-allocated method reference (not a lambda)
**Constructor:** `OmsClusteredService(algoSorPublication, clientPublication, intentSub, maxNotional, maxOrders, maxChildren, intentFragmentLimit, permittedSymbols...)` — capacity values resolved by `OmsNode.loadOmsConfig()` via `OmsConfig` (see `oms-config` module) and passed as constructor arguments; env vars `OMS_MAX_ORDERS` (default 65_536), `OMS_MAX_CHILDREN` (default 32_768), `OMS_INTENT_FRAGMENT_LIMIT` (default 20) remain the primary source names
**Inputs:** `onSessionMessage()` (Raft commit), `onTimerEvent()` (Raft timer), `onStart()` (snapshot restore), `onTakeSnapshot()` (snapshot write)
**Outputs:** `algoSorPublication.offer()` on stream 30; `session.offer()` (egress) on cluster egress; `fixEncoder.sendNewOrderSingle()` on stream 10
**Invariants:** `pollIntents()` is called at the start of every `onSessionMessage()` and `onTimerEvent()` to drain pending intents on the cluster thread; `intentFragmentLimit` is an instance field (configurable, not static); `ParentOrderState.registerTransitions()` called in `onStart()` before snapshot load; `nextOrderId` increments monotonically and is recomputed after restore by scanning both `OrderBook` and `ChildOrderRegistry`; `republishRoutingOrders()` called after every `onStart()` to re-route NEW/ROUTING/PARTIALLY_FILLED parents
**Failure mode:** A crash between `childRegistry.createChild()` and `fixEncoder.sendNewOrderSingle()` leaves a child in the registry with no corresponding NOS sent; reconciled via FIX OrderStatusRequest (35=H) on reconnect

---

#### FIXMessageEncoder

**Class:** `com.cobain.oms.fix.FIXMessageEncoder`
**Module:** `oms-core`
**Thread model:** single-threaded Raft commit thread; one instance per `OmsClusteredService`
**Zero-allocation contract:** Single `outbound` `UnsafeBuffer` (76 bytes) pre-allocated in constructor; all encode methods write into this buffer and call `offer()` — one Aeron offer, zero allocation
**Inputs:** `sendNewOrderSingle(OrderFlyweight)`, `sendCancelRequest()`, `sendCancelReplace()`, `sendExecReport()`
**Outputs:** `fixEnginePublication.offer(outbound, 0, 76)` — non-blocking; returns publication position or negative back-pressure code
**Invariants:** `offer()` is non-blocking — back-pressure is not handled here (callers may see negative result without retry); output stream 10 (`STREAM_OMS_TO_FIX`) is shared between parent exec reports and child NOS
**Failure mode:** A negative `offer()` result on `sendNewOrderSingle()` means the child NOS is dropped; child exists in `ChildOrderRegistry` but venue never received the order — requires FIX reconciliation

---

## algo-sor Components

Components in this section run inside `AlgoSorAgent`'s dedicated thread and must
never reference `ChildOrderRegistry`, `OrderBook`, or `FIXMessageEncoder`.

---

#### AlgoSorAgent

**Class:** `com.cobain.oms.algoagent.AlgoSorAgent`
**Module:** `algo-sor`
**Thread model:** single-threaded `AgentRunner` (dedicated OS thread via `AgentRunner.startOnThread()`); `doWork()` is the duty cycle
**Zero-allocation contract:** `payloadBuffer` (zero-length `UnsafeBuffer` re-wrapped on each fragment — no allocation), `parentView` (pre-allocated flyweight), `intentOfferBuffer` (`HEADER_LENGTH + BLOCK_LENGTH = 64` bytes pre-allocated); header bytes written once at construction
**Constructor:** `AlgoSorAgent(parentOrderSub, intentPub, icebergEngine, twapEngine, sor, fragmentLimit, maxVenues)` — capacity values resolved by `OmsLauncher.loadOmsConfig()` via `OmsConfig`; env vars `OMS_ALGO_FRAGMENT_LIMIT` (default 10), `OMS_MAX_VENUES` (default 10), `OMS_MAX_ICEBERG_ORDERS` (default 4_096), `OMS_MAX_TWAP_ORDERS` (default 512) are the primary source names
**Inputs:** `doWork()` polls `parentOrderSub` (stream 30) with `fragmentLimit` fragments per cycle (instance field, not static)
**Outputs:** `intentPub.offer()` on stream 12 via `publishIntent()`; spins with `Thread.onSpinWait()` on back-pressure — never sleeps
**Invariants:** `fragmentLimit` is an instance field (default 10 via `OMS_ALGO_FRAGMENT_LIMIT`); venue arrays (`venueIds`, `venuePrices`, `venueQtys`) pre-allocated at `maxVenues` in constructor; publishes ONLY `CHILD_ORDER_INTENT (40)` messages; never calls `ChildOrderRegistry`, `OrderBook`, or `FIXMessageEncoder`; `SmartOrderRouter` reuses a single pre-allocated `intent` flyweight per `route()` call
**Failure mode:** Back-pressure on `intentPub` spins indefinitely if `oms-core` is not consuming from stream 12; `Publication.CLOSED` exits the spin without error

---

#### SmartOrderRouter

**Class:** `com.cobain.oms.algo.SmartOrderRouter`
**Module:** `algo-sor`
**Thread model:** single-threaded `AlgoSorAgent`
**Zero-allocation contract:** Single `ChildOrderIntentFlyweight intent` pre-allocated via `ChildOrderIntentFlyweight.allocate()` in constructor; all arithmetic in longs; `sink.onIntent(intent)` is called with the same `intent` instance for every slice
**Inputs:** `route(OrderFlyweight, int[], long[], long[], int, ChildIntentSink)` called from `AlgoSorAgent.onFragment()`
**Outputs:** calls `sink.onIntent(intent)` once per venue slice; returns count of intents published
**Constructor:** `SmartOrderRouter(int maxVenues)` — `OmsLauncher` reads `OMS_MAX_VENUES` (default 10) from `OmsConfig`; no-arg delegates to `MAX_VENUES` default
**Invariants:** `maxVenues` is an instance field (default `MAX_VENUES = 10`); if no venue matches, routes entire qty to venue ID 1 at `parent.price()`; `intent` is reused — callers must copy before calling `onIntent()` returns
**Failure mode:** `sink.onIntent(intent)` retaining the reference beyond the call boundary reads stale data from the next slice; venue ID 0 is skipped (treated as inactive)

---

#### IcebergAlgoEngine

**Class:** `com.cobain.oms.algo.IcebergAlgoEngine`
**Module:** `algo-sor`
**Thread model:** single-threaded `AlgoSorAgent`
**Zero-allocation contract:** `remainingQty` and `peakQty` are `Long2LongHashMap` instances pre-sized to `maxConcurrent × 2` at load factor 0.65f in the constructor — no rehash on the hot path. One `ChildOrderIntentFlyweight intent` pre-allocated via `ChildOrderIntentFlyweight.allocate()`.
**Constructor:** `IcebergAlgoEngine(long peakFraction, int maxConcurrent)` — `OmsLauncher` passes `peakFraction=10` and reads `OMS_MAX_ICEBERG_ORDERS` (default 4_096) from `OmsConfig`; 1-arg constructor delegates to `maxConcurrent=4_096`.
**Invariants:** `MAX_SLICES_PER_PARENT = 256` — enforced by guard in `onSlice()`; sliceIndex is a byte (0-255) so the clOrdId formula `parentOrderId × 10_000 + (sliceIndex & 0xFF)` supports at most 256 children per parent. At default `peakFraction=10`, at most ~10 slices are dispatched per parent.
**Failure mode:** If `dispatched ≥ MAX_SLICES_PER_PARENT`, remaining qty is not dispatched and WARN is logged — the parent order stalls; increase `peakFraction` or reduce order qty to avoid this.

---

#### TwapAlgoEngine

**Class:** `com.cobain.oms.algo.TwapAlgoEngine`
**Module:** `algo-sor`
**Thread model:** single-threaded `AlgoSorAgent`
**Zero-allocation contract:** All per-handle arrays (`parentOrderId`, `sliceQty`, `slicesFired`, etc.) pre-allocated at `maxTwapOrders` in the constructor; `freeHandles` free-stack for O(1) alloc/free; one pre-allocated `ChildOrderIntentFlyweight intent`.
**Constructor:** `TwapAlgoEngine(int maxTwapOrders)` — `OmsLauncher` reads `OMS_MAX_TWAP_ORDERS` (default 512) from `OmsConfig`; no-arg constructor delegates to `MAX_TWAP_ORDERS = 512`.
**Invariants:** `maxTwapOrders` is an instance field; `DEFAULT_SLICES = 12`; `DEFAULT_INTERVAL_MS = 5 min`; when all handles are occupied `acquireHandle()` returns -1 and `onSlice()` falls back to single-slice dispatch with a WARN log.
**Failure mode:** Fallback to single-slice on capacity exhaustion is silent except for the WARN log — TWAP schedule is abandoned; raise `OMS_MAX_TWAP_ORDERS` if the WARN fires in production.
