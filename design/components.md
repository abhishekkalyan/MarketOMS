# Component Contracts
<!-- Load when: touching any class listed below, or introducing a new
     component. Update the relevant entry after any contract change. -->

## Section 4 — Component Contracts

#### OrderLayout

**Class:** `com.cobain.oms.model.OrderLayout`
**Module:** `oms-codec`
**Thread model:** static constants class — no instances, no threads
**Zero-allocation contract:** All fields are `public static final` primitives; class-loaded once, never instantiated
**Inputs:** consumed by every flyweight, encoder, decoder, and validator
**Outputs:** defines byte offsets used by `OrderFlyweight`, `FIXMessageDecoder`, `FIXMessageEncoder`, `SnapshotManager`
**Invariants:** `OFFSET_PARENT_OR_FIRST_CHILD_ID + Long.BYTES == BLOCK_LENGTH` (static assert at class load)
**Failure mode:** If a new field is added past offset 120, the static assert fires at JVM startup as `AssertionError`

---

#### OrderFlyweight

**Class:** `com.cobain.oms.model.OrderFlyweight`
**Module:** `oms-codec`
**Thread model:** single-threaded; one instance per logical thread (one shared instance in `OrderBook`, two in `ChildOrderRegistry`)
**Zero-allocation contract:** Allocated once at construction; `wrap(MutableDirectBuffer, int)` assigns two fields — no objects created; all getters/setters are single `buffer.getLong/putLong` calls
**Inputs:** `wrap()` call pointing at an `OrderBook` slot or `ChildOrderRegistry` store slot
**Outputs:** direct buffer reads/writes; `copyFrom()` for bulk copy; `applyFill()` for quantity update
**Invariants:** caller must not retain the reference past the next `wrap()` call on the same instance; `buffer` and `offset` always point at a valid allocated slot
**Failure mode:** Stale reference after a subsequent `wrap()` points at the wrong order; `applyFill()` with a negative `lastQty` corrupts `filledQty`

---

#### ChildOrderIntentFlyweight

**Class:** `com.cobain.oms.codec.ChildOrderIntentFlyweight`
**Module:** `oms-codec`
**Thread model:** single-threaded; one pre-allocated instance per algo engine, one in `OmsClusteredService` (`intentView`)
**Zero-allocation contract:** `wrap()` / `wrapReadOnly()` assign two fields; `copyTo()` is a single `putBytes()`; `allocate()` creates one instance with its own `UnsafeBuffer` at startup only
**Inputs:** `wrapReadOnly()` over the Aeron fragment buffer in `OmsClusteredService.handleIntentFragment()`; setters called by `SmartOrderRouter.route()`
**Outputs:** `copyTo()` used by `AlgoSorAgent.publishIntent()` to stage into the offer buffer
**Invariants:** `BLOCK_LENGTH = 56`; caller must not retain reference past the next `wrap()` call
**Failure mode:** Retaining a reference after `onIntent()` returns gives stale data because `SmartOrderRouter` reuses the same `intent` instance for every slice

---

#### ClusterMessageType

**Class:** `com.cobain.oms.codec.ClusterMessageType`
**Module:** `oms-codec`
**Thread model:** static constants class — no instances, no threads
**Zero-allocation contract:** All fields are `public static final` primitives
**Inputs:** referenced by `OmsClusteredService`, `AlgoSorAgent`, `OmsLauncher`, harness
**Outputs:** defines framing constants used to encode/decode all IPC messages
**Invariants:** `IPC_MESSAGE_SIZE = 1 + OrderLayout.MESSAGE_SIZE = 129`; `HEADER_LENGTH = 8` for intent messages
**Failure mode:** Adding a new message type without updating all `switch` sites in `OmsClusteredService.onSessionMessage()` causes silent message drop on the `default` branch

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

#### OrderState + OrderEvent

**Class:** `com.cobain.oms.model.OrderState`, `com.cobain.oms.model.OrderEvent`
**Module:** `oms-codec`
**Thread model:** static constants classes — no instances, no threads
**Zero-allocation contract:** All fields are `public static final` byte/int primitives
**Inputs:** used as array indices into `OrderStateMachine.TRANSITION_TABLE[state][event]`
**Outputs:** `isTerminal(byte)` returns `state >= FILLED` (>= 6); `nameOf()` allocates a String (off-hot-path only)
**Invariants:** `NUM_STATES = 16`, `NUM_EVENTS = 10`; `INVALID_TRANSITION = -1 (0xFF)`; terminal states are FILLED(6), CANCELED(7), REJECTED(8), EXPIRED(9)
**Failure mode:** Adding a new state ≥ 16 causes `ArrayIndexOutOfBoundsException` in `OrderStateMachine.transition()`

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

#### OrderBook

**Class:** `com.cobain.oms.core.OrderBook` (physically in `oms-codec` module — see anomaly in principles.md Section 3.1)
**Module:** `oms-codec` (compile), `oms-core` (runtime ownership)
**Thread model:** single-threaded; `flyweight` is shared across all callers — not safe for concurrent use
**Zero-allocation contract:** All state is pre-allocated in constructor: `byte[]` backing array (65,536 × 128 bytes = 8 MB), two `Long2LongHashMap` indexes, one `int[]` free-slot stack, two `OrderFlyweight` instances
**Inputs:** `allocateSlot()`, `index()`, `wrapFlyweight()`, `freeSlot()`, `unindex()`, `restoreOrder()`
**Outputs:** `wrapFlyweight(slot)` returns `flyweight` (shared — caller must not retain); `buffer()` returns the raw `UnsafeBuffer`; `slotByOrderId()` / `slotByClOrdId()` return slot index or -1
**Invariants:** `MAX_ORDERS = 65_536`; `EMPTY = Long.MIN_VALUE`; `freeTop == 0` means full (`allocateSlot()` returns -1); `slotToOrderId[slot] == 0L` means free
**Failure mode:** Calling `wrapFlyweight()` on a freed slot gives a flyweight pointing at zeroed bytes with no error; `freeSlot()` without `unindex()` leaves stale entries in the maps causing phantom lookups

---

#### ChildOrderRegistry

**Class:** `com.cobain.oms.core.ChildOrderRegistry`
**Module:** `oms-core`
**Thread model:** single-threaded (Raft commit thread); two flyweights (`sharedFlyweight`, `secondFlyweight`) prevent clobbering when child and parent are accessed in the same cycle
**Zero-allocation contract:** All state pre-allocated: `UnsafeBuffer` store (32,768 × 128 bytes = 4 MB), two `Long2LongHashMap` indexes, `int[]` free-slot stack, two `OrderFlyweight` instances
**Inputs:** `createChild(ChildOrderIntentFlyweight, OrderFlyweight, long childOrderId)`, `applyFillAndAggregate()`, `computeLiveChildQty()`, `cancelAllChildren()`
**Outputs:** `createChild()` returns `sharedFlyweight` or null if full; `applyFillAndAggregate()` returns `secondFlyweight` (parent) or null if child not found
**Invariants:** `DEFAULT_CAPACITY = 32_768`; `MISSING = Long.MIN_VALUE`; `NO_SIBLING = -1`; child linked list: `parent.parentOrFirstChildId()` = first child orderId; `child.nextSiblingSlot()` = next sibling slot or -1; child clOrdId formula: `parentOrderId * 10_000L + (sliceIndex & 0xFF)`
**Failure mode:** Retaining `sharedFlyweight` reference past the next `createChild()` call gives stale data; `computeLiveChildQty()` is O(n) where n = live children — must be called before `validate()` in the same cycle

---

#### SnapshotManager

**Class:** `com.cobain.oms.cluster.SnapshotManager`
**Module:** `oms-core`
**Thread model:** called from the Raft commit thread during `onTakeSnapshot()` and `onStart()`; not called during normal order processing
**Zero-allocation contract:** `snapshotBuffer` is a pre-allocated `UnsafeBuffer` backed by a `ByteBuffer.allocateDirect(MAX_SNAPSHOT_BYTES)` at construction — no runtime allocation during snapshot/restore
**Inputs:** `takeSnapshot(OrderBook, ValidationEngine, ExclusivePublication, IdleStrategy)`, `loadSnapshot(Image, OrderBook, ValidationEngine, IdleStrategy)`
**Outputs:** serialized snapshot offered to `snapshotPublication`; `OrderBook.restoreOrder()` and `ValidationEngine.restoreEntry()` called during load
**Invariants:** `SNAPSHOT_VERSION = 1`; `HEADER_SIZE = 16`; `DEDUP_ENTRY_SIZE = 16`; `MAX_SNAPSHOT_BYTES = 16 + 65_536 × 128 + 65_536 × 16 ≈ 9.4 MB`; snapshot wire order: header → order records → dedup entries
**Failure mode:** Snapshot taken without `ChildOrderRegistry` state (current implementation) loses child orders on failover; `handleSnapshotFragment()` throws `IllegalStateException` if version mismatches

---

#### OmsClusteredService

**Class:** `com.cobain.oms.cluster.OmsClusteredService`
**Module:** `oms-core`
**Thread model:** single-threaded Raft commit thread (Aeron Cluster `ClusteredService` contract); `intentFragmentHandler` is called from within `onSessionMessage()` and `onTimerEvent()` — same thread
**Zero-allocation contract:** All components pre-allocated in constructor; `algoOutbound` (129 bytes), `egressBuffer` (129 bytes), `intentView`, `decodeFlyweight` all pre-allocated; `intentFragmentHandler` is a pre-allocated method reference (not a lambda)
**Inputs:** `onSessionMessage()` (Raft commit), `onTimerEvent()` (Raft timer), `onStart()` (snapshot restore), `onTakeSnapshot()` (snapshot write)
**Outputs:** `algoSorPublication.offer()` on stream 30; `session.offer()` (egress) on cluster egress; `fixEncoder.sendNewOrderSingle()` on stream 10
**Invariants:** `pollIntents()` is called at the start of every `onSessionMessage()` and `onTimerEvent()` to drain pending intents on the cluster thread; `ParentOrderState.registerTransitions()` called in `onStart()` before snapshot load; `nextOrderId` increments monotonically from 1
**Failure mode:** A crash between `childRegistry.createChild()` and `fixEncoder.sendNewOrderSingle()` leaves a child in the registry with no corresponding NOS sent; reconciled via FIX OrderStatusRequest (35=H) on reconnect

---

#### AlgoSorAgent

**Class:** `com.cobain.oms.algoagent.AlgoSorAgent`
**Module:** `algo-sor`
**Thread model:** single-threaded `AgentRunner` (dedicated OS thread via `AgentRunner.startOnThread()`); `doWork()` is the duty cycle
**Zero-allocation contract:** `payloadBuffer` (zero-length `UnsafeBuffer` re-wrapped on each fragment — no allocation), `parentView` (pre-allocated flyweight), `intentOfferBuffer` (`HEADER_LENGTH + BLOCK_LENGTH = 64` bytes pre-allocated); header bytes written once at construction
**Inputs:** `doWork()` polls `parentOrderSub` (stream 30) with `FRAGMENT_LIMIT = 10` fragments per cycle
**Outputs:** `intentPub.offer()` on stream 12 via `publishIntent()`; spins with `Thread.onSpinWait()` on back-pressure — never sleeps
**Invariants:** `FRAGMENT_LIMIT = 10`; publishes ONLY `CHILD_ORDER_INTENT (40)` messages; never calls `ChildOrderRegistry`, `OrderBook`, or `FIXMessageEncoder`; `SmartOrderRouter` reuses a single pre-allocated `intent` flyweight per `route()` call
**Failure mode:** Back-pressure on `intentPub` spins indefinitely if `oms-core` is not consuming from stream 12; `Publication.CLOSED` exits the spin without error

---

#### SmartOrderRouter

**Class:** `com.cobain.oms.algo.SmartOrderRouter`
**Module:** `algo-sor`
**Thread model:** single-threaded `AlgoSorAgent`
**Zero-allocation contract:** Single `ChildOrderIntentFlyweight intent` pre-allocated via `ChildOrderIntentFlyweight.allocate()` in constructor; all arithmetic in longs; `sink.onIntent(intent)` is called with the same `intent` instance for every slice
**Inputs:** `route(OrderFlyweight, int[], long[], long[], int, ChildIntentSink)` called from `AlgoSorAgent.onFragment()`
**Outputs:** calls `sink.onIntent(intent)` once per venue slice; returns count of intents published
**Invariants:** `MAX_VENUES = 10`; if no venue matches, routes entire qty to venue ID 1 at `parent.price()`; `intent` is reused — callers must copy before calling `onIntent()` returns
**Failure mode:** `sink.onIntent(intent)` retaining the reference beyond the call boundary reads stale data from the next slice; venue ID 0 is skipped (treated as inactive)

---

#### FIXMessageDecoder

**Class:** `com.cobain.oms.fix.FIXMessageDecoder`
**Module:** `oms-codec`
**Thread model:** static utility class — called on the Raft commit thread from `OmsClusteredService.handleExecReport()`
**Zero-allocation contract:** All methods are static; each method is a sequence of `buffer.getLong/getByte` + flyweight setter calls; `mapExecTypeToEvent()` is a single `switch` tableswitch — zero allocation
**Inputs:** `DirectBuffer src` at `srcOffset` containing a 76-byte FIX Binary message; writable `OrderFlyweight target`
**Outputs:** fields written directly into the target flyweight; `decodeExecReport()` returns the `execType` byte; `getLastQty()` returns `long` fill qty
**Invariants:** `FIX_BINARY_SIZE = 76`; used ONLY for the FIX bridge → oms-core IPC path (stream 11); must NOT be used to decode cluster ingress messages (those use `ClusterMessageType` framing)
**Failure mode:** Decoding a cluster ingress message with `FIXMessageDecoder` interprets the `ClusterMessageType` byte (1/2/3) as an ASCII MsgType — silent data corruption

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
