# MarketOMS Design Knowledge Base

**Repository:** abhishekkalyan/MarketOMS
**System type:** Sell-side Order Management System (OMS)
**Language:** Java 17
**Build system:** Gradle 9.3 multi-module monorepo (`market-oms`)
**Package root:** `com.cobain.oms`
**Modules:** `oms-codec`, `oms-core`, `algo-sor`, `oms-launcher`, `oms-harness`
**Primary transport:** Aeron IPC (stream 30, stream 12) and Aeron Cluster (Raft)
**Key libraries:** Aeron 1.47.0, Agrona 2.0.1
**Latency target:** Deterministic sub-millisecond; zero GC on hot path
**Last updated:** 2026-06-01

## Purpose of This Document

This is the authoritative design reference for MarketOMS. It must be read
by every Claude Code session before performing design analysis, impact
assessment, or architecture review. See the Design Analysis Checklist
(Section 12) for the mandatory procedure before any change.

---

## Section 2 — Core Design Principles

1. **Zero object allocation on the hot path**
   - **Rule:** No `new` expressions in `onSessionMessage()`, `validateNewOrder()`, `validate()`, `createChild()`, or `onFragment()`.
   - **Enforced by:** `OrderFlyweight`, `ChildOrderIntentFlyweight`, `AlgoSorAgent` (all pre-allocated in constructors; re-wrapped on every message via `wrap()`)
   - **Violation consequence:** GC pauses introduce non-deterministic latency spikes in p99.99; BusySpinIdleStrategy exacerbates this because pauses are visible as latency, not CPU yield

2. **Single-threaded execution per component**
   - **Rule:** No `synchronized`, `AtomicReference`, or `ReentrantLock` in hot-path code.
   - **Enforced by:** `OmsClusteredService` (runs on the Raft commit thread), `AlgoSorAgent` (runs inside a single `AgentRunner`)
   - **Violation consequence:** Concurrent mutation of `OrderBook.flyweight` or `ChildOrderRegistry.sharedFlyweight` causes data corruption with no exception thrown

3. **`oms-core` is the golden source of all order state**
   - **Rule:** All parent and child order records are born, mutated, and deleted exclusively inside `oms-core`.
   - **Enforced by:** `ChildOrderRegistry.createChild()` is the only birth point for child orders; `OrderBook.allocateSlot()` is the only birth point for parent orders
   - **Violation consequence:** State created outside `oms-core` is not replicated by Raft; after a leader failover it disappears, leaving fills unmatched

4. **`algo-sor` is a pure computation engine — publishes intents, creates no state**
   - **Rule:** `algo-sor` code must not call `ChildOrderRegistry`, `OrderBook`, or `FIXMessageEncoder`.
   - **Enforced by:** `AlgoExecutionEngine.ChildIntentSink` interface — engines return intents, not orders; Gradle compile-time boundary prevents `algo-sor` depending on `oms-core`
   - **Violation consequence:** `algo-sor` state that survives a crash is irrecoverable because only `oms-core` state is snapshotted

5. **`ChildOrderIntentValidator` runs before `ChildOrderRegistry.createChild()`**
   - **Rule:** Every `createChild()` call must be preceded by a `PASS` result from `ChildOrderIntentValidator.validate()`.
   - **Enforced by:** `OmsClusteredService.onChildOrderIntent()` call order: validate → create
   - **Violation consequence:** Over-allocation corrupts `leavesQty` below zero; `applyFill()` clamps to zero so subsequent fills are silently discarded

6. **Module boundaries enforced at compile time**
   - **Rule:** `algo-sor` must not import `oms-core`; `oms-codec` must not import Aeron Cluster.
   - **Enforced by:** Gradle `build.gradle` dependency declarations; verify with `./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core` (must produce no output)
   - **Violation consequence:** `algo-sor` importing `oms-core` breaks the independent-restart guarantee and creates circular deployment dependency

7. **Fixed-point long arithmetic for all prices (`PRICE_MULTIPLIER = 10_000L`)**
   - **Rule:** All price values are `long` multiplied by `OrderLayout.PRICE_MULTIPLIER`. No `double`, `float`, or `BigDecimal` on the hot path.
   - **Enforced by:** `OrderLayout.PRICE_MULTIPLIER`, `ValidationEngine.validateNewOrder()` notional check uses integer division only
   - **Violation consequence:** `double` introduces non-determinism across Raft replicas due to CPU-specific floating-point rounding; `BigDecimal` allocates a new object on every arithmetic operation

8. **Raft-replicated state machine — all mutations in `onSessionMessage()`**
   - **Rule:** Every mutation to `OrderBook` or `ChildOrderRegistry` must execute inside `onSessionMessage()` or `onLoadSnapshot()`, which fire only after Raft quorum commit.
   - **Enforced by:** `OmsClusteredService` implements `ClusteredService`; `onChildOrderIntent()` is called from within `onSessionMessage()` via `pollIntents()`
   - **Violation consequence:** State mutated outside the Raft commit path diverges replicas silently — two nodes serve different order states after a leader election

9. **Idempotent validation via `ValidationEngine.seenClOrdIds`**
   - **Rule:** Every accepted order's `clOrdId` is registered in `seenClOrdIds` before the method returns; all subsequent duplicate attempts return `ERR_DUPLICATE_CL_ORD_ID`.
   - **Enforced by:** `ValidationEngine.validateNewOrder()` dedup check + `ValidationEngine.registerAccepted()` called after every accepted order
   - **Violation consequence:** During Raft log replay after failover, a replayed order is accepted a second time, creating a duplicate in `OrderBook` and a double-allocation of `nextOrderId`

10. **Symbol encoded as long — no String on hot path**
    - **Rule:** Symbol comparisons use `long` equality; `OrderFlyweight.encodeSymbol(String)` is called only at startup or order creation setup.
    - **Enforced by:** `OrderFlyweight.encodeSymbol()` (off-hot-path helper); `ValidationEngine.permittedSymbols` is a `LongHashSet`
    - **Violation consequence:** String allocation on every order check creates GC pressure proportional to order rate

---

## Section 3 — Module Dependency Graph

### 3.1 Compile-time dependencies

| Module | Depends On | Must Never Depend On | Boundary Check Command |
|--------|------------|---------------------|----------------------|
| `oms-codec` | Agrona only | Aeron Cluster, `oms-core`, `algo-sor` | `./gradlew :oms-codec:dependencies --configuration compileClasspath \| grep aeron-cluster` → no output |
| `oms-core` | `oms-codec`, Aeron (all) | `algo-sor` | `./gradlew :oms-core:dependencies --configuration compileClasspath \| grep algo-sor` → no output |
| `algo-sor` | `oms-codec`, Aeron client/driver/cluster | `oms-core` | `./gradlew :algo-sor:dependencies --configuration compileClasspath \| grep oms-core` → no output |
| `oms-launcher` | `oms-codec`, `oms-core`, `algo-sor`, Aeron (all) | — | — |
| `oms-harness` | `oms-codec`, Aeron (all) | `oms-core`, `algo-sor` | `./gradlew :oms-harness:dependencies --configuration compileClasspath \| grep -E "oms-core\|algo-sor"` → no output |

**Anomaly:** `OrderBook.java` is physically in `oms-codec/src/main/java/com/cobain/oms/core/` but its package
is `com.cobain.oms.core`. It is compiled as part of `oms-codec`. This allows `oms-harness` to use
`OrderBook` without depending on `oms-core`, at the cost of a package name that contradicts the module boundary.

### 3.2 Runtime Aeron IPC channel map

| Stream ID | Publisher | Subscriber | Message Type | Payload | Notes |
|-----------|-----------|------------|--------------|---------|-------|
| 10 | `oms-core` (`FIXMessageEncoder`) | FIX connectivity engine | FIX Binary NOS | 76 bytes | Outbound child NOS to venue |
| 11 | FIX connectivity engine | `oms-core` (`OmsClusteredService`) | FIX Binary ExecReport | 76 bytes | Inbound venue fills |
| 12 | `algo-sor` (`AlgoSorAgent.intentPub`) | `oms-core` (`OmsClusteredService.intentSub`) | `CHILD_ORDER_INTENT (40)` | 8-byte header + 56-byte intent | `STREAM_CHILD_INTENTS` |
| 20 | `oms-core` | Venue 1 FIX engine | FIX Binary NOS | 76 bytes | `STREAM_VENUE_1_OUT` |
| 21 | `oms-core` | Venue 2 FIX engine | FIX Binary NOS | 76 bytes | `STREAM_VENUE_2_OUT` |
| 22 | `oms-core` | Venue 3 FIX engine | FIX Binary NOS | 76 bytes | `STREAM_VENUE_3_OUT` |
| 30 | `oms-core` (`OmsClusteredService.algoSorPublication`) | `algo-sor` (`AlgoSorAgent.parentOrderSub`) | `NEW_ORDER (1)` + OrderLayout | 129 bytes (1 + 128) | `STREAM_OMS_TO_ALGO` / `STREAM_PARENT_ORDERS` |
| 9000 (UDP) | Cluster clients | `OmsClusteredService` via ConsensusModule | `ClusterMessageType` framed | 129 bytes | Aeron Cluster ingress |

### 3.3 Module responsibility matrix

| Module | Owns | Must Not Own | Stateful? |
|--------|------|--------------|-----------|
| `oms-codec` | Binary wire formats, field constants, FIX decoder, `OrderBook` (anomaly — see 3.1) | Business logic, Aeron connections | none |
| `oms-core` | All order state (parent + child), state machine, validation, Raft service | Algo computation, routing decisions | durable (Raft-snapshotted) |
| `algo-sor` | Routing computation, slice scheduling, algo engines | Order records, `OrderBook`, `ChildOrderRegistry` | soft (ephemeral, lost on restart) |
| `oms-launcher` | Process wiring, `AgentRunner` lifecycle | Order processing logic | none |
| `oms-harness` | Test scenario injection, exec report validation | Production order processing | none |

---

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

**Class:** `com.cobain.oms.core.OrderBook` (physically in `oms-codec` module — see anomaly in 3.1)
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

---

## Section 5 — Data Flow Diagrams

### Diagram 5.1: Inbound new order — client to venue

```
FIX Client (TCP)
       │ FIX tag=value ASCII
       ▼
FIX Connectivity Engine (external)
  • parses to FIX Binary (76 bytes)
       │ Aeron IPC stream 11
       ▼
OmsClusteredService.onSessionMessage()
  ┌─────────────────────────────────────────────────────────┐
  │  1. pollIntents() — drain any pending ChildOrderIntents │
  │  2. msgType = buffer.getByte(offset + OFFSET_MSG_TYPE)  │
  │  3. handleNewOrderSingle()                              │
  │     a. orderBook.allocateSlot() → slot (or -1 if full) │
  │     b. orderBook.wrapFlyweight(slot) — populate fields  │
  │        from buffer at OFFSET_PAYLOAD                    │
  │     c. order.orderId(nextOrderId++), state=NEW          │
  │     d. validationEngine.validateNewOrder() → int VALID  │
  │     e. orderBook.index(slot, clOrdId, orderId)          │
  │     f. validationEngine.registerAccepted(clOrdId,ordId) │
  │     g. algoSorPublication.offer(algoOutbound, 0, 129)   │
  │        stream 30: NEW_ORDER(1) + 128-byte OrderLayout   │
  │     h. sendEgressExecReport(session, NEW_ORDER, order)  │
  └─────────────────────────────────────────────────────────┘
       │ Aeron IPC stream 30
       ▼
AlgoSorAgent.doWork() → parentOrderSub.poll(this, 10)
  onFragment():
    payloadBuffer.wrap(buffer, offset + OFFSET_PAYLOAD, 128)
    parentView.wrap(payloadBuffer, 0)
    SmartOrderRouter.route(parentView, venueIds, venuePrices, venueQtys, count, this::publishIntent)
      → for each venue: intent.set*() + sink.onIntent(intent)
    publishIntent(intent):
      intent.copyTo(intentOfferBuffer, HEADER_LENGTH=8)
      intentPub.offer(intentOfferBuffer, 0, 64)  [8 header + 56 intent]
       │ Aeron IPC stream 12
       ▼
OmsClusteredService.handleIntentFragment() [called from pollIntents()]
  ┌─────────────────────────────────────────────────────────┐
  │  1. intentView.wrapReadOnly(buffer, offset+HEADER_LENGTH)│
  │  2. parentOrderId = intentView.getParentOrderId()       │
  │  3. parentSlot = orderBook.slotByOrderId(parentOrderId) │
  │  4. parent = orderBook.wrapFlyweight(parentSlot)        │
  │  5. liveChildQty = childRegistry.computeLiveChildQty()  │
  │  6. intentValidator.validate() → byte PASS=0            │
  │  7. childOrderId = nextOrderId++                        │
  │  8. child = childRegistry.createChild(intent, parent, …)│
  │  9. if parent.state==NEW: OrderStateMachine             │
  │        .transitionToRouting(parent)                     │
  │ 10. fixEncoder.sendNewOrderSingle(child) → stream 10   │
  └─────────────────────────────────────────────────────────┘
       │ Aeron IPC stream 10
       ▼
FIX Connectivity Engine → Venue / Exchange
```

### Diagram 5.2: Venue fill → client execution report

```
Venue / Exchange
       │ FIX ExecReport (35=8)
       ▼
FIX Connectivity Engine
       │ FIX Binary ExecReport (76 bytes), Aeron IPC stream 11
       ▼
OmsClusteredService.handleExecReport()
  execType = buffer.getByte(offset + EXEC_TYPE_OFFSET_IN)
  event = FIXMessageDecoder.mapExecTypeToEvent(execType)
  clOrdId = buffer.getLong(offset + CL_ORD_ID_OFFSET_IN)
  child = childRegistry.getByClOrdId(clOrdId)
  if child != null:
    lastQty = FIXMessageDecoder.getLastQty(buffer, offset)
    parent = childRegistry.applyFillAndAggregate(clOrdId, lastQty, 0, orderBook)
      1. clOrdIdToOrderId.get(childClOrdId) → childOrderId
      2. orderIdToSlot.get(childOrderId)    → childSlot
      3. sharedFlyweight.wrap(store, childSlot * 128)
      4. sharedFlyweight.applyFill(lastQty)
      5. if leavesQty==0: child.orderState(FILLED)
      6. parentOrderId = sharedFlyweight.parentOrFirstChildId()
      7. parentSlot = orderBook.slotByOrderId(parentOrderId)
      8. secondFlyweight.wrap(parentBook.buffer(), parentBook.offsetForSlot(parentSlot))
      9. secondFlyweight.applyFill(lastQty)
     10. update parent state: FILLED if leavesQty==0, else PARTIALLY_FILLED
    fixEncoder.sendExecReport(parent, execType, lastQty) → stream 10 → client FIX engine
```

### Diagram 5.3: Aeron Cluster node topology

```
FIX Client ──[UDP 9000]──► OmsNode (node 0 — leader)
                                │  ConsensusModule (Raft)
                                │  ◄──────────────────────►  OmsNode (node 1 — follower)
                                │  ◄──────────────────────►  OmsNode (node 2 — follower)
                                │
                           OmsClusteredService
                           (onSessionMessage after Raft quorum commit)
                                │
                    ┌───────────┴────────────┐
                    │                        │
               stream 30                stream 10
            (parent orders)          (NOS to FIX bridge)
                    │
              AlgoSorAgent
              (AgentRunner, dedicated thread)
                    │
               stream 12
            (ChildOrderIntents)
                    │
               OmsClusteredService
            (pollIntents → onChildOrderIntent)
```

---

## Section 6 — Order State Model

### 6.1 Complete state inventory

| State | Byte Value | Applies To | Terminal? | Description |
|-------|-----------|-----------|----------|-------------|
| `PENDING_NEW` | 0 | parent, child | no | Order created in OMS, not yet confirmed by venue |
| `NEW` | 1 | parent, child | no | Venue confirmed order is live |
| `PARTIALLY_FILLED` | 2 | parent, child | no | One or more partial fills received |
| `PENDING_CANCEL` | 3 | parent, child | no | Cancel request sent, awaiting venue ack |
| `PENDING_REPLACE` | 4 | parent | no | Replace request sent, awaiting venue ack |
| `REPLACED` | 5 | parent | no | Venue confirmed replace; order is live again |
| `FILLED` | 6 | parent, child | **yes** | All qty filled |
| `CANCELED` | 7 | parent, child | **yes** | Cancel confirmed |
| `REJECTED` | 8 | parent, child | **yes** | New/replace rejected by venue |
| `EXPIRED` | 9 | parent | **yes** | Expired (DAY, GTD TIF) |
| `ROUTING` | 10 | **parent only** | no | Accepted; ≥ 1 children dispatched, awaiting fills |

`NUM_STATES = 16`, `NUM_EVENTS = 10`. Terminal check: `OrderState.isTerminal(state)` ≡ `state >= FILLED` (≥ 6).

### 6.2 Legal transition table

| From State | Event | To State | Registered In |
|-----------|-------|---------|--------------|
| `PENDING_NEW` | `EXEC_NEW` | `NEW` | `OrderStateMachine` static block |
| `PENDING_NEW` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PENDING_NEW` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PENDING_NEW` | `EXEC_REJECTED` | `REJECTED` | `OrderStateMachine` static block |
| `PENDING_NEW` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `NEW` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `NEW` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `NEW` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `NEW` | `REPLACE_REQUEST` | `PENDING_REPLACE` | `OrderStateMachine` static block |
| `NEW` | `EXEC_EXPIRED` | `EXPIRED` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `REPLACE_REQUEST` | `PENDING_REPLACE` | `OrderStateMachine` static block |
| `PENDING_CANCEL` | `EXEC_CANCELED` | `CANCELED` | `OrderStateMachine` static block |
| `PENDING_CANCEL` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PENDING_CANCEL` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_REPLACED` | `REPLACED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_REJECTED` | `REJECTED` | `OrderStateMachine` static block |
| `REPLACED` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `REPLACED` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `REPLACED` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `REPLACED` | `REPLACE_REQUEST` | `PENDING_REPLACE` | `OrderStateMachine` static block |
| `REPLACED` | `EXEC_EXPIRED` | `EXPIRED` | `OrderStateMachine` static block |
| `ROUTING` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `ParentOrderState.registerTransitions()` |
| `ROUTING` | `EXEC_FILL` | `FILLED` | `ParentOrderState.registerTransitions()` |
| `ROUTING` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `ParentOrderState.registerTransitions()` |
| `ROUTING` | `EXEC_REJECTED` | `REJECTED` | `ParentOrderState.registerTransitions()` |

All other (state, event) combinations return `INVALID_TRANSITION (-1)`. Terminal states (FILLED, CANCELED, REJECTED, EXPIRED) have no outbound transitions.

### 6.3 State invariants

- **PENDING_NEW:** `filledQty == 0`, `leavesQty == qty`, order exists in `OrderBook`
- **NEW:** `filledQty == 0`, `leavesQty == qty`, `orderId` may still be `NULL_ID` until `EXEC_NEW` arrives
- **ROUTING:** `childCount >= 1`, `leavesQty > 0`, `parentOrFirstChildId != 0`, transition was via `OrderStateMachine.transitionToRouting()` which requires prior state == `NEW`
- **PARTIALLY_FILLED:** `filledQty > 0`, `leavesQty > 0`, `filledQty + leavesQty == qty`
- **PENDING_CANCEL:** cancel NOS sent via `FIXMessageEncoder.sendCancelRequest()`; child orders in `PENDING_CANCEL` via `childRegistry.cancelAllChildren()`
- **PENDING_REPLACE:** replace NOS sent via `FIXMessageEncoder.sendCancelReplace()`
- **REPLACED:** new price/qty applied in `handleCancelReplace()` before transition
- **FILLED:** `leavesQty == 0`, `filledQty == qty`; `OrderBook.unindex()` + `freeSlot()` called

---

## Section 7 — Binary Wire Formats

### 7.1 OrderLayout (128-byte order record)

| Offset | Size | Type | Constant | Applies To | Semantics |
|--------|------|------|----------|-----------|-----------|
| 0 | 8 | long | `ACCOUNT_ID_OFFSET` | both | Internal account identifier |
| 8 | 8 | long | `CL_ORD_ID_OFFSET` | both | Client-assigned order ID (FIX tag 11) |
| 16 | 8 | long | `ORDER_ID_OFFSET` | both | Venue-assigned order ID (FIX tag 37); `NULL_ID` until `EXEC_NEW` |
| 24 | 8 | long | `ORIG_CL_ORD_ID_OFFSET` | both | Original ClOrdID for cancel/replace (FIX tag 41) |
| 32 | 8 | long | `SYMBOL_OFFSET` | both | ASCII ticker packed as long, big-endian, left-aligned, space-padded |
| 40 | 8 | long | `PRICE_OFFSET` | both | Limit price × `PRICE_MULTIPLIER (10_000)` |
| 48 | 8 | long | `QTY_OFFSET` | both | Total order quantity |
| 56 | 8 | long | `FILLED_QTY_OFFSET` | both | Cumulative filled quantity (FIX tag 14) |
| 64 | 8 | long | `LEAVES_QTY_OFFSET` | both | Remaining open quantity (FIX tag 151) |
| 72 | 1 | byte | `SIDE_OFFSET` | both | `Side` constants |
| 73 | 1 | byte | `TIME_IN_FORCE_OFFSET` | both | `TimeInForce` constants |
| 74 | 1 | byte | `ORDER_STATE_OFFSET` | both | `OrderState` constants |
| 75 | 1 | byte | `RESERVED_OFFSET` | both | Future flags (zero-fill) |
| 76 | 4 | int | `VENUE_ID_OFFSET` | both | Routing target (maps to Aeron publication stream) |
| 80 | 8 | long | `TRANSACT_TIME_OFFSET` | both | Last state-change timestamp (nanos from `System.nanoTime()`) |
| 88 | 8 | long | `RESERVED1_OFFSET` | both | Reserved (zero-fill) |
| 96 | 8 | long | `RESERVED2_OFFSET` | both | Reserved (zero-fill) |
| 104 | 8 | long | `RESERVED3_OFFSET` | both | Reserved (zero-fill) |
| 112 | 4 | int | `OFFSET_CHILD_COUNT` | parent | Number of live children; 0 on child orders |
| 116 | 4 | int | `OFFSET_NEXT_SIBLING_SLOT` | child | Slot index of next sibling; -1 = end of list |
| 120 | 8 | long | `OFFSET_PARENT_OR_FIRST_CHILD_ID` | both | Parent: orderId of first child (0=none). Child: orderId of its parent |
| **128** | — | — | `BLOCK_LENGTH` / `MESSAGE_SIZE` | — | End of record; static assert enforces this |

`NULL_ID = Long.MIN_VALUE`; `PRICE_MULTIPLIER = 10_000L`

### 7.2 ChildOrderIntentFlyweight (56-byte intent record)

| Offset | Size | Type | Field |
|--------|------|------|-------|
| 0 | 8 | long | `parentOrderId` |
| 8 | 8 | long | `parentClOrdId` |
| 16 | 8 | long | `sliceQty` |
| 24 | 8 | long | `limitPrice` (fixed-point × 10_000) |
| 32 | 4 | int | `venueId` |
| 36 | 1 | byte | `algoType` (1=SOR, 2=ICEBERG, 3=TWAP) |
| 37 | 1 | byte | `sliceIndex` (0-based, wraps at 255) |
| 38 | 2 | short | `_pad` (alignment) |
| 40 | 8 | long | `intentTimestampNanos` |
| 48 | 8 | long | `_reserved` (zero-fill) |
| **56** | — | — | `BLOCK_LENGTH` |

### 7.3 Price encoding

- `PRICE_MULTIPLIER = 10_000L` (from `OrderLayout`)
- `£12.3456` stored as `123456L`
- Maximum representable: `Long.MAX_VALUE / 10_000 = 922_337_203_685_477`
- Why not `double`: non-deterministic across CPU architectures — identical computations produce different results on different Raft replicas
- Why not `BigDecimal`: every operation allocates a new `BigDecimal` on the heap
- Notional check: `price > (maxNotional * PRICE_MULTIPLIER) / qty` (overflow-safe integer division)

### 7.4 Symbol encoding

From `OrderFlyweight.encodeSymbol(String)`:
- Initialized to `0x2020202020202020L` (8 ASCII spaces)
- Left-aligned, right-padded with `0x20`; big-endian byte packing
- Each character packed: `encoded = (encoded & ~(0xFFL << ((7-i)*8))) | (ch << ((7-i)*8))`
- Hot-path comparison: single `long` equality check — no String allocation
- Off-hot-path only: called at startup (`OmsNode.parseSymbols()`) and at order creation setup

### 7.5 FIX Binary inbound format (76 bytes)

From `FIXMessageDecoder` field offsets:

| Offset | Size | Field | FIX Tag |
|--------|------|-------|---------|
| 0 | 1 | `msgType` | MsgType (35); `'D'=NOS, 'F'=Cancel, 'G'=Replace, '8'=ExecReport` |
| 1 | 1 | `side` | 54 |
| 2 | 1 | `timeInForce` | 59 |
| 3 | 1 | `execType` | 150 (ExecReport only) |
| 4 | 8 | `clOrdId` | 11 |
| 12 | 8 | `origClOrdId` | 41 |
| 20 | 8 | `orderId` | 37 |
| 28 | 8 | `accountId` | 1 |
| 36 | 8 | `symbol` | 55 (packed ASCII long) |
| 44 | 8 | `price` | 44 (fixed-point × 10_000) |
| 52 | 8 | `orderQty` | 38 |
| 60 | 8 | `lastQty` | 32 |
| 68 | 8 | `leavesQty` | 151 |
| **76** | — | — | `FIX_BINARY_SIZE` |

### 7.6 Cluster ingress/egress wire format (129 bytes)

```
[0]      msgType  : byte         — ClusterMessageType constant (1=NEW_ORDER, 2=CANCEL_ORDER, 3=REPLACE_ORDER)
[1..128] payload  : OrderLayout  — 128-byte order record (OrderLayout field offsets)
```
Total: `IPC_MESSAGE_SIZE = 129`.

---

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

---

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

---

## Section 10 — Scaling Model

### 10.1 Current topology

Single shard. All symbols processed by one Aeron Cluster (3-node Raft group). `AeronTransport.IPC_CHANNEL = "aeron:ipc"`.

### 10.2 Horizontal scaling path

Symbol-partitioned sharding (not yet implemented). Shard isolation would use separate `aeronDirectoryName` per shard, making stream IDs shard-local.

### 10.3 What changes when a shard is added

`OmsLauncher.java` (new channel wiring), `OmsNode.java` (new env var for shard routing), `AeronTransport` (new stream ID constants). No changes to `OrderStateMachine`, `ValidationEngine`, `ChildOrderRegistry`, or `ChildOrderIntentValidator`.

### 10.4 Cross-shard concerns

Symbol-scoped: fills always arrive for the same symbol as the parent → same shard. Cross-shard risk: account-level notional limits span symbols — not yet implemented.

---

## Section 11 — Design Decision Log

1. **`OrderLayout` as static constants class, not an enum**
   - `byte` constants indexed directly into `TRANSITION_TABLE[state][event]`
   - Enum ordinal() requires virtual call; `enum.values()` creates array copy
   - Reversing: `TRANSITION_TABLE` cannot be indexed by enum without boxing

2. **`AlgoExecutionEngine` as a sealed interface**
   - `permits IcebergAlgoEngine, TwapAlgoEngine` enables exhaustive switch in Java 17+
   - Open interface would prevent JIT devirtualisation; abstract class would add a vtable layer
   - Reversing: new algo types require recompile of all switch sites

3. **Intent-based protocol (algo-sor publishes intents, not child orders)**
   - `algo-sor` is stateless between cycles; all child state in `oms-core`
   - Direct dispatch meant child orders outside Raft replication (lost on failover)
   - Reversing: child fills after failover would be unmatched (`ChildOrderRegistry` empty)

4. **`OrderBook` physically in `oms-codec` module**
   - Allows `oms-harness` to use `OrderBook` without depending on `oms-core`
   - Ideally belongs in `oms-core`; trade-off against harness simplicity
   - Note: represents a module boundary anomaly (package `com.cobain.oms.core` in `oms-codec` directory)

5. **`ChildOrderRegistry.NO_SIBLING = -1` (int)**
   - Distinguishes "end of linked list" from slot 0 (a valid slot)
   - Using slot 0 as sentinel would waste the first slot and cause off-by-one in traversal
   - Reversing: all linked-list traversal in `cancelAllChildren()` and `computeLiveChildQty()` must be updated

6. **`ValidationEngine` uses `LongHashSet` for symbols (not a flat array)**
   - O(1) lookup vs O(n) scan; symbol count can be large (thousands)
   - Flat array is only faster for ≤ ~8 symbols (fits in a cache line)
   - Reversing: `permittedSymbols.contains()` must be replaced

7. **`SnapshotManager` pre-allocates `MAX_SNAPSHOT_BYTES ≈ 9.4 MB` snapshot buffer**
   - Worst-case: `16 + 65_536 × 128 + 65_536 × 16` bytes via `ByteBuffer.allocateDirect()`
   - Allocated once at class construction, not at snapshot time — eliminates GC pressure during failover
   - Reversing: `ByteBuffer.allocateDirect()` at snapshot time would stall the Raft commit thread

8. **`ParentOrderState.registerTransitions()` is explicit, not a static initializer**
   - Caller controls timing (before AgentRunners start); called in both `OmsLauncher.main()` and `OmsClusteredService.onStart()`
   - Static initializer would run at class-load time, possibly before `NUM_STATES = 16` is in effect
   - Reversing: ROUTING state transitions may not be registered before the first order is processed

9. **`childClOrdId = parentOrderId * 10_000L + (sliceIndex & 0xFF)`**
   - Globally unique, self-describing (divide by 10_000 = parentOrderId)
   - FIX-safe (fits in a long); survives roundtrip through FIX connectivity layer
   - Reversing: `ChildOrderRegistry.clOrdIdToOrderId` key collision possible if formula changes

10. **`AlgoSorAgent.FRAGMENT_LIMIT = 10`**
    - Bounds the number of parent orders processed per `doWork()` cycle
    - Prevents one busy period from starving other `AgentRunner` agents on the same thread
    - Reversing: unbounded polling can cause intent publication latency spikes

11. **`OmsClusteredService` polls intents via `pollIntents()` on the Raft commit thread**
    - Intent processing must be deterministic across replicas — running it on the Raft thread achieves this
    - A dedicated intent thread would require synchronization with the Raft state, violating the single-thread invariant
    - Reversing: a separate intent thread mutating `OrderBook` without Raft coordination diverges replicas

---

## Section 12 — Design Analysis Checklist

Before implementing any change, answer every question YES / NO / N/A with a one-sentence justification. A NO answer requires documented exception.

**Zero-GC compliance**
- [ ] Does the change introduce any `new` expression in `onSessionMessage()`, `ValidationEngine.validateNewOrder()`, `ChildOrderIntentValidator.validate()`, `ChildOrderRegistry.createChild()`, or `AlgoSorAgent.onFragment()`?
- [ ] Does the change call `String.format()`, `toString()`, or any varargs method that creates an `Object[]` on the hot path?
- [ ] If new pre-allocated buffers are needed, are they allocated in the relevant constructor and documented in Section 4?

**Golden source integrity**
- [ ] Does the change mutate order state anywhere other than `OmsClusteredService.onSessionMessage()` or `OmsClusteredService.onChildOrderIntent()`?
- [ ] If a new order record type is introduced, is it stored in `ChildOrderRegistry` or `OrderBook` and included in `SnapshotManager.takeSnapshot()`?
- [ ] Does the change preserve the invariant that `AlgoSorAgent` never calls `ChildOrderRegistry.createChild()` or any `OrderBook` mutation?

**State model correctness**
- [ ] If a new state byte constant is added, does it fit within `NUM_STATES = 16`?
- [ ] If a new state is added, is it registered via a `registerTransitions()`-style call in both `OmsLauncher.main()` and `OmsClusteredService.onStart()` before `AgentRunner` starts?
- [ ] If a new state is terminal, is it handled by `OrderState.isTerminal()` (currently `state >= FILLED`, i.e. ≥ 6)?
- [ ] Does the change preserve all invariants in Section 6.3?

**Module boundary compliance**
- [ ] Does the change add a new compile dependency to `algo-sor` on `oms-core`?
- [ ] Does the change add Aeron Cluster to `oms-codec`?
- [ ] Will `./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core` still return no output?
- [ ] Will `./gradlew :oms-codec:dependencies --configuration compileClasspath | grep aeron-cluster` still return no output?

**Sequencing and idempotency**
- [ ] If a new message type is added to `ClusterMessageType`, does it have a corresponding dedup mechanism in `ValidationEngine` or `ChildOrderIntentValidator`?
- [ ] If a new state mutation is added to `onSessionMessage()`, is it safe when replayed by Raft on a follower that already has the state?
- [ ] If a new Aeron stream is added, is its ID declared in both `AeronTransport` and `ClusterMessageType`, and recorded in Section 3.2?

**Failover safety**
- [ ] Is all new durable state serialized in `SnapshotManager.takeSnapshot()`?
- [ ] Is the corresponding deserialization added to `SnapshotManager.handleSnapshotFragment()` and the relevant `reset()` + restore calls?
- [ ] Is `MAX_SNAPSHOT_BYTES` updated if the snapshot size grows?

---

## Section 13 — Glossary

| Term | Definition | First appears in |
|------|------------|-----------------|
| ClOrdID | Client-assigned order identifier (FIX tag 11), stored as hashed long | `OrderLayout.CL_ORD_ID_OFFSET` |
| OrigClOrdID | Original ClOrdID for cancel/replace (FIX tag 41) | `OrderLayout.ORIG_CL_ORD_ID_OFFSET` |
| OrderID | Venue-assigned order identifier (FIX tag 37) | `OrderLayout.ORDER_ID_OFFSET` |
| leavesQty | Remaining open quantity (FIX tag 151) | `OrderLayout.LEAVES_QTY_OFFSET` |
| filledQty | Cumulative filled quantity (FIX tag 14) — code uses `filledQty`, not `cumQty` | `OrderLayout.FILLED_QTY_OFFSET` |
| NOS | New Order Single (FIX MsgType 'D') | `FIXMessageDecoder.MSG_NEW_ORDER_SINGLE` |
| ExecReport | Execution Report (FIX MsgType '8') | `FIXMessageDecoder.MSG_EXEC_REPORT` |
| SOR | Smart Order Router — routes parent order across venues | `SmartOrderRouter` |
| TWAP | Time-Weighted Average Price — time-sliced execution algo | `TwapAlgoEngine` |
| Iceberg | Display-qty execution algo — reveals only a slice at a time | `IcebergAlgoEngine` |
| ChildIntentSink | Callback interface for routing instructions from algo engines | `AlgoExecutionEngine.ChildIntentSink` |
| Flyweight | Zero-allocation wrapper pointing at a buffer region | `OrderFlyweight`, `ChildOrderIntentFlyweight` |
| ChildOrderIntent | Routing instruction from `algo-sor` to `oms-core` | `ChildOrderIntentFlyweight` |
| Golden source | `oms-core` is the single authoritative store for all order state | `CLAUDE.md` |
| ROUTING | Parent-only state (byte=10): accepted, children dispatched | `ParentOrderState.ROUTING` |
| PENDING_NEW | Order created locally, not yet confirmed by venue (byte=0) | `OrderState.PENDING_NEW` |
| fixed-point price | Price stored as `long × PRICE_MULTIPLIER (10_000)` | `OrderLayout.PRICE_MULTIPLIER` |
| PRICE_MULTIPLIER | `10_000L` — scale factor for fixed-point price encoding | `OrderLayout.PRICE_MULTIPLIER` |
| NULL_ID | `Long.MIN_VALUE` — sentinel for unset order IDs | `OrderLayout.NULL_ID` |
| MISSING | `Long.MIN_VALUE` — sentinel for absent map entries | `ChildOrderRegistry` |
| NO_SIBLING | `-1` — end-of-list sentinel in child linked list | `ChildOrderRegistry` |
| BLOCK_LENGTH | 128 bytes — size of one order record | `OrderLayout.BLOCK_LENGTH` |
| MESSAGE_SIZE | 128 bytes — alias for `BLOCK_LENGTH` | `OrderLayout.MESSAGE_SIZE` |
| IPC_MESSAGE_SIZE | 129 bytes — 1-byte type header + 128-byte order payload | `ClusterMessageType.IPC_MESSAGE_SIZE` |
| INVALID_TRANSITION | `-1 (0xFF)` — sentinel for illegal state+event combination | `OrderEvent.INVALID_TRANSITION` |
| NUM_STATES | 16 — size of first dimension of `TRANSITION_TABLE` | `OrderState.NUM_STATES` |
| NUM_EVENTS | 10 — size of second dimension of `TRANSITION_TABLE` | `OrderEvent.NUM_EVENTS` |
| MAX_ORDERS | 65_536 — capacity of `OrderBook` | `OrderBook.MAX_ORDERS` |
| AgentRunner | Agrona single-threaded execution loop for `AlgoSorAgent` | `OmsLauncher` |
| Raft | Consensus protocol used by Aeron Cluster for log replication | `OmsNode`, `ConsensusModule` |
| ClusteredService | Aeron Cluster interface implemented by `OmsClusteredService` | `OmsClusteredService` |
| UnsafeBuffer | Agrona off-heap or on-heap buffer with direct memory access | `OrderBook`, `ChildOrderRegistry` |
| zero-GC | No object allocation on the critical execution path | `CLAUDE.md` Engineering Rules |
| hot path | Code executed on every message: `onSessionMessage`, `onFragment`, `validate` | `CLAUDE.md` |
| STREAM_OMS_TO_ALGO | 30 — `oms-core` → `algo-sor` parent order stream | `AeronTransport` / `ClusterMessageType` |
| STREAM_CHILD_INTENTS | 12 — `algo-sor` → `oms-core` intent stream | `ClusterMessageType` |
| STREAM_OMS_TO_FIX | 10 — `oms-core` → FIX connectivity engine | `AeronTransport` |
| STREAM_FIX_TO_OMS | 11 — FIX connectivity engine → `oms-core` | `AeronTransport` |
| FRAGMENT_LIMIT | 10 — max parent orders polled per `AlgoSorAgent.doWork()` cycle | `AlgoSorAgent` |
| INTENT_FRAGMENT_LIMIT | 20 — max intents drained per `pollIntents()` call | `OmsClusteredService` |
