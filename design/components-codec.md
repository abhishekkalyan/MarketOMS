# Component Contracts — oms-codec
<!-- Load when: touching any oms-codec class listed below, or introducing a
     new codec/wire-format component. Update the entry after any contract change. -->

## oms-codec Components

Components in this file compile as part of `oms-codec`. They are pure
wire-format and constant classes — no Aeron Cluster dependency, no runtime state.

---

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

#### FIXMessageDecoder

**Class:** `com.cobain.oms.fix.FIXMessageDecoder`
**Module:** `oms-codec`
**Thread model:** static utility class — called on the Raft commit thread from `OmsClusteredService.handleExecReport()`
**Zero-allocation contract:** All methods are static; each method is a sequence of `buffer.getLong/getByte` + flyweight setter calls; `mapExecTypeToEvent()` is a single `switch` tableswitch — zero allocation
**Inputs:** `DirectBuffer src` at `srcOffset` containing a 76-byte FIX Binary message; writable `OrderFlyweight target`
**Outputs:** fields written directly into the target flyweight; `decodeExecReport()` returns the `execType` byte; `getLastQty()` returns `long` fill qty
**Invariants:** `FIX_BINARY_SIZE = 76`; used ONLY for the FIX bridge → oms-core IPC path (stream 11); must NOT be used to decode cluster ingress messages (those use `ClusterMessageType` framing)
**Failure mode:** Decoding a cluster ingress message with `FIXMessageDecoder` interprets the `ClusterMessageType` byte (1/2/3) as an ASCII MsgType — silent data corruption
