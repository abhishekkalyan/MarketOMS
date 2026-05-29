# Claude Code Task: Market OMS — Intent-Driven Child Order Architecture
# Version 2 — supersedes CLAUDE_CODE_PROMPT.md

---

## Context and Prior Work

You are continuing work on a zero-GC, mechanically sympathetic sell-side OMS in Java 21+
using Aeron Cluster, Agrona, and Aeron IPC. The previous task (V1 prompt) refactored the
codebase into a Gradle multi-module monorepo with this structure:

```
market-oms/
  oms-codec/      codec/, fix/         — shared binary contract, Agrona only
  oms-core/       statemachine/, validation/, cluster/, common/
  algo-sor/       algo/, algoagent/
  oms-launcher/   launcher/
```

That structure is CORRECT and must be preserved. This task changes the BEHAVIOUR of the
system, not its module boundaries.

---

## Architectural Mandate

### The Problem with the Current Design

`algo-sor` currently receives an accepted parent order and directly dispatches child order
flyweights to the FIX bridge via Aeron IPC. This means:

1. Child orders are BORN in `algo-sor` — `oms-core` never knows they exist
2. Child order state (venue, qty, fill progress) lives only in `algo-sor` memory
3. If `algo-sor` crashes, all in-flight child state is lost
4. If the parent is cancelled, `algo-sor` keeps dispatching slices because it receives
   no signal
5. `oms-core`'s `OrderBook` shows a parent with `leavesQty > 0` but no record of WHY

### The Mandated Fix

`oms-core` is the SINGLE golden source of all order state — parent and child.

The new contract is:

- `algo-sor` is a PURE COMPUTATION ENGINE. It receives parent orders, runs routing/slicing
  arithmetic, and publishes `ChildOrderIntent` messages back to `oms-core`.
  It creates NO order state. It touches NO order book. It dispatches NOTHING to the FIX bridge.

- `oms-core` receives each `ChildOrderIntent`, VALIDATES it against the current parent state,
  CREATES the child order as a first-class entry in a new `ChildOrderRegistry`,
  LINKS the child to its parent, and THEN dispatches the child NOS to the FIX bridge.

- Every state transition — on both parent and child — goes through `OrderStateMachine`
  inside `oms-core`. No state is mutated anywhere else.

```
BEFORE (broken):
  oms-core ──[parent order]──► algo-sor ──[child NOS]──► FIX bridge ──► Venue
                                              ↑
                                    child born here, oms-core blind

AFTER (correct):
  oms-core ──[parent order]──► algo-sor ──[ChildOrderIntent]──► oms-core
                                                                     │
                                                         validates intent
                                                         creates child in ChildOrderRegistry
                                                         links child → parent in OrderBook
                                                         transitions parent to ROUTING state
                                                             │
                                                             └──[child NOS]──► FIX bridge ──► Venue
```

All Aeron Cluster replication, snapshotting, and recovery operates only on `oms-core` state.
`algo-sor` is stateless between its AgentRunner work cycles and recovers automatically on restart
by re-subscribing to the parent order stream.

---

## Complete List of Changes Required

### A — New files to create

| File | Module | Purpose |
|------|--------|---------|
| `oms-codec/.../codec/ChildOrderIntentFlyweight.java` | `oms-codec` | Wire format for algo-sor → oms-core routing instructions |
| `oms-core/.../common/ChildOrderRegistry.java` | `oms-core` | Golden-source store for all child orders |
| `oms-core/.../statemachine/ParentOrderState.java` | `oms-core` | Extended parent state constants including ROUTING |
| `oms-core/.../validation/ChildOrderIntentValidator.java` | `oms-core` | Validates intents before child creation |

### B — Files to modify

| File | Module | Change |
|------|--------|--------|
| `OrderFields.java` | `oms-codec` | Add 4 new field offsets in reserved region for parent-child linkage |
| `ClusterMessageType.java` | `oms-codec` | Add `CHILD_ORDER_INTENT` message type constant |
| `AlgoSorAgent.java` | `algo-sor` | Publish `ChildOrderIntent` instead of child NOS flyweights |
| `SmartOrderRouter.java` | `algo-sor` | Return intent records instead of dispatching to FIX |
| `IcebergAlgoEngine.java` | `algo-sor` | Return intent records instead of dispatching to FIX |
| `TwapAlgoEngine.java` | `algo-sor` | Return intent records instead of dispatching to FIX |
| `OmsClusteredService.java` | `oms-core` | Handle `CHILD_ORDER_INTENT` messages; create children; dispatch to FIX |
| `AeronEgressPublisher.java` | `oms-core` | Add `publishChildOrderIntent()` for algo-sor outbound |
| `OrderStateMachine.java` | `oms-core` | Add `transitionToRouting()` and child-fill aggregation |
| `OmsLauncher.java` | `oms-launcher` | Add Stream ID 12 for intent return channel |

### C — Files that do NOT change

`OrderFlyweight.java`, `OrderState.java` (base constants), `OrderValidationEngine.java`,
`SymbolUniverse.java`, `ValidationResult.java`, `SequenceTracker.java`,
`FixFields.java`, `FixToFlyweightTranslator.java`, `OrderBook.java`

---

## Step-by-Step Instructions

### Step 1 — Explore First (use plan mode)

Read every file listed in sections A and B above in full before writing a single line of code.
Then produce a written plan with:
1. The exact new field offsets in `OrderFields.java` (verify the 16 reserved bytes at
   offset 112–127 are available and the layout stays at 128 bytes total)
2. The `ChildOrderIntentFlyweight` binary layout with all offsets and sizes summing to ≤ 64 bytes
3. A precise description of what `OmsClusteredService.onChildOrderIntent()` does, step by step,
   before writing any code for it
4. The updated Aeron IPC channel map (stream IDs 10, 11, 12 and their directions)
5. Any risk of the shared `sharedFlyweight` in `ChildOrderRegistry` being clobbered during
   a fill + cancel arriving in the same work cycle (and how you will guard against it)

Do not edit any files until this plan is complete.

---

### Step 2 — Update `OrderFields.java` (oms-codec)

Add the following four fields using the 16 reserved bytes at offsets 112–127.
The block length stays at 128 bytes — do NOT increase it.

```
Offset  Size  Type  Field name                  Semantics
------  ----  ----  --------------------------  -----------------------------------------
 112     4    int   CHILD_COUNT                 Parent only: number of live (non-terminal)
                                                children. 0 on child orders.
 116     4    int   NEXT_SIBLING_SLOT           Child only: OrderBook slot index of the
                                                next sibling in the parent's child list.
                                                0 = end of list / not used on parent.
 120     8    long  PARENT_OR_FIRST_CHILD_ID    Dual-purpose:
                                                • On a PARENT order: orderId of the first
                                                  child (linked list head). 0 = no children.
                                                • On a CHILD order: orderId of its parent.
```

Name the constants:
```java
public static final int OFFSET_CHILD_COUNT            = 112;
public static final int OFFSET_NEXT_SIBLING_SLOT      = 116;
public static final int OFFSET_PARENT_OR_FIRST_CHILD_ID = 120;
```

Verify: 120 + 8 = 128. Exactly fills the block. Assert this in a static initializer:
```java
static {
    assert OFFSET_PARENT_OR_FIRST_CHILD_ID + Long.BYTES == BLOCK_LENGTH
        : "OrderFields layout overflow";
}
```

---

### Step 3 — Add `CHILD_ORDER_INTENT` to `ClusterMessageType.java` (oms-codec)

Add one constant in the "Internal Cluster Control" section:
```java
/** Routing instruction published by algo-sor → oms-core over Aeron IPC stream 12. */
public static final byte CHILD_ORDER_INTENT = 40;
```

Also add the stream ID constants used by the launcher:
```java
public static final int STREAM_PARENT_ORDERS  = 10; // oms-core → algo-sor
public static final int STREAM_CHILD_INTENTS  = 12; // algo-sor → oms-core (NEW)
```

Stream 11 (child NOS from algo-sor to FIX bridge) is REMOVED. `algo-sor` no longer sends
anything to the FIX bridge.

---

### Step 4 — Create `ChildOrderIntentFlyweight.java` (oms-codec)

Package: `com.sellside.oms.codec`

Binary layout — 56 bytes total, fits in one cache line:

```
Offset  Size  Type  Field
------  ----  ----  --------------------------
  0      8    long  parentOrderId
  8      8    long  parentClOrdId
 16      8    long  sliceQty
 24      8    long  limitPrice         (fixed-point * PRICE_SCALE)
 32      4    int   venueId
 36      1    byte  algoType           (1=SOR, 2=ICEBERG, 3=TWAP)
 37      1    byte  sliceIndex         (0-based, wraps at 255)
 38      2    short _pad
 40      8    long  intentTimestampNanos
 48      8    long  _reserved
```

Constants:
```java
public static final byte ALGO_SOR     = 1;
public static final byte ALGO_ICEBERG = 2;
public static final byte ALGO_TWAP    = 3;
public static final int  BLOCK_LENGTH = 56;
```

Implement:
- `wrap(MutableDirectBuffer, int)` and `wrapReadOnly(DirectBuffer, int)`
- Full set of getters and setters — all primitives, no allocation
- A `copyTo(MutableDirectBuffer dest, int destOffset)` method

ZERO-ALLOCATION contract: no method allocates any object. No toString() that returns
a new String is called on the hot path (add one for logging, annotated with a comment).

---

### Step 5 — Update `AlgoExecutionEngine` interface (algo-sor)

The `ChildOrderSink` callback currently receives a mutated `OrderFlyweight`. Change it to
receive a `ChildOrderIntentFlyweight` instead:

```java
@FunctionalInterface
public interface ChildIntentSink {
    /**
     * Called for each computed child slice. The flyweight is pre-allocated
     * and MUST NOT be retained past this call.
     *
     * @param intent  pre-allocated intent flyweight (do not retain reference)
     */
    void onIntent(ChildOrderIntentFlyweight intent);
}
```

Remove the old `ChildOrderSink` interface. Update `onSlice()` signature accordingly:
```java
long onSlice(OrderFlyweight parent, ChildIntentSink sink, long nowNanos);
```

---

### Step 6 — Update `IcebergAlgoEngine.java` and `TwapAlgoEngine.java` (algo-sor)

Both engines must:
1. Remove the pre-allocated child `UnsafeBuffer` and child `OrderFlyweight`
2. Add a pre-allocated `ChildOrderIntentFlyweight` wrapping a pre-allocated
   56-byte `UnsafeBuffer` (one per engine instance, allocated in the constructor)
3. In `onSlice()`, populate the intent flyweight fields and call `sink.onIntent(intent)`
   instead of `sink.onChildOrder(childFlyweight, ...)`
4. The intent must set: `parentOrderId`, `parentClOrdId`, `sliceQty`, `limitPrice`,
   `venueId`, `algoType`, `sliceIndex`, `intentTimestampNanos = nowNanos`

ZERO-ALLOCATION: the pre-allocated intent flyweight is reused every slice. The sink
implementation in `AlgoSorAgent` must publish and release before returning.

---

### Step 7 — Update `SmartOrderRouter.java` (algo-sor)

Change `route()` signature:
```java
public int route(
    OrderFlyweight parent,
    int[]  venueIds,
    long[] venuePrices,
    long[] venueQtys,
    int    venueCount,
    AlgoExecutionEngine.ChildIntentSink sink   // ← was ChildOrderSink
)
```

In the greedy sweep loop:
- Populate the pre-allocated `ChildOrderIntentFlyweight` (one per `SmartOrderRouter`
  instance, allocated in the constructor wrapping a 56-byte `UnsafeBuffer`)
- Set `algoType = ChildOrderIntentFlyweight.ALGO_SOR`
- Call `sink.onIntent(intentFlyweight)` instead of `sink.onChildOrder(...)`
- Do NOT write to any FIX buffer. Do NOT hold a reference to any publication.

`SmartOrderRouter` now has ZERO Aeron dependencies. Remove any `ExclusivePublication`
constructor parameters.

---

### Step 8 — Update `AlgoSorAgent.java` (algo-sor)

**Constructor change** — remove `childOrderPub` (Aeron publication to FIX bridge).
Add `intentPub` (Aeron publication to oms-core on stream 12):

```java
public AlgoSorAgent(
    Subscription             parentOrderSub,  // stream 10: oms-core → algo-sor
    ExclusivePublication     intentPub,       // stream 12: algo-sor → oms-core
    IcebergAlgoEngine        icebergEngine,
    TwapAlgoEngine           twapEngine,
    SmartOrderRouter         sor
)
```

**Pre-allocated fields** (all in constructor, zero allocation on hot path):
```java
// Inbound parent order view
private final OrderFlyweight parentView = new OrderFlyweight();

// Outbound intent — shared across all engines via the sink below
// One UnsafeBuffer for the Aeron offer; the engine writes its own pre-alloc'd buffer,
// this one is used for the publication offer call only.
private final UnsafeBuffer   intentOfferBuffer;  // 8-byte header + 56-byte intent = 64 bytes
private final ChildOrderIntentFlyweight intentReadView = new ChildOrderIntentFlyweight();

// Market depth arrays (mock — in production these are updated by a market data handler)
private final int[]  venueIds    = new int[SmartOrderRouter.MAX_VENUES];
private final long[] venuePrices = new long[SmartOrderRouter.MAX_VENUES];
private final long[] venueQtys   = new long[SmartOrderRouter.MAX_VENUES];
```

**`onFragment()` implementation:**

```java
@Override
public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    final byte msgType = buffer.getByte(offset + ClusterMessageType.OFFSET_MSG_TYPE);

    if (msgType == ClusterMessageType.NEW_ORDER) {
        // Wrap parent view over payload — zero copy
        parentView.wrapReadOnly(buffer, offset + ClusterMessageType.OFFSET_PAYLOAD);

        // Populate mock market depth (in production: read from market data cache)
        loadMockMarketDepth(parentView);

        // Run SOR — produces ChildOrderIntent messages via the sink below
        sor.route(parentView, venueIds, venuePrices, venueQtys,
                  activeVenueCount, this::publishIntent);

        // For algo orders, the AlgoSorAgent would also call icebergEngine or
        // twapEngine.onSlice() here based on a strategy byte on the parent order.
    }
    // All other message types ignored by algo-sor
}
```

**`publishIntent()` — the ChildIntentSink implementation:**

```java
// This is the ChildIntentSink lambda target — called once per child slice
// HOT PATH — ZERO ALLOCATIONS: copies 56 bytes from engine's pre-alloc'd
// intent buffer into the offer buffer, then calls Aeron offer.
private void publishIntent(final ChildOrderIntentFlyweight intent) {
    // Write message type header into the offer buffer
    intentOfferBuffer.putByte(0, ClusterMessageType.CHILD_ORDER_INTENT);
    intentOfferBuffer.putByte(1, ClusterMessageType.PROTOCOL_VERSION);

    // Copy the 56-byte intent record into the offer buffer at offset 8
    intent.copyTo(intentOfferBuffer, 8);

    // Offer to oms-core — spin on back-pressure
    final int totalLength = 8 + ChildOrderIntentFlyweight.BLOCK_LENGTH; // 64 bytes
    while (true) {
        final long result = intentPub.offer(intentOfferBuffer, 0, totalLength);
        if (result > 0L) return;
        if (result == Publication.CLOSED) return;
        Thread.onSpinWait();
    }
}
```

**`MAX_VENUES` visibility** — make `SmartOrderRouter.MAX_VENUES` package-visible or
add a public getter so `AlgoSorAgent` can size its arrays correctly.

---

### Step 9 — Create `ChildOrderIntentValidator.java` (oms-core)

Package: `com.sellside.oms.validation`

This validator runs inside `oms-core` when a `CHILD_ORDER_INTENT` arrives, BEFORE the
child order is created. It is the state-model gatekeeper.

```java
package com.sellside.oms.validation;

import com.sellside.oms.codec.ChildOrderIntentFlyweight;
import com.sellside.oms.codec.OrderFields;
import com.sellside.oms.codec.OrderFlyweight;
import com.sellside.oms.statemachine.OrderState;

/**
 * ChildOrderIntentValidator — validates algo-sor routing instructions
 * against live parent order state before oms-core creates a child order.
 *
 * ZERO-ALLOCATION: all checks operate on primitive fields. Returns a byte
 * result code — never throws, never allocates.
 *
 * STATE MODEL RULES enforced here:
 *   1. Parent must exist in OrderBook
 *   2. Parent must be in a routable state: NEW or ROUTING or PARTIALLY_FILLED
 *      (PENDING_CANCEL, PENDING_REPLACE, FILLED, CANCELED, REPLACED, REJECTED
 *       are all non-routable — intent is silently dropped)
 *   3. sliceQty must be > 0
 *   4. sliceQty must be ≤ parent.leavesQty
 *      (algo-sor computed this against a stale depth snapshot; by the time
 *       the intent arrives the parent may have partial fills reducing leavesQty)
 *   5. limitPrice must be > 0
 *   6. venueId must be > 0
 *   7. Sum of all live child leavesQty + sliceQty must not exceed parent leavesQty
 *      (over-allocation guard: prevents algo-sor from dispatching more qty than remains)
 */
public final class ChildOrderIntentValidator {

    // Result codes — extend ValidationResult with child-specific codes
    public static final byte PASS                        = 0;
    public static final byte REJECT_PARENT_NOT_FOUND     = 20;
    public static final byte REJECT_PARENT_NOT_ROUTABLE  = 21;
    public static final byte REJECT_INVALID_SLICE_QTY    = 22;
    public static final byte REJECT_SLICE_EXCEEDS_LEAVES = 23;
    public static final byte REJECT_INVALID_PRICE        = 24;
    public static final byte REJECT_INVALID_VENUE        = 25;
    public static final byte REJECT_QTY_OVERALLOCATION   = 26;

    public ChildOrderIntentValidator() {}

    /**
     * Validate a ChildOrderIntent against the current parent order state.
     *
     * HOT PATH — ZERO ALLOCATIONS.
     *
     * @param intent         the inbound intent from algo-sor
     * @param parent         the parent order flyweight from OrderBook (null if not found)
     * @param liveChildQty   sum of leavesQty across all currently live children of this parent
     *                       (maintained by ChildOrderRegistry, passed as a primitive long)
     * @return               PASS or a REJECT_* code
     */
    public byte validate(final ChildOrderIntentFlyweight intent,
                         final OrderFlyweight parent,
                         final long liveChildQty) {

        // Rule 1: parent must exist
        if (parent == null) {
            return REJECT_PARENT_NOT_FOUND;
        }

        // Rule 2: parent must be in a routable state
        // Only NEW, ROUTING (new state introduced below), and PARTIALLY_FILLED
        // are valid states from which new children can be dispatched.
        final byte parentState = parent.getOrderState();
        if (!isRoutableState(parentState)) {
            return REJECT_PARENT_NOT_ROUTABLE;
        }

        // Rule 3: sliceQty > 0
        final long sliceQty = intent.getSliceQty();
        if (sliceQty <= 0L) {
            return REJECT_INVALID_SLICE_QTY;
        }

        // Rule 4: sliceQty ≤ parent.leavesQty
        final long parentLeaves = parent.getLeavesQty();
        if (sliceQty > parentLeaves) {
            return REJECT_SLICE_EXCEEDS_LEAVES;
        }

        // Rule 5: limitPrice > 0
        if (intent.getLimitPrice() <= 0L) {
            return REJECT_INVALID_PRICE;
        }

        // Rule 6: venueId > 0
        if (intent.getVenueId() <= 0) {
            return REJECT_INVALID_VENUE;
        }

        // Rule 7: over-allocation guard
        // liveChildQty is the total qty already committed to live children.
        // Adding sliceQty must not exceed the parent's remaining open quantity.
        if (liveChildQty + sliceQty > parentLeaves) {
            return REJECT_QTY_OVERALLOCATION;
        }

        return PASS;
    }

    /**
     * Returns true if the parent state permits new child orders to be routed.
     * Pure byte comparison — no allocation, no boxing.
     */
    public static boolean isRoutableState(final byte state) {
        return state == OrderState.NEW
            || state == ParentOrderState.ROUTING        // introduced below
            || state == OrderState.PARTIALLY_FILLED;
    }

    /** Off-hot-path description. */
    public static String describe(final byte result) {
        return switch (result) {
            case PASS                        -> "PASS";
            case REJECT_PARENT_NOT_FOUND     -> "PARENT_NOT_FOUND";
            case REJECT_PARENT_NOT_ROUTABLE  -> "PARENT_NOT_ROUTABLE";
            case REJECT_INVALID_SLICE_QTY    -> "INVALID_SLICE_QTY";
            case REJECT_SLICE_EXCEEDS_LEAVES -> "SLICE_EXCEEDS_LEAVES";
            case REJECT_INVALID_PRICE        -> "INVALID_PRICE";
            case REJECT_INVALID_VENUE        -> "INVALID_VENUE";
            case REJECT_QTY_OVERALLOCATION   -> "QTY_OVERALLOCATION";
            default                          -> "UNKNOWN(" + (result & 0xFF) + ")";
        };
    }
}
```

---

### Step 10 — Create `ParentOrderState.java` (oms-core)

Package: `com.sellside.oms.statemachine`

The base `OrderState` byte constants (NEW=0 through REJECTED=7) are used by both parent
and child orders. The `ROUTING` state is parent-only and must not conflict.

```java
package com.sellside.oms.statemachine;

/**
 * ParentOrderState — additional state constants that apply only to parent orders.
 *
 * ZERO-ALLOCATION: plain byte constants. No enum. No boxing.
 *
 * These constants EXTEND OrderState and must use values not already taken.
 * OrderState occupies 0–7. Start parent-only states at 10 to leave gap
 * for future base state additions.
 *
 * ROUTING state lifecycle:
 *   NEW → ROUTING           : first ChildOrderIntent accepted by oms-core
 *   ROUTING → PARTIALLY_FILLED : first child fill aggregated into parent
 *   ROUTING → CANCELED      : all children canceled before any fill
 *   ROUTING → FILLED        : all qty filled via children (edge case: one large child)
 *
 * The ROUTING state is essential for the state-model validation in
 * ChildOrderIntentValidator — it distinguishes "order accepted, no children yet"
 * (NEW) from "order accepted, children dispatched" (ROUTING) from
 * "order partially executed" (PARTIALLY_FILLED).
 */
public final class ParentOrderState {

    private ParentOrderState() {}

    /**
     * Parent order has been accepted and one or more child orders have been
     * created and dispatched to venues. Awaiting fills or cancels.
     *
     * Valid incoming events in ROUTING state:
     *   EVT_PARTIAL_FILL → PARTIALLY_FILLED  (first fill arrives)
     *   EVT_FILL         → FILLED            (full qty filled via children)
     *   EVT_CANCEL_REQ   → PENDING_CANCEL    (client cancel request)
     *   EVT_REJECT       → REJECTED          (venue mass-cancel / bust)
     */
    public static final byte ROUTING = 10;

    /**
     * Add ROUTING to the OrderState transition table.
     * Called once at startup from OmsClusteredService or a static initialiser.
     *
     * ZERO-ALLOCATION: mutates the existing static transition table array.
     * Must be called BEFORE any orders are processed.
     */
    public static void registerTransitions() {
        // From ROUTING: partial fill → PARTIALLY_FILLED
        OrderState.TRANSITION_TABLE[ROUTING & 0xFF][OrderState.EVT_PARTIAL_FILL] =
                OrderState.PARTIALLY_FILLED;
        // From ROUTING: full fill → FILLED
        OrderState.TRANSITION_TABLE[ROUTING & 0xFF][OrderState.EVT_FILL] =
                OrderState.FILLED;
        // From ROUTING: cancel request → PENDING_CANCEL
        OrderState.TRANSITION_TABLE[ROUTING & 0xFF][OrderState.EVT_CANCEL_REQ] =
                OrderState.PENDING_CANCEL;
        // From ROUTING: reject → REJECTED
        OrderState.TRANSITION_TABLE[ROUTING & 0xFF][OrderState.EVT_REJECT] =
                OrderState.REJECTED;
    }

    /** Returns true if this state is the ROUTING parent-specific state. */
    public static boolean isRouting(final byte state) {
        return state == ROUTING;
    }
}
```

**Also update `OrderState.java`** to increase `NUM_STATES` from 8 to 16 so the transition
table has room for the parent-only state at index 10:

```java
private static final int NUM_STATES = 16;  // was 8
```

---

### Step 11 — Create `ChildOrderRegistry.java` (oms-core)

Package: `com.sellside.oms.common`

Implement the full child order store as specified below. Every method on the hot path
must be zero-allocation. The registry is the ONLY place child orders live.

**Fields** (all pre-allocated in constructor):
```java
private final UnsafeBuffer   store;           // off-heap, capacity * BLOCK_LENGTH bytes
private final int            capacity;
private final Long2LongHashMap orderIdToSlot; // childOrderId → slot index
private final Long2LongHashMap clOrdIdToOrderId; // childClOrdId → childOrderId
private final int[]          freeSlots;       // free slot stack
private int                  freeTop;
private final OrderFlyweight sharedFlyweight; // shared — callers must not retain
private final OrderFlyweight secondFlyweight; // second shared view for concurrent reads
                                              // in the same work cycle (fill + parent lookup)
```

Use TWO pre-allocated flyweights to avoid clobbering. When `applyFillAndAggregate()`
needs both the child and the parent simultaneously, use `sharedFlyweight` for the child
and `secondFlyweight` for the parent.

**Methods to implement:**

```java
/**
 * Create a child order from a validated ChildOrderIntent.
 * Links the child into the parent's linked list (prepend, O(1)).
 * Called from OmsClusteredService AFTER ChildOrderIntentValidator.validate() == PASS.
 *
 * Returns the child flyweight (sharedFlyweight) pointing at the new slot.
 * Returns null if registry is full.
 */
public OrderFlyweight createChild(
    ChildOrderIntentFlyweight intent,
    OrderFlyweight parent,
    long childOrderId);

/**
 * Apply a fill to a child order and aggregate the delta into the parent.
 *
 * Steps (all zero-allocation):
 *   1. Locate child by childClOrdId → childOrderId → slot
 *   2. Apply fillQty to child (applyFill on child buffer)
 *   3. If child.leavesQty == 0: set child state = FILLED, decrement parent.childCount
 *   4. Read parentOrderId from child buffer (OFFSET_PARENT_OR_FIRST_CHILD_ID)
 *   5. Locate parent via parentBook.get(parentOrderId)
 *   6. Apply fillQty to parent (applyFill on parent buffer)
 *   7. If parent.leavesQty == 0: set parent state = FILLED
 *      else if parent.cumQty > 0: set parent state = PARTIALLY_FILLED
 *   8. Update parent.transactTime
 *   9. Return parent flyweight (secondFlyweight) for exec report publication
 *
 * Returns null if child not found (unknown clOrdId — log and discard).
 */
public OrderFlyweight applyFillAndAggregate(
    long childClOrdId,
    long fillQty,
    long fillPrice,
    OrderBook parentBook);

/**
 * Transition all live children of a parent to PENDING_CANCEL.
 * Walk the parent's child linked list via OFFSET_PARENT_OR_FIRST_CHILD_ID
 * and OFFSET_NEXT_SIBLING_SLOT. For each non-terminal child, set state =
 * PENDING_CANCEL and call cancelSink.onCancelRequest(childFlyweight).
 *
 * Returns count of children given cancel requests.
 */
public int cancelAllChildren(OrderFlyweight parent, CancelRequestSink cancelSink);

/**
 * Compute the total leavesQty across all live (non-terminal) children of a parent.
 * Used by ChildOrderIntentValidator for the over-allocation guard.
 * Walk the linked list — O(n) where n = live child count (typically ≤ 20).
 */
public long computeLiveChildQty(OrderFlyweight parent);

/** Lookup by orderId. Returns sharedFlyweight or null. */
public OrderFlyweight getByOrderId(long childOrderId);

/** Lookup by clOrdId. Returns sharedFlyweight or null. */
public OrderFlyweight getByClOrdId(long childClOrdId);

/** Remove a terminal child from indexes and return its slot to the free list. */
public void removeTerminal(long childOrderId);

/** Snapshot: write all child records to a buffer for Aeron Cluster snapshot. */
public int snapshot(MutableDirectBuffer dest, int destOffset);

/** Restore: read all child records from a snapshot buffer. */
public int restore(DirectBuffer src, int srcOffset, OrderBook parentBook);

@FunctionalInterface
public interface CancelRequestSink {
    void onCancelRequest(OrderFlyweight childInPendingCancelState);
}
```

**Child clOrdId generation rule:**
```java
// Inside createChild():
// Child clOrdId = parentOrderId * 10_000L + (sliceIndex & 0xFF)
// This makes child clOrdIds:
//   • Globally unique (parentOrderId uniqueness + per-parent slice index uniqueness)
//   • Self-describing: divide by 10_000 to get the parentOrderId
//   • FIX-safe: fits in a long, round-trips through the FIX connectivity layer
final long childClOrdId = intent.getParentOrderId() * 10_000L
                        + (intent.getSliceIndex() & 0xFF);
```

---

### Step 12 — Update `OmsClusteredService.java` (oms-core)

**Add fields** (pre-allocated in constructor or `onStart()`):
```java
private final ChildOrderRegistry        childRegistry;
private final ChildOrderIntentValidator intentValidator;
private final ChildOrderIntentFlyweight intentView;   // wraps inbound buffer
private long nextOrderId = 1L;                        // shared counter: parents + children
```

**Add `onChildOrderIntent()` private method:**

```java
/**
 * Handle a CHILD_ORDER_INTENT arriving from algo-sor over Aeron IPC stream 12.
 *
 * This method is the gateway between algo-sor's computation and oms-core's
 * golden-source state. It runs entirely within the single-threaded Aeron
 * Cluster commit path — deterministic, replicated, recoverable.
 *
 * FLOW:
 *   1. Wrap intent view over inbound buffer (zero copy)
 *   2. Look up parent in OrderBook
 *   3. Compute liveChildQty from ChildOrderRegistry
 *   4. Run ChildOrderIntentValidator — if REJECT, log and return (no child created)
 *   5. Assign childOrderId from nextOrderId counter
 *   6. Create child in ChildOrderRegistry (links to parent, updates parent.childCount)
 *   7. If parent state is NEW: transition parent → ROUTING via OrderStateMachine
 *   8. Dispatch child NOS to FIX bridge via AeronEgressPublisher.dispatchChildOrderToFix()
 *
 * ZERO-ALLOCATION: all operations on pre-allocated flyweights and primitive fields.
 * DETERMINISM: nextOrderId is monotonically incremented here and only here.
 *              All cluster nodes see identical increments in identical order.
 */
private void onChildOrderIntent(final DirectBuffer buffer, final int offset) {
    // Step 1: wrap intent view — zero copy, no allocation
    intentView.wrapReadOnly(buffer, offset + ClusterMessageType.HEADER_LENGTH);

    // Step 2: look up parent
    final long parentOrderId = intentView.getParentOrderId();
    final OrderFlyweight parent = orderBook.get(parentOrderId);

    // Step 3: compute live child qty for over-allocation guard
    final long liveChildQty = (parent != null)
            ? childRegistry.computeLiveChildQty(parent)
            : 0L;

    // Step 4: validate — fast-fail on any rule violation
    final byte validationResult = intentValidator.validate(intentView, parent, liveChildQty);
    if (validationResult != ChildOrderIntentValidator.PASS) {
        // Log rejection off hot-path (use a pre-allocated error buffer, not String.format)
        // In production: publish a INTENT_REJECT event for ops monitoring
        return;
    }

    // Step 5: assign system childOrderId — single-threaded counter, no CAS needed
    final long childOrderId = nextOrderId++;

    // Step 6: create child — born here, in oms-core, in the replicated log
    // parent is non-null (validated above)
    final OrderFlyweight child = childRegistry.createChild(intentView, parent, childOrderId);
    if (child == null) {
        // Registry capacity exhausted — this is an ops alert condition
        return;
    }

    // Step 7: transition parent from NEW → ROUTING on first child dispatch
    // ROUTING is a parent-only state indicating children are live at venues.
    // Subsequent children leave the parent in ROUTING (re-entering is idempotent).
    if (parent.getOrderState() == OrderState.NEW) {
        stateMachine.transitionToRouting(parent); // new method on OrderStateMachine
    }

    // Step 8: dispatch child to FIX bridge — child is durably recorded BEFORE this line
    // If this process crashes here, the child exists in the next snapshot and can be
    // reconciled with the venue on reconnect via a FIX OrderStatusRequest (35=H).
    egressPublisher.dispatchChildOrderToFix(child);
}
```

**Update `onSessionMessage()` dispatch switch** to route `CHILD_ORDER_INTENT` messages:

```java
final byte msgType = buffer.getByte(offset + ClusterMessageType.OFFSET_MSG_TYPE);
switch (msgType) {
    case ClusterMessageType.NEW_ORDER        -> onNewOrder(buffer, offset, session, position);
    case ClusterMessageType.CANCEL_ORDER     -> onCancelOrder(buffer, offset, session, position);
    case ClusterMessageType.REPLACE_ORDER    -> onReplaceOrder(buffer, offset, session, position);
    case ClusterMessageType.CHILD_ORDER_INTENT -> onChildOrderIntent(buffer, offset);
    // CHILD_ORDER_INTENT has no ClientSession — it comes from algo-sor via IPC,
    // not from a client. The session parameter is null for these messages.
    default -> { /* unknown message type — log and discard */ }
}
```

**Update `onCancelOrder()`** to call `childRegistry.cancelAllChildren()` after transitioning
the parent to `PENDING_CANCEL`. The `CancelRequestSink` lambda calls
`egressPublisher.dispatchChildCancelToFix(childFlyweight)`.

**Update `onTakeSnapshot()`** to also snapshot `childRegistry`:
```java
// After snapshotting OrderBook entries:
egressPublisher.writeSnapshotBegin(snapshotPub, ClusterMessageType.SNAPSHOT_CHILD_ORDER,
                                   childRegistry.size());
childRegistry.snapshot(snapshotScratchBuffer, 0);
// ... write to snapshotPub ...
egressPublisher.writeSnapshotEnd(snapshotPub);
```

**Update `onLoadSnapshot()`** to restore `childRegistry` and re-link parent-child
relationships in `OrderBook` (call `childRegistry.restore(buf, offset, orderBook)`).

---

### Step 13 — Add `transitionToRouting()` to `OrderStateMachine.java` (oms-core)

```java
/**
 * Transition a parent order from NEW to ROUTING.
 * ROUTING means: accepted, children dispatched, awaiting fills.
 *
 * Only valid from NEW state. No-op if already ROUTING (idempotent).
 * Returns false if the current state is not NEW or ROUTING.
 *
 * ZERO-ALLOCATION: single byte read + write.
 */
public boolean transitionToRouting(final OrderFlyweight parent) {
    final byte currentState = parent.getOrderState();
    if (currentState == ParentOrderState.ROUTING) {
        return true; // Already routing — idempotent
    }
    if (currentState != OrderState.NEW) {
        return false; // Invalid: only NEW → ROUTING is permitted
    }
    parent.setOrderState(ParentOrderState.ROUTING);
    parent.setTransactTime(System.nanoTime());
    // No handler callback for ROUTING — it is an internal state, not a FIX exec report event
    return true;
}
```

---

### Step 14 — Update `OmsLauncher.java` (oms-launcher)

Update the IPC channel map:

```java
// Stream 10: oms-core publishes accepted parent orders → algo-sor subscribes
ExclusivePublication parentOrderPub = aeron.addExclusivePublication("aeron:ipc", 10);
Subscription         parentOrderSub = aeron.addSubscription("aeron:ipc", 10);

// Stream 12: algo-sor publishes ChildOrderIntents → oms-core subscribes
// Stream 11 is REMOVED (algo-sor no longer sends to FIX bridge directly)
ExclusivePublication intentPub = aeron.addExclusivePublication("aeron:ipc", 12);
Subscription         intentSub = aeron.addSubscription("aeron:ipc", 12);
```

Wire `intentSub` into `OmsClusteredService` so it polls for intents on each work cycle.
Wire `intentPub` into `AlgoSorAgent` constructor (replacing the old `childOrderPub`).

Also call `ParentOrderState.registerTransitions()` before starting any AgentRunners:
```java
ParentOrderState.registerTransitions(); // extend transition table once, at startup
```

---

### Step 15 — Build and Verify

**Build:**
```bash
./gradlew clean build 2>&1 | tail -80
```

Fix all compilation errors. Re-run until `BUILD SUCCESSFUL`.

**Boundary check — algo-sor must not know about oms-core:**
```bash
./gradlew :algo-sor:dependencies --configuration compileClasspath 2>&1 | grep "oms-core"
# Expected: no output
```

**Boundary check — algo-sor must not hold FIX bridge references:**
```bash
grep -r "dispatchChildOrderToFix\|FixToFlyweight\|AeronEgress" algo-sor/src/
# Expected: no output
```

**State model check — ROUTING state is registered:**
```bash
grep -r "ROUTING" oms-core/src/ | grep -v "\.class"
# Expected: hits in ParentOrderState.java, OmsClusteredService.java,
#           ChildOrderIntentValidator.java, OrderStateMachine.java
```

**Intent flow check — oms-core creates child, not algo-sor:**
```bash
grep -r "createChild\|ChildOrderRegistry" algo-sor/src/
# Expected: no output (ChildOrderRegistry lives in oms-core only)

grep -r "createChild\|ChildOrderRegistry" oms-core/src/
# Expected: hits in ChildOrderRegistry.java, OmsClusteredService.java
```

---

## Hard Constraints

These override everything else. No exception.

1. **`algo-sor` publishes ONLY `ChildOrderIntent` messages.** It does not send NOS messages
   to the FIX bridge. It does not write to `OrderBook`. It does not write to `ChildOrderRegistry`.
   It has no Aeron publication except to stream 12.

2. **Every child order is created inside `oms-core`.** The `createChild()` call in
   `ChildOrderRegistry` is the ONLY place child order records are created.

3. **Every state transition goes through `OrderStateMachine`.** No direct `setOrderState()`
   calls outside of `OrderStateMachine` methods or `ChildOrderRegistry.createChild()`
   (which sets initial state to `OrderState.NEW` as part of construction).

4. **`ChildOrderIntentValidator` runs before `createChild()`.** No child is ever created
   without a `PASS` result from the validator. This is the state-model correctness guarantee.

5. **No object allocation on the hot path.** `onChildOrderIntent()`,
   `ChildOrderIntentValidator.validate()`, `ChildOrderRegistry.createChild()`,
   and `ChildOrderRegistry.applyFillAndAggregate()` must each contain zero `new` expressions.

6. **`OrderState.TRANSITION_TABLE` size is 16.** `NUM_STATES` is updated before
   `ParentOrderState.registerTransitions()` is called. The `ROUTING` state at index 10
   must not overflow the array.

7. **`algo-sor` is stateless between work cycles.** The only persistent state in `algo-sor`
   is the in-progress slice counters inside `IcebergAlgoEngine` and `TwapAlgoEngine` instances.
   These are accepted as soft state: if `algo-sor` restarts, `oms-core` will re-send the
   parent order and slicing resumes. The `ChildOrderIntentValidator`'s over-allocation guard
   prevents duplicate child qty from being created.

---

## Verification Criteria

The task is complete when ALL of the following pass:

- [ ] `./gradlew clean build` → `BUILD SUCCESSFUL`
- [ ] `./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core` → no output
- [ ] `grep -r "dispatchChildOrderToFix" algo-sor/src/` → no output
- [ ] `grep -r "createChild" algo-sor/src/` → no output
- [ ] `grep -r "ROUTING" oms-core/src/ | wc -l` → ≥ 4 hits
- [ ] `ChildOrderIntentValidator.validate()` contains no `new` expressions
- [ ] `ChildOrderRegistry.createChild()` contains no `new` expressions
- [ ] `ChildOrderRegistry.applyFillAndAggregate()` contains no `new` expressions
- [ ] `OmsClusteredService.onChildOrderIntent()` calls `intentValidator.validate()` BEFORE
      `childRegistry.createChild()` — verify by reading the method in order
- [ ] `OrderFields` static assert passes: `OFFSET_PARENT_OR_FIRST_CHILD_ID + Long.BYTES == BLOCK_LENGTH`
- [ ] `ParentOrderState.registerTransitions()` is called in `OmsLauncher.main()` before
      any `AgentRunner` is started
