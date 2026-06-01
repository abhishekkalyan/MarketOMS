# Engineering Rules
- Strictly Zero-GC in core packages.
- No object allocation, no boxing/unboxing, no standard Java collections on the hot path.
- Use Agrona primitive collections and DirectBuffers exclusively.
- Use fixed-point math (long) for prices.

---

# Module Structure

Gradle multi-module monorepo. Four bounded modules + launcher:

```
market-oms/
  oms-codec/      codec/, fix/         — shared binary contract, Agrona only (no Aeron)
  oms-core/       statemachine/, validation/, cluster/, common/
  algo-sor/       algo/, algoagent/
  oms-launcher/   launcher/
```

**Hard boundary:** `algo-sor` must NOT depend on `oms-core`. The only shared module is `oms-codec`. All inter-module communication is via Aeron IPC at runtime.

**No Spring, no CDI, no reflection.** All wiring is explicit constructor injection in `OmsLauncher`.

**Java 21 only.** `sealed` interfaces or `records` are permitted only where they introduce no hot-path allocation.

---

# Aeron IPC Channel Map

| Stream ID | Direction              | Purpose                                      |
|-----------|------------------------|----------------------------------------------|
| 10        | oms-core → algo-sor    | Accepted parent orders                       |
| 12        | algo-sor → oms-core    | `ChildOrderIntent` routing instructions      |

Stream 11 (direct child NOS from algo-sor to FIX bridge) is **removed**. `algo-sor` never writes to the FIX bridge.

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
6. Dispatches child NOS to FIX bridge via `AeronEgressPublisher`

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

Base states (`OrderState.java`, byte constants 0–7): `NEW`, `PENDING_NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `PENDING_CANCEL`, `REPLACED`, `REJECTED`.

Parent-only extension (`ParentOrderState.java`, values 10+):
- `ROUTING = 10` — parent accepted, one or more children dispatched, awaiting fills.

`OrderState.NUM_STATES = 16` (was 8) to accommodate parent-only states at index 10. `ParentOrderState.registerTransitions()` must be called in `OmsLauncher.main()` before any `AgentRunner` is started.

ROUTING lifecycle:
- `NEW → ROUTING` — first `ChildOrderIntent` accepted
- `ROUTING → PARTIALLY_FILLED` — first child fill aggregated
- `ROUTING → CANCELED` — all children canceled before any fill
- `ROUTING → FILLED` — all qty filled via children

---

# Binary Layout — `OrderFields.java`

Block length: **128 bytes** (fixed, never increase).

Reserved region (offsets 112–127) is consumed by parent-child linkage fields:

| Offset | Size | Type | Constant                          | Semantics                                                      |
|--------|------|------|-----------------------------------|----------------------------------------------------------------|
| 112    | 4    | int  | `OFFSET_CHILD_COUNT`              | Parent: number of live children. 0 on child orders.           |
| 116    | 4    | int  | `OFFSET_NEXT_SIBLING_SLOT`        | Child: slot index of next sibling. 0 = end of list.           |
| 120    | 8    | long | `OFFSET_PARENT_OR_FIRST_CHILD_ID` | Parent: first child orderId. Child: parent orderId.           |

Static assert: `OFFSET_PARENT_OR_FIRST_CHILD_ID + Long.BYTES == BLOCK_LENGTH`.

---

# Binary Layout — `ChildOrderIntentFlyweight.java`

56 bytes total (fits in one cache line). Package: `com.sellside.oms.codec`.

| Offset | Size | Type  | Field                 |
|--------|------|-------|-----------------------|
| 0      | 8    | long  | parentOrderId         |
| 8      | 8    | long  | parentClOrdId         |
| 16     | 8    | long  | sliceQty              |
| 24     | 8    | long  | limitPrice (fixed-pt) |
| 32     | 4    | int   | venueId               |
| 36     | 1    | byte  | algoType (1=SOR, 2=ICEBERG, 3=TWAP) |
| 37     | 1    | byte  | sliceIndex            |
| 38     | 2    | short | _pad                  |
| 40     | 8    | long  | intentTimestampNanos  |
| 48     | 8    | long  | _reserved             |

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
