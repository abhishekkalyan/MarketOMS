# Design Principles
<!-- Load for every task. -->

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
(checklist.md) for the mandatory procedure before any change.

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
