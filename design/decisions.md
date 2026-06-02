# Design Decision Log
<!-- Load when: a proposed change might reverse a prior decision,
     or when making a new non-obvious architectural choice. -->

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

## Scaling Model

### 10.1 Current topology

Single shard. All symbols processed by one Aeron Cluster (3-node Raft group). `AeronTransport.IPC_CHANNEL = "aeron:ipc"`.

### 10.2 Horizontal scaling path

Symbol-partitioned sharding (not yet implemented). Shard isolation would use separate `aeronDirectoryName` per shard, making stream IDs shard-local.

### 10.3 What changes when a shard is added

`OmsLauncher.java` (new channel wiring), `OmsNode.java` (new env var for shard routing), `AeronTransport` (new stream ID constants). No changes to `OrderStateMachine`, `ValidationEngine`, `ChildOrderRegistry`, or `ChildOrderIntentValidator`.

### 10.4 Cross-shard concerns

Symbol-scoped: fills always arrive for the same symbol as the parent → same shard. Cross-shard risk: account-level notional limits span symbols — not yet implemented.
