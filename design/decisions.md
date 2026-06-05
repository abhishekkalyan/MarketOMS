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
   - **Hard limit: max 256 unique children per parent order** (sliceIndex is a byte; wrapping causes clOrdId collision in `ChildOrderRegistry.clOrdIdToOrderId`)
   - Current defaults are safe: TWAP dispatches at most `DEFAULT_SLICES = 12` slices; Iceberg with `peakFraction = 10` dispatches ~10 slices
   - `IcebergAlgoEngine.onSlice()` enforces this with a WARN + break at `MAX_SLICES_PER_PARENT = 256`
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

---

## Capacity Constants — Decision 12

All per-process capacity limits are now env-var-configurable with defaults that match the prior hard-coded values:

| Env Var | Default | Owned by |
|---------|---------|----------|
| `OMS_MAX_ORDERS` | 65_536 | `OrderBook` |
| `OMS_MAX_CHILDREN` | 32_768 | `ChildOrderRegistry` |
| `OMS_INTENT_FRAGMENT_LIMIT` | 20 | `OmsClusteredService` |
| `OMS_ALGO_FRAGMENT_LIMIT` | 10 | `AlgoSorAgent` |
| `OMS_MAX_VENUES` | 10 | `SmartOrderRouter` / `AlgoSorAgent` |
| `OMS_MAX_ICEBERG_ORDERS` | 4_096 | `IcebergAlgoEngine` |
| `OMS_MAX_TWAP_ORDERS` | 512 | `TwapAlgoEngine` |

**Constraint:** Changing `OMS_MAX_ORDERS` or `OMS_MAX_CHILDREN` after a snapshot has been taken requires wiping the archive once (`OMS_ARCHIVE_DELETE_ON_START=true` for one restart, then revert to `false`). The snapshot version 3 header embeds the capacity values the snapshot was taken with; restore fails with a clear error if current capacity is smaller than the snapshot capacity.

**Stream ID note (F9):** `AeronTransport` and `ClusterMessageType` both declare `STREAM_CHILD_INTENTS = 12` and the oms-to-algo stream ID (30) as named constants. When sharding is added, shard-scoped stream IDs must be declared per-shard in `AeronTransport`; the existing constants become the base/default shard. See Section 10.2–10.3.

**Buffer sizes (F10):** `OmsClusteredService.algoOutbound` and `egressBuffer` use `ClusterMessageType.IPC_MESSAGE_SIZE` (= 1 + `OrderLayout.MESSAGE_SIZE` = 129 bytes). These are not capacity walls — the message format is fixed regardless of `OMS_MAX_ORDERS`.

---

## Decision 13 — `oms-config` module and pluggable `ConfigSource`

**Context:** Decision 12 introduced env-var-driven capacity configuration but left reading
scattered across two entry points (`OmsNode`, `OmsLauncher`) with no startup validation
and no way to supply values from sources other than OS env vars.

**Decision:** Introduce a dedicated `oms-config` Gradle module containing:
- `ConfigSource` interface — pluggable value supplier (env, file, future: Consul/etcd)
- `EnvVarConfigSource` — reads OS env vars (default, preserves all existing variable names)
- `PropertiesFileConfigSource` — loads a `.properties` file; path from `OMS_CONFIG_FILE`
- `ChainedConfigSource` — file values take priority over env vars when both are present
- `OmsConfig` — immutable value object; validates all fields at construction; logs each
  resolved field at INFO with its value and source name

**Dependency rule:** `oms-config` depends only on `oms-codec` and `slf4j-api`.
`algo-sor` and `oms-harness` must never depend on `oms-config`.
`oms-core` and `oms-launcher` depend on `oms-config`; they build a `ConfigSource` in their
entry points (`OmsNode.loadOmsConfig()`, `OmsLauncher.loadOmsConfig()`) and pass the
resolved `OmsConfig` fields as constructor arguments to components — no component reads
env vars or `OmsConfig` directly.

**Reversing:** Removing `oms-config` requires reinstating the per-entry-point `intEnv`/
`longEnv` helpers and losing startup validation and source-name logging.

---

## Decision 14 — `LatencyHistogram` implementation (custom `long[]` vs HdrHistogram)

**Context:** The performance harness (`oms-harness/perf`) requires a zero-allocation histogram
for recording round-trip latency at the `record()` call site. Two candidates were evaluated:
HdrHistogram (external jar, `org.HdrHistogram`) and a custom `long[]`-backed histogram.

**Decision:** Custom `long[]` histogram (`LatencyHistogram` in `com.cobain.oms.harness.perf`).
- 100 000 buckets × 1 µs each (covers 0–100 ms range)
- `record(long)` is one array-index write — provably zero-allocation
- `percentile(double)` is a primitive-array scan — zero-allocation
- No external dependency added to `oms-harness/build.gradle`

**Alternatives rejected:**
- HdrHistogram: `Histogram.recordValue()` is also zero-allocation for pre-allocated instances,
  but adds an external dependency (`org.hdrhistogram:HdrHistogram`) that must be vetted and
  version-pinned. Custom histogram eliminates the dependency management cost.

**Consequence of reversing:** Replacing with HdrHistogram requires adding `org.hdrhistogram:HdrHistogram`
to `oms-harness/build.gradle` and re-running the module boundary check
`./gradlew :oms-harness:dependencies --configuration compileClasspath | grep -E "oms-core|algo-sor"`
to confirm HdrHistogram does not pull in a conflicting transitive dependency.
