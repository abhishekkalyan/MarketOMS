# market-oms

A sell-side Order Management System (OMS) built for deterministic sub-millisecond execution with zero garbage collection on the hot path. Built in Java 21 using Aeron Cluster for fault-tolerant state replication, Agrona for off-heap memory management, and Aeron IPC/UDP for inter-component transport.

---

## Design Goals

- **Zero-GC hot path.** Every component on the critical execution path — validation, state machine, child order routing — operates on pre-allocated off-heap buffers. No object allocation occurs between message receipt and FIX dispatch.
- **Deterministic p99.99 latency.** Single-threaded execution loops pinned to isolated CPU cores. No `synchronized` blocks, no lock contention, no JVM safepoints on the trading path.
- **Golden-source order state.** `oms-core` is the single authoritative store for all order state — parent and child. No component outside `oms-core` creates, mutates, or persists order records.
- **Transparent failover.** Aeron Cluster replicates all state transitions via Raft. If the leader node fails, a follower promotes automatically, replays the commit log from the last snapshot, and resumes processing without data loss or manual intervention.
- **Horizontal scalability.** Symbol-partitioned sharding via a stateless `InboundRouter`. Each shard is an independent, self-contained deployment of the full stack. Adding a shard requires no changes to any module's logic.

---

## Architecture

### Module Map

```
market-oms/
├── oms-codec/          Binary contract — zero external dependencies beyond Agrona
├── oms-core/           Order state machine, validation, Aeron Cluster service
├── algo-sor/           Algo execution engines and Smart Order Router
├── oms-router/         Stateless inbound symbol-to-shard router
└── oms-launcher/       Process entry point — wires all modules, starts AgentRunners
```

Each module has a hard dependency boundary enforced by Gradle. `algo-sor` depends only on `oms-codec` — never on `oms-core`. All inter-module communication at runtime is over Aeron IPC channels.

### Component Interactions

```
Client (FIX 4.4 TCP)
        │
        ▼
FIX Connectivity Layer          ← external, pre-existing
  • Owns TCP sessions, heartbeats, sequence numbers
  • Pre-parses FIX ASCII → compact binary buffer
        │ Aeron IPC
        ▼
oms-router (InboundRouter)
  • Reads symbol field (two longs, no String)
  • Hashes symbol → shard index (ShardAssignment.shardFor)
  • Forwards raw buffer to target shard — zero copy
        │ Aeron IPC (per-shard stream)
        ▼
oms-fix-bridge (FixToFlyweightTranslator)
  • Maps FIX binary buffer fields → OrderFlyweight offsets
  • No intermediate objects, no String conversion
        │ Aeron Cluster ingress
        ▼
Aeron Cluster (3-node Raft group per shard)
  • Commits message to replicated log across quorum
  • onSessionMessage() fires only after commit
        │
        ▼
oms-core (OmsClusteredService — single AgentRunner thread)
  ┌─────────────────────────────────────────────────┐
  │  1. SequenceTracker   — dedup / replay guard     │
  │  2. OrderValidationEngine — fast-fail pre-trade  │
  │  3. OrderStateMachine — state transitions        │
  │  4. OrderBook         — off-heap parent store    │
  └─────────────────────────────────────────────────┘
        │ Aeron IPC stream 10
        ▼
algo-sor (AlgoSorAgent — separate AgentRunner thread)
  • SmartOrderRouter — splits qty across venues by price
  • IcebergAlgoEngine — display-qty slicing
  • TwapAlgoEngine   — time-interval slicing
  • Publishes ChildOrderIntent (NOT a child order) back to oms-core
        │ Aeron IPC stream 12
        ▼
oms-core (onChildOrderIntent)
  • ChildOrderIntentValidator — validates intent against live parent state
  • ChildOrderRegistry.createChild() — child born here, linked to parent
  • OrderStateMachine.transitionToRouting() — parent state advances
        │ Aeron IPC → FIX bridge
        ▼
FIX Connectivity Layer → Venue / Exchange
```

### Why algo-sor Sends Intents, Not Orders

`algo-sor` is a pure computation engine. It calculates *what* should be routed based on market depth, but `oms-core` decides *whether* to honour the instruction based on current live order state. By the time a `ChildOrderIntent` arrives at `oms-core`, the parent may have received fills (reducing `leavesQty`), a cancel request, or a full fill. The `ChildOrderIntentValidator` enforces seven state-model rules — including an over-allocation guard — before any child record is created. This makes `algo-sor` stateless between work cycles and means all durable state lives in the replicated `oms-core` cluster.

### Order State Model

```
                    ┌─────────┐
              ──────►   NEW   ├──────────────────────────► REJECTED
                    └────┬────┘
                         │ first ChildOrderIntent accepted
                         ▼
                    ┌─────────┐
                    │ ROUTING │◄─── (additional child intents)
                    └────┬────┘
                         │ first fill aggregated
                         ▼
                  ┌──────────────┐
          ┌──────►│ PARTIALLY    │
          │       │   FILLED     ├──────────────────────► CANCELED
          │       └──────┬───────┘      (via PENDING_CANCEL)
          │fills         │ leavesQty == 0
          └──────────────▼
                    ┌─────────┐
                    │ FILLED  │  (terminal)
                    └─────────┘
```

Child orders follow the same base state constants (`OrderState.NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `PENDING_CANCEL`). Parent orders additionally use `ParentOrderState.ROUTING`.

### Memory Layout

All orders — parent and child — are stored as contiguous 128-byte binary records in pre-allocated off-heap `UnsafeBuffer` instances. Field access is a direct memory read or write at a computed byte offset. No `Order` object is ever allocated on the hot path.

```
OrderFlyweight binary layout (128 bytes, 2 × 64-byte cache lines):

 Offset  Size  Field
 ------  ----  ----------------------------
   0      8    accountId
   8      8    clOrdId
  16      8    orderId
  24      8    origClOrdId
  32      8    symbolHi          (8 ASCII chars packed as long)
  40      8    symbolLo          (next 8 chars)
  48      1    side              (1=BUY 2=SELL 3=SHORT_SELL)
  56      8    price             (fixed-point × 10,000)
  64      8    qty
  72      8    cumQty
  80      8    leavesQty
  88      1    timeInForce
  89      1    orderState
  90      1    execType
  92      4    venueId
  96      8    timestamp         (nanos since epoch)
 104      8    transactTime
 112      4    childCount        (parent only)
 116      4    nextSiblingSlot   (child only)
 120      8    parentOrFirstChildId
```

Prices are stored as `long` values scaled by `10,000`. `$12.3456` is stored as `123456L`. No `double`, no `BigDecimal` anywhere in the hot path.

### Aeron IPC Channel Map (per shard)

| Stream ID | Direction | Content |
|-----------|-----------|---------|
| 10 | `oms-core` → `algo-sor` | Accepted parent orders (OrderFlyweight) |
| 12 | `algo-sor` → `oms-core` | ChildOrderIntents (56-byte binary record) |
| 20 | `oms-core` → FIX bridge | Child order NOS dispatch |
| 30 | All shards → Risk Service | Fill events for account-level risk aggregation |

### Failover and Recovery

Each shard runs as a 3-node Aeron Cluster. State is snapshotted periodically and on leader change. `onTakeSnapshot()` serialises both `OrderBook` and `ChildOrderRegistry` directly into the snapshot buffer as raw binary records — no Java serialisation, no JSON. `onLoadSnapshot()` restores both stores and re-establishes parent-child links before any new messages are processed.

The `SequenceTracker` persists two watermark maps in the snapshot:
- Per-session Aeron log position (prevents double-processing replayed cluster messages)
- Per-venue FIX sequence number (prevents double-applying retransmitted fill reports)

`algo-sor` holds no durable state. On restart it re-subscribes to stream 10 and resumes. The `ChildOrderIntentValidator`'s over-allocation guard prevents duplicate child qty from being created if intents are re-submitted.

### Horizontal Scaling

Symbol-to-shard assignment uses a deterministic FNV-1a hash of the symbol's two packed longs, implemented in `ShardAssignment.shardFor()` in `oms-codec`. Both `InboundRouter` and `OrderValidationEngine` import from this single implementation — they can never diverge. `OrderValidationEngine` rejects any order whose symbol hashes to a different shard, providing a hard safety boundary against router misconfiguration.

For tier-1 symbols (SPY, AAPL, etc.) that generate disproportionate message volume, the router supports explicit static symbol-to-shard overrides that bypass the hash, giving those symbols a dedicated shard.

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| JDK | 21+ | GraalVM or OpenJDK both supported |
| Gradle | 8.5+ | Wrapper included — use `./gradlew` |
| OS | Linux | Required for CPU pinning and `/dev/shm` Aeron IPC |
| RAM | 8 GB minimum | 16 GB recommended for full 3-node local cluster |

> **macOS note.** Aeron IPC uses `/dev/shm` by default, which does not exist on macOS. Override the Aeron directory to `/tmp/aeron-oms` in `OmsLauncher` for local development on macOS. Latency characteristics will not reflect production.

> **Windows.** Not supported. Aeron's low-latency native transport requires POSIX shared memory.

---

## Building

```bash
# Clone
git clone https://github.com/your-org/market-oms.git
cd market-oms

# Build all modules
./gradlew clean build

# Build a specific module only
./gradlew :oms-core:build

# Skip tests (faster iteration)
./gradlew clean build -x test
```

### Verify module boundaries after build

```bash
# algo-sor must not depend on oms-core
./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core
# Expected: no output

# oms-codec must not depend on Aeron Cluster
./gradlew :oms-codec:dependencies --configuration compileClasspath | grep aeron-cluster
# Expected: no output
```

---

## Running Locally

A full local deployment runs one shard (3 Aeron Cluster nodes) plus the router and algo-sor agent. The launcher handles all wiring. For a single-node development setup, Aeron Cluster can be run in single-node mode by setting `clusterMemberCount=1`.

### Single-node development mode (fastest startup)

```bash
./gradlew :oms-launcher:run --args="--shard 0 --shards 1 --cluster-members 1 --dev"
```

This starts:
- One embedded Aeron `MediaDriver` (dedicated threading mode)
- One Aeron Cluster node (single-node Raft — no election needed)
- One `OmsClusteredService` AgentRunner
- One `AlgoSorAgent` AgentRunner
- Aeron IPC streams 10 and 12

Output on successful startup:
```
[INFO] MediaDriver started: /dev/shm/aeron-oms-shard-0
[INFO] Cluster node started: shard=0 role=LEADER
[INFO] OmsClusteredService ready
[INFO] AlgoSorAgent ready — polling stream 10
[INFO] market-oms shard 0 READY
```

### Three-node cluster mode (production-like)

Run three terminals, one per node. All nodes must be on the same host for local testing (they use different ports):

**Terminal 1 — node 0 (will become leader):**
```bash
./gradlew :oms-launcher:run \
  --args="--shard 0 --shards 1 --node 0 --cluster-members 3 \
          --cluster-hosts localhost:9000,localhost:9010,localhost:9020"
```

**Terminal 2 — node 1:**
```bash
./gradlew :oms-launcher:run \
  --args="--shard 0 --shards 1 --node 1 --cluster-members 3 \
          --cluster-hosts localhost:9000,localhost:9010,localhost:9020"
```

**Terminal 3 — node 2:**
```bash
./gradlew :oms-launcher:run \
  --args="--shard 0 --shards 1 --node 2 --cluster-members 3 \
          --cluster-hosts localhost:9000,localhost:9010,localhost:9020"
```

Once all three nodes start, Raft elects a leader (watch for `role=LEADER` in one terminal). The other two nodes log `role=FOLLOWER`.

### Multi-shard mode (4 shards locally)

Each shard needs its own set of three terminals. For a 4-shard local deployment, that is 12 terminal windows — use a process supervisor or run shards as background processes:

```bash
# Start 4 shards, single-node each (dev mode, no Raft replication)
for shard in 0 1 2 3; do
  ./gradlew :oms-launcher:run \
    --args="--shard $shard --shards 4 --cluster-members 1 --dev" \
    > logs/shard-${shard}.log 2>&1 &
  echo "Started shard $shard (PID $!)"
done

# Start the inbound router (routes by symbol hash across all 4 shards)
./gradlew :oms-router:run \
  --args="--shards 4 --shard-base-port 9000" \
  > logs/router.log 2>&1 &
echo "Started router (PID $!)"
```

### Shutting down

The launcher registers a JVM shutdown hook. Send `SIGTERM` or press `Ctrl+C`:

```bash
# Graceful shutdown — drains in-flight messages before stopping
kill -TERM <pid>

# Or if using the Gradle run task directly
# Ctrl+C in the terminal triggers the shutdown hook
```

On shutdown, the launcher closes AgentRunners first, then the Aeron client, then the MediaDriver. Shutdown order matters — reversing it causes MediaDriver to close IPC channels while agents are still writing.

---

## Configuration Reference

All configuration is passed as command-line arguments to `OmsLauncher`. There is no configuration file — this is intentional. External configuration files introduce startup-time I/O and String parsing on the initialisation path.

| Argument | Default | Description |
|----------|---------|-------------|
| `--shard` | `0` | Zero-based index of this shard |
| `--shards` | `1` | Total number of shards in the deployment |
| `--node` | `0` | Cluster node index within this shard's Raft group |
| `--cluster-members` | `1` | Number of Raft nodes per shard (1 or 3) |
| `--cluster-hosts` | `localhost:9000` | Comma-separated `host:port` for each cluster node |
| `--order-book-capacity` | `50000` | Max concurrent open parent orders per shard |
| `--child-registry-capacity` | `200000` | Max concurrent open child orders per shard |
| `--cl-ord-id-map-capacity` | `500000` | ClOrdID dedup map capacity (daily order volume estimate) |
| `--max-notional` | `100000000000` | Max notional per order in fixed-point units (£10M default) |
| `--aeron-dir` | `/dev/shm/aeron-oms-shard-{n}` | Aeron MediaDriver directory |
| `--dev` | `false` | Single-node mode, relaxed CPU affinity, verbose logging |

---

## Project Structure

```
market-oms/
│
├── oms-codec/
│   └── src/main/java/com/sellside/oms/
│       ├── codec/
│       │   ├── OrderFields.java              Binary layout constants and price scaling
│       │   ├── OrderFlyweight.java           Zero-allocation order view over DirectBuffer
│       │   └── ChildOrderIntentFlyweight.java Routing instruction from algo-sor to oms-core
│       └── fix/
│           ├── FixFields.java                Pre-parsed FIX buffer offsets
│           └── FixToFlyweightTranslator.java Zero-copy FIX → OrderFlyweight field mapping
│
├── oms-core/
│   └── src/main/java/com/sellside/oms/
│       ├── statemachine/
│       │   ├── OrderState.java               State constants + 16×8 transition table
│       │   ├── ParentOrderState.java         ROUTING state + transition registration
│       │   └── OrderStateMachine.java        State transition engine
│       ├── validation/
│       │   ├── ValidationResult.java         Byte result codes
│       │   ├── SymbolUniverse.java           Long-pair symbol registry (no String on hot path)
│       │   ├── OrderValidationEngine.java    Fast-fail inbound validation + ClOrdID dedup
│       │   └── ChildOrderIntentValidator.java State-model guard before child creation
│       ├── cluster/
│       │   ├── ClusterMessageType.java       Wire protocol message type constants
│       │   ├── OmsClusteredService.java      Aeron ClusteredService implementation
│       │   ├── AeronEgressPublisher.java     Outbound message publisher (zero-allocation)
│       │   └── SequenceTracker.java          Replay dedup — session + venue watermarks
│       └── common/
│           ├── OrderBook.java                Off-heap parent order store
│           └── ChildOrderRegistry.java       Off-heap child order store with parent linkage
│
├── algo-sor/
│   └── src/main/java/com/sellside/oms/
│       ├── algo/
│       │   ├── AlgoExecutionEngine.java      Interface: ChildIntentSink contract
│       │   ├── IcebergAlgoEngine.java        Display-qty iceberg slicer
│       │   ├── TwapAlgoEngine.java           Time-interval TWAP slicer
│       │   └── SmartOrderRouter.java         Venue depth sweep, selection-sort routing
│       └── algoagent/
│           └── AlgoSorAgent.java             Aeron Agent: polls stream 10, publishes to stream 12
│
├── oms-router/
│   └── src/main/java/com/sellside/oms/
│       └── router/
│           ├── InboundRouter.java            Symbol hash → shard forward (zero-copy)
│           └── RouterLauncher.java           Router process entry point
│
└── oms-launcher/
    └── src/main/java/com/sellside/oms/
        └── launcher/
            └── OmsLauncher.java              Wires all modules, manages AgentRunners
```

---

## Key Design Decisions

### Why Aeron Cluster and not Kafka?

Kafka provides durable, ordered log replication but its consumer model is pull-based with configurable batch sizes and delivery latency measured in milliseconds. Aeron Cluster provides deterministic, synchronous Raft consensus where `onSessionMessage()` fires only after quorum commit — the processing latency is the replication latency, not a polling interval added on top. For a state machine that must guarantee ordering and exactly-once processing, Aeron Cluster's model is correct by construction.

### Why not put Algo/SOR inside oms-core?

Algo engines are timer-driven and market-data-driven — their work cycle is fundamentally different from the Raft commit path. Embedding them in the `ClusteredService` would replicate ephemeral slice-scheduling state across Raft nodes unnecessarily, and a slow TWAP calculation could introduce jitter into the OMS acknowledgment latency. The intent-based separation means `algo-sor` can be upgraded, restarted, or replaced independently of the cluster.

### Why fixed-point longs for prices?

`double` arithmetic is non-deterministic across JVM implementations and CPU architectures — the same computation can produce different results on different nodes, which would corrupt the replicated state machine. `BigDecimal` allocates on every operation. A `long` scaled by `10,000` provides four decimal places of price precision (sufficient for equities and most fixed income), is deterministic everywhere, and adds zero GC pressure.

### Why selection sort in SmartOrderRouter?

The venue count is bounded by `MAX_VENUES = 16`. For n ≤ 16, selection sort outperforms `Arrays.sort()` because it avoids the `Comparator` object allocation and the overhead of the dual-pivot quicksort's branching for small arrays. The sort is in-place over three parallel primitive arrays — no objects created.

---

## Testing

```bash
# Run all tests
./gradlew test

# Run tests for a specific module
./gradlew :oms-core:test

# Run a specific test class
./gradlew :oms-core:test --tests "com.sellside.oms.statemachine.OrderStateMachineTest"

# Generate test report
./gradlew test jacocoTestReport
open build/reports/jacoco/test/html/index.html
```

### What is tested

- `OrderStateMachine` — all valid and invalid state transitions, including ROUTING and the `ParentOrderState` extensions
- `OrderValidationEngine` — each reject code including `REJECT_WRONG_SHARD`
- `ChildOrderIntentValidator` — all seven rules including the over-allocation guard
- `ChildOrderRegistry` — create, fill aggregation, cancel propagation, and linked-list integrity
- `SequenceTracker` — dedup logic for both session and venue watermarks, snapshot round-trip
- `ShardAssignment` — deterministic hash stability across symbol encodings

### Zero-allocation assertion

Tests that exercise the hot path run with `-Xmx256m -verbose:gc` and assert that no GC events occur during the test. This is enforced by a JUnit 5 extension that captures `GarbageCollectionNotificationInfo` events via JMX.

---

## Dependency Versions

| Library | Version | Dependency |
|---------|---------|------------|
| `io.aeron:aeron-all` | 1.44.x | `oms-core`, `algo-sor`, `oms-router`, `oms-launcher` |
| `io.aeron:agrona` | 1.22.x | All modules |
| `org.junit.jupiter` | 5.10.x | Test scope, all modules |

All versions are declared in `gradle/libs.versions.toml`. To update, change the version there and run `./gradlew clean build`.

---

## Useful Commands

```bash
# Check dependency tree for a module
./gradlew :oms-core:dependencies --configuration compileClasspath

# Find all classes that depend on oms-core (should only be oms-launcher)
./gradlew dependencies | grep oms-core

# Run with GC logging enabled (confirm zero-GC hot path)
./gradlew :oms-launcher:run \
  --jvm-args="-Xlog:gc*:file=logs/gc.log:time,uptime:filecount=5,filesize=20m" \
  --args="--shard 0 --shards 1 --cluster-members 1 --dev"

# CPU affinity — pin the JVM process to cores 2-5 (requires taskset on Linux)
taskset -c 2-5 ./gradlew :oms-launcher:run --args="..."

# Heap dump on OOM (should never trigger in production; useful during development)
./gradlew :oms-launcher:run \
  --jvm-args="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/oms-heap.hprof" \
  --args="..."
```

---

## Licence

Proprietary. All rights reserved.