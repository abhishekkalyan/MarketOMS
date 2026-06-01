# market-oms

A sell-side Order Management System (OMS) built for deterministic sub-millisecond execution with zero garbage collection on the hot path. Written in Java 21 using Aeron Cluster for fault-tolerant state replication, Agrona for off-heap memory management, and Aeron IPC/UDP for inter-component transport.

---

## Design Goals

- **Zero-GC hot path.** Every component on the critical execution path — validation, state machine, child order routing — operates on pre-allocated off-heap buffers. No object allocation occurs between message receipt and exec report dispatch.
- **Deterministic latency.** Single-threaded execution loop on the cluster service thread. No `synchronized` blocks, no lock contention, no JVM safepoints on the trading path.
- **Golden-source order state.** `oms-core` is the single authoritative store for all order state — parent and child. No component outside `oms-core` creates, mutates, or persists order records.
- **Transparent failover.** Aeron Cluster replicates all state transitions via Raft. If the leader node fails, a follower promotes automatically, replays the commit log from the last snapshot, and resumes processing without data loss.
- **Explicit wiring, no frameworks.** No Spring, no CDI, no reflection. All construction and wiring happens in `OmsLauncher.main()` via constructor injection.

---

## Architecture

### Module Map

```
market-oms/
├── oms-codec/       Binary contract — shared flyweights, field offsets, ClusterMessageType
├── oms-core/        Order state machine, validation, Aeron Cluster service, OmsNode entry point
├── algo-sor/        Algo execution engines and Smart Order Router (pure computation, stateless)
├── oms-launcher/    Process entry point for algo-sor — wires AlgoSorAgent, starts AgentRunner
└── oms-harness/     Black-box test harness — injects orders via Aeron Cluster client API
```

**Hard module boundary:** `algo-sor` must never depend on `oms-core`. The only shared module is `oms-codec`. All inter-module communication at runtime is over Aeron IPC.

### Component Interactions

```
Harness / FIX Client
        │
        │  Aeron Cluster ingress (aeron:udp, port 9000)
        │  Wire: ClusterMessageType byte + 128-byte OrderLayout
        ▼
Aeron Cluster (Raft — single node for dev, 3-node for prod)
  • Commits message to replicated log across quorum
  • onSessionMessage() fires only after commit
        │
        ▼
oms-core  OmsClusteredService  (clustered-service thread)
  ┌──────────────────────────────────────────────────────┐
  │  1. ValidationEngine  — 8 fast-fail rules + dedup    │
  │  2. OrderBook         — off-heap parent store        │
  │  3. OrderStateMachine — state transitions            │
  │  4. ChildOrderIntentValidator — 7 pre-child rules    │
  │  5. ChildOrderRegistry — off-heap child store        │
  └──────────────────────────────────────────────────────┘
        │ Aeron IPC stream 10 (parent orders for routing)
        ▼
algo-sor  AlgoSorAgent  (separate AgentRunner thread)
  • SmartOrderRouter — splits qty across venues by price
  • IcebergAlgoEngine — display-qty slicing
  • TwapAlgoEngine   — time-interval slicing
  • Publishes ChildOrderIntent back to oms-core — NEVER creates child state
        │ Aeron IPC stream 12 (ChildOrderIntents)
        ▼
oms-core  onChildOrderIntent
  • ChildOrderIntentValidator — validates against live parent state (7 rules)
  • ChildOrderRegistry.createChild() — child born here and only here
  • OrderStateMachine.transitionToRouting() — parent advances to ROUTING
        │ Aeron IPC → FIX bridge
        ▼
FIX Connectivity Layer → Venue / Exchange
```

Exec reports from venues flow back through the FIX bridge into `oms-core` via `handleExecReport`, where fills are aggregated into child and parent state and acknowledgements are sent back to the cluster client via `ClientSession.offer()`.

### Why algo-sor Sends Intents, Not Orders

`algo-sor` is a pure computation engine. It calculates *what* should be routed based on order parameters, but `oms-core` decides *whether* to honour the instruction based on current live state. By the time a `ChildOrderIntent` arrives, the parent may have received fills, a cancel, or a full fill. The `ChildOrderIntentValidator` enforces seven rules — including an over-allocation guard — before any child record is created. This makes `algo-sor` stateless between work cycles and means all durable state lives in the replicated `oms-core` cluster.

### Cluster Ingress Wire Format

All messages between Aeron Cluster clients and `OmsClusteredService` use `ClusterMessageType` framing:

```
[0]      msgType  : byte         — 1=NEW_ORDER, 2=CANCEL_ORDER, 3=REPLACE_ORDER
[1..128] payload  : OrderLayout  — 128-byte order record at OrderLayout field offsets
```
Total: 129 bytes. Exec report responses use the same format sent back via `ClientSession.offer()`.

`FIXMessageDecoder` handles the separate FIX bridge → oms-core path only (pre-parsed FIX Binary, stream 10); it is never used for cluster ingress/egress.

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

`ROUTING` is a parent-only extension state (`ParentOrderState`, value 10). Base states (`OrderState`, bytes 0–7) are shared by both parent and child orders. `OrderState.NUM_STATES = 16` to accommodate parent-only states. `ParentOrderState.registerTransitions()` must be called before any `AgentRunner` starts.

### Memory Layout

All orders — parent and child — are stored as contiguous 128-byte binary records in pre-allocated off-heap `UnsafeBuffer` instances. Field access is a direct memory read/write at a computed byte offset. No `Order` object is ever allocated on the hot path.

```
OrderLayout (128 bytes = 2 × 64-byte cache lines):

  Offset  Size  Field
  ------  ----  --------------------------------
     0      8   accountId
     8      8   clOrdId
    16      8   orderId
    24      8   origClOrdId
    32      8   symbol               (packed ASCII long, up to 8 chars)
    40      8   price                (fixed-point × 10,000)
    48      8   qty
    56      8   filledQty
    64      8   leavesQty
    72      1   side
    73      1   timeInForce
    74      1   orderState
    75      1   (reserved)
    76      4   venueId
    80      8   transactTime         (nanos)
    88–111  24  (reserved)
   112      4   childCount           (parent only)
   116      4   nextSiblingSlot      (child only — slot index of next sibling)
   120      8   parentOrFirstChildId (parent: first child orderId; child: parent orderId)
```

Prices are stored as `long` scaled by `10,000`. `£12.3456` → `123456L`. No `double`, no `BigDecimal` on the hot path.

### Aeron IPC Channel Map

| Stream ID | Direction | Content |
|-----------|-----------|---------|
| 10 | `oms-core` → `algo-sor` | Accepted parent orders (ClusterMessageType + OrderLayout) |
| 12 | `algo-sor` → `oms-core` | ChildOrderIntents (ClusterMessageType header + 56-byte intent) |

Stream 11 is removed. `algo-sor` never writes to the FIX bridge.

### Failover and Recovery

Each node runs as an Aeron Cluster member. State is snapshotted via `onTakeSnapshot()`, which serialises `OrderBook`, `ChildOrderRegistry`, and `ValidationEngine.seenClOrdIds` into the snapshot buffer as raw binary records — no Java serialisation, no JSON. `onStart()` with a non-null `snapshotImage` restores all state before any new messages are processed.

**State persistence:** Aeron Cluster stores two separate state trees on disk:
- **Archive dir** — Raft log recordings (`.rec` files), archive catalog. Wiped by `Archive.Context.deleteArchiveOnStart(true)`.
- **Cluster dir** (subdirectory of archive dir) — `node-state.dat`, `recording.log`, cluster mark files. Wiped by `ConsensusModule.Context.deleteDirOnStart(true)`. **Both must be wiped together.** Deleting only the archive leaves the cluster dir intact, causing the ConsensusModule to replay all prior session messages and restore `ValidationEngine` state even without a snapshot.

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| JDK | 21 | OpenJDK 21 or GraalVM 21 |
| Gradle | 8+ | Wrapper included — use `./gradlew` |
| OS | Linux or macOS | See macOS note below |
| RAM | 4 GB minimum | 8 GB recommended |

**macOS note.** `/dev/shm` does not exist on macOS. `OMS_AERON_DIR` defaults to `/dev/shm/oms-aeron-<nodeId>`. For local development set `OMS_AERON_DIR=/tmp/oms-aeron-0` (the harness script does this automatically). Latency will not reflect production.

**JVM flags.** All modules that use Agrona require these `--add-opens` in `applicationDefaultJvmArgs` (already set in each module's `build.gradle`):
```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
```
Omitting `jdk.internal.misc` causes `IllegalAccessError` from `UnsafeApi` at startup.

---

## Building

```bash
# Build and install all modules
./gradlew installDist

# Build a specific module
./gradlew :oms-core:installDist

# Verify module boundary: algo-sor must not depend on oms-core
./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core
# Expected: no output
```

---

## Running a Single Node

`OmsNode` is the entry point for `oms-core`. All configuration is via environment variables — no config files, no command-line arg parsing.

```bash
export OMS_NODE_ID=0
export OMS_AERON_DIR=/tmp/oms-aeron-0
export OMS_ARCHIVE_DIR=/tmp/oms-archive-0
export OMS_MAX_NOTIONAL=10000000
export OMS_SYMBOLS="AAPL,MSFT,GOOG,AMZN"
export OMS_CLUSTER_MEMBERS="0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010"

./gradlew :oms-core:installDist
oms-core/build/install/oms-core/bin/oms-core
```

Successful startup logs:
```
INFO  OmsNode - Starting OMS node 0 with 4 permitted symbols (deleteArchiveOnStart=false)
INFO  OmsNode - OMS node 0 is running. Waiting for shutdown signal...
INFO  OmsClusteredService - OMS cluster role changed: FOLLOWER → LEADER
INFO  OmsClusteredService - New leadership term: leaderMemberId=0 termId=0
```

### Environment Variables Reference

#### OmsNode (oms-core)

| Variable | Default | Description |
|----------|---------|-------------|
| `OMS_NODE_ID` | `0` | Integer node index within the cluster |
| `OMS_AERON_DIR` | `/dev/shm/oms-aeron-<nodeId>` | Aeron MediaDriver shared memory directory. Use `/tmp/...` on macOS |
| `OMS_ARCHIVE_DIR` | `/tmp/oms-archive-<nodeId>` | Aeron Archive directory (Raft log + snapshots) |
| `OMS_MAX_NOTIONAL` | `10000000` | Per-order notional limit in base currency units |
| `OMS_SYMBOLS` | `AAPL,MSFT,GOOG,AMZN` | Comma-separated whitelist of permitted ticker symbols |
| `OMS_CLUSTER_MEMBERS` | `0,localhost:9000,...` | Aeron Cluster member string — see format below |
| `OMS_CLUSTER_INGRESS_CHANNEL` | `aeron:udp?endpoint=localhost:9000` | Endpoint where the leader accepts client session requests |
| `OMS_ARCHIVE_CONTROL_CHANNEL` | `aeron:udp?endpoint=localhost:8010` | Archive bind channel (UDP; used for cross-node replication) |
| `OMS_ARCHIVE_LOCAL_CONTROL_CHANNEL` | `aeron:ipc` | Archive request channel for in-process clients (must be IPC when archive is embedded) |
| `OMS_ARCHIVE_LOCAL_RESPONSE_CHANNEL` | `aeron:ipc` | Archive response channel for in-process clients |
| `OMS_ARCHIVE_REPLICATION_CHANNEL` | `aeron:udp?endpoint=localhost:0` | Endpoint peers use for log replication. Use a fixed port in multi-node deployments |
| `OMS_ARCHIVE_DELETE_ON_START` | `false` | Set `true` to wipe all archive and cluster state on restart (use in test environments only) |

**`OMS_CLUSTER_MEMBERS` format** (Aeron 1.40+):
```
<nodeId>,<clientHost:port>,<memberHost:port>,<logHost:port>,<transferHost:port>,<archiveControlHost:port>
```
Pipe-separate (`|`) multiple nodes. Single-node default:
```
0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010
```
Three-node example:
```
0,host1:9000,host1:9001,host1:9002,host1:9003,host1:8010|1,host2:9000,host2:9001,host2:9002,host2:9003,host2:8010|2,host3:9000,host3:9001,host3:9002,host3:9003,host3:8010
```

#### AlgoSorAgent (oms-launcher)

| Variable | Default | Description |
|----------|---------|-------------|
| `OMS_AERON_DIR` | `/dev/shm/oms-aeron-launcher` | Aeron dir for the launcher process (must differ from OmsNode's dir) |

---

## Test Harness

`oms-harness` is a black-box integration test harness that connects to a running `OmsNode` as an Aeron Cluster client, injects orders, and asserts exec report responses.

### Quick start

```bash
./run-harness.sh
```

This script:
1. Builds all modules via Gradle
2. Deletes stale Aeron and archive directories
3. Starts `OmsNode` (node 0) and waits for its MediaDriver to be ready
4. Starts `AlgoSorAgent` and waits 3 seconds for it to connect
5. Runs the harness scenarios
6. Prints a pass/fail summary and exits 0 on full pass

Expected output on success:
```
Building all modules...
Build OK.
Starting OmsNode (node 0)... logging to /tmp/oms-node-harness.log
  Waiting for OmsNode MediaDriver... ready.
Starting AlgoSorAgent... logging to /tmp/oms-launcher-harness.log

Running test harness...
================================================================
...
  HARNESS RESULTS:  5 passed,  0 failed
  Exec reports received: 55
  Filled: 0  Canceled: 0  Rejected: 2
================================================================
ALL SCENARIOS PASSED
```

### Harness scenarios

| # | Scenario | What it validates |
|---|----------|------------------|
| 1 | Single order accepted | Valid NEW_ORDER reaches `state=NEW`; exec report arrives via cluster egress |
| 2 | Order rejected — zero qty | `qty=0` returns `state=REJECTED` |
| 3 | Order rejected — unknown symbol | Symbol not in `OMS_SYMBOLS` whitelist returns `state=REJECTED` |
| 4 | Cancel request | Order accepted then cancel reaches `state=PENDING_CANCEL` |
| 5 | Bulk injection (50 orders) | All 50 orders accepted and acknowledged within timeout |

### Harness environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HARNESS_AERON_DIR` | `/tmp/oms-aeron-harness` | Aeron dir for the harness process (must differ from node and launcher) |
| `HARNESS_CLUSTER_INGRESS` | `0=localhost:9000` | Cluster ingress endpoint in `nodeId=host:port` format |
| `HARNESS_TIMEOUT_MS` | `8000` | Per-scenario wait timeout in milliseconds |

### Running against a remote node

```bash
HARNESS_CLUSTER_INGRESS="0=10.0.0.5:9000" ./run-harness.sh
```

### Logs

| File | Content |
|------|---------|
| `/tmp/oms-node-harness.log` | OmsNode startup, leadership, session events, order processing |
| `/tmp/oms-launcher-harness.log` | AlgoSorAgent startup and intent publication |

---

## Three-Node Cluster (production-like)

Run three `OmsNode` processes with different `OMS_NODE_ID` values. All three must agree on `OMS_CLUSTER_MEMBERS`. For a local multi-node test (all on localhost with different ports):

**Node 0:**
```bash
export OMS_NODE_ID=0
export OMS_AERON_DIR=/tmp/oms-aeron-0
export OMS_ARCHIVE_DIR=/tmp/oms-archive-0
export OMS_CLUSTER_MEMBERS="0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010|1,localhost:9100,localhost:9101,localhost:9102,localhost:9103,localhost:8110|2,localhost:9200,localhost:9201,localhost:9202,localhost:9203,localhost:8210"
export OMS_CLUSTER_INGRESS_CHANNEL="aeron:udp?endpoint=localhost:9000"
export OMS_ARCHIVE_REPLICATION_CHANNEL="aeron:udp?endpoint=localhost:8020"
oms-core/build/install/oms-core/bin/oms-core
```

**Node 1** (different `OMS_NODE_ID`, `OMS_AERON_DIR`, `OMS_ARCHIVE_DIR`, and per-node channels):
```bash
export OMS_NODE_ID=1
export OMS_AERON_DIR=/tmp/oms-aeron-1
export OMS_ARCHIVE_DIR=/tmp/oms-archive-1
export OMS_CLUSTER_MEMBERS="..."   # same string as node 0
export OMS_CLUSTER_INGRESS_CHANNEL="aeron:udp?endpoint=localhost:9100"
export OMS_ARCHIVE_CONTROL_CHANNEL="aeron:udp?endpoint=localhost:8110"
export OMS_ARCHIVE_REPLICATION_CHANNEL="aeron:udp?endpoint=localhost:8120"
oms-core/build/install/oms-core/bin/oms-core
```

Raft elects one leader (watch for `role=LEADER` in the logs). The other two log `role=FOLLOWER`. The harness or FIX client connects to the leader's `OMS_CLUSTER_INGRESS_CHANNEL` endpoint.

---

## Dependency Versions

| Library | Version |
|---------|---------|
| `io.aeron:aeron-cluster` | 1.47.0 |
| `io.aeron:aeron-archive` | 1.47.0 |
| `io.aeron:aeron-driver` | 1.47.0 |
| `io.aeron:aeron-client` | 1.47.0 |
| `org.agrona:agrona` | 2.0.1 |
| `ch.qos.logback:logback-classic` | 1.5.6 |

All versions are declared in `gradle/libs.versions.toml`.

---

## Key Design Decisions

### Why Aeron Cluster and not Kafka?

Kafka's consumer model is pull-based with polling latency added on top of replication. Aeron Cluster's `onSessionMessage()` fires only after quorum commit — the processing latency equals the replication latency with nothing added. For a state machine that must guarantee ordering and exactly-once processing, Aeron Cluster's model is correct by construction.

### Why algo-sor is stateless

Algo engines are computation-driven, not state-driven. Embedding them in the `ClusteredService` would replicate ephemeral slice-scheduling state across Raft nodes unnecessarily. The intent-based separation means `algo-sor` can be upgraded, restarted, or replaced independently of the cluster. Its statelessness also means the `ChildOrderIntentValidator`'s over-allocation guard is the sole protection against over-allocation after parent state changes.

### Why fixed-point longs for prices

`double` arithmetic is non-deterministic across JVM implementations and CPU architectures — the same computation can produce different results on different nodes, corrupting the replicated state machine. `BigDecimal` allocates on every operation. A `long` scaled by `10,000` provides four decimal places of price precision (sufficient for equities), is deterministic everywhere, and adds zero GC pressure.

### Why ClusterMessageType framing on cluster ingress, not FIX Binary

The Aeron Cluster client API is a binary transport — it has no concept of FIX fields. `FIXMessageDecoder` operates on a pre-parsed compact FIX Binary format that the upstream FIX engine produces. Using FIX Binary on the cluster ingress would couple the cluster wire protocol to a specific FIX engine's internal format and require the cluster client to know FIX ASCII byte values. `ClusterMessageType` + `OrderLayout` is self-contained within `oms-codec` and lets any client (harness, FIX bridge, test tool) submit orders without FIX knowledge.

---

## Licence

Proprietary. All rights reserved.
