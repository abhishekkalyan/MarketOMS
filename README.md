# market-oms

A sell-side Order Management System (OMS) built for deterministic sub-millisecond execution with zero garbage collection on the hot path. Written in Java 21 using Aeron Cluster for fault-tolerant state replication, Agrona for off-heap memory management, and Aeron IPC for inter-component transport.

---

## Design Goals

- **Zero-GC hot path.** Every component on the critical execution path — validation, state machine, child order routing — operates on pre-allocated off-heap buffers. No object allocation occurs between message receipt and exec report dispatch.
- **Deterministic latency.** Single-threaded execution on the cluster service thread. No `synchronized` blocks, no lock contention, no JVM safepoints on the trading path.
- **Golden-source order state.** `oms-core` is the single authoritative store for all order state — parent and child. No component outside `oms-core` creates, mutates, or persists order records.
- **Transparent failover.** Aeron Cluster replicates all state transitions via Raft. If the leader node fails, a follower elects and resumes processing automatically — no data loss, no human intervention.
- **Zero single points of failure.** Three-node quorum, periodic snapshots every 5 minutes, algo-sor disconnect detection via image handlers, durable archive storage outside `/tmp`.
- **Explicit wiring, no frameworks.** No Spring, no CDI, no reflection. All wiring is explicit constructor injection in `OmsLauncher.main()`.

---

## Architecture

### Module Map

```
market-oms/
├── oms-codec/       Binary contract — shared flyweights, field offsets, ClusterMessageType
├── oms-config/      Centralised configuration — OmsConfig, ConfigSource, ChainedConfigSource
├── oms-core/        Order state machine, validation, Aeron Cluster service, OmsNode entry point
├── algo-sor/        Algo execution engines and Smart Order Router (pure computation, stateless)
├── oms-launcher/    Process entry point for algo-sor — wires AlgoSorAgent, starts AgentRunner
└── oms-harness/     Black-box integration test harness — injects orders via Aeron Cluster client API
```

**Hard module boundary:** `algo-sor` must never depend on `oms-core` or `oms-config`. The only shared module is `oms-codec`. All inter-module communication is over Aeron IPC at runtime.

### Component Interactions

```
Harness / FIX Client
        │
        │  Aeron Cluster ingress (aeron:udp, port 9000)
        │  Wire: ClusterMessageType byte + 128-byte OrderLayout
        ▼
Aeron Cluster (Raft — single node dev, 3-node production)
  • Commits message to replicated log across quorum
  • onSessionMessage() fires only after commit
        │
        ▼
oms-core  OmsClusteredService  (cluster service thread)
  ┌──────────────────────────────────────────────────────┐
  │  1. ValidationEngine  — 8 fast-fail rules + dedup    │
  │  2. OrderBook         — off-heap parent order store  │
  │  3. OrderStateMachine — state transitions            │
  │  4. ChildOrderIntentValidator — 7 pre-child rules    │
  │  5. ChildOrderRegistry — off-heap child order store  │
  └──────────────────────────────────────────────────────┘
        │ Aeron IPC stream 30 (parent orders for routing)
        ▼
algo-sor  AlgoSorAgent  (separate AgentRunner thread)
  • SmartOrderRouter    — splits qty across venues by price
  • IcebergAlgoEngine   — display-qty (iceberg) slicing
  • TwapAlgoEngine      — time-interval slicing
  • Publishes ChildOrderIntent back to oms-core — NEVER creates child state
        │ Aeron IPC stream 12 (ChildOrderIntents)
        ▼
oms-core  onChildOrderIntent
  • ChildOrderIntentValidator — validates against live parent state
  • ChildOrderRegistry.createChild() — child born here and only here
  • OrderStateMachine.transitionToRouting() — parent advances to ROUTING
        │ Aeron IPC stream 10 (FIX Binary NOS)
        ▼
FIX Connectivity Layer → Venue / Exchange
        │ Aeron IPC stream 11 (FIX Binary exec reports)
        ▼
oms-core  handleExecReport  →  aggregates fills  →  egress to client
```

### Why algo-sor Sends Intents, Not Orders

`algo-sor` is a pure computation engine. It calculates *what* should be routed; `oms-core` decides *whether* to honour the instruction based on current live state. By the time a `ChildOrderIntent` arrives, the parent may have received fills, a cancel, or a full fill. The `ChildOrderIntentValidator` enforces seven rules — including an over-allocation guard — before any child record is created. This makes `algo-sor` stateless between work cycles and means all durable state lives in the replicated cluster.

### Aeron IPC Channel Map

| Stream | Direction | Content | Size |
|--------|-----------|---------|------|
| 30 | `oms-core` → `algo-sor` | Accepted parent orders (`ClusterMessageType` + `OrderLayout`) | 129 bytes |
| 12 | `algo-sor` → `oms-core` | `ChildOrderIntent` messages (8-byte header + 56-byte intent) | 64 bytes |
| 10 | `oms-core` → FIX bridge | FIX Binary NOS / cancel / replace | 76 bytes |
| 11 | FIX bridge → `oms-core` | FIX Binary execution reports | 76 bytes |

### Cluster Ingress Wire Format

```
[0]      msgType  : byte         — 1=NEW_ORDER, 2=CANCEL_ORDER, 3=REPLACE_ORDER
[1..128] payload  : OrderLayout  — 128-byte order record at OrderLayout offsets
```

Total: 129 bytes (`ClusterMessageType.IPC_MESSAGE_SIZE`). Exec report responses use the same framing sent back via `ClientSession.offer()`.

`FIXMessageDecoder` handles only the FIX bridge → oms-core path (stream 11); it is never used for cluster ingress/egress.

### Order State Model

```
             ┌─────────┐
       ──────►   NEW   ├───────────────────────────────► REJECTED
             └────┬────┘
                  │ first ChildOrderIntent accepted
                  ▼
             ┌─────────┐
             │ ROUTING │◄─── (additional child intents)
             └────┬────┘
                  │ first fill aggregated
                  ▼
           ┌──────────────┐
   ┌──────►│  PARTIALLY   │
   │       │   FILLED     ├────────────────────────────► CANCELED
   │       └──────┬───────┘           (via PENDING_CANCEL)
   │ fills        │ leavesQty == 0
   └──────────────▼
             ┌─────────┐
             │ FILLED  │  (terminal)
             └─────────┘
```

`ROUTING` is a parent-only extension state (`ParentOrderState.ROUTING = 10`). Base states (`OrderState`, bytes 0–9) are shared by both parent and child orders. `OrderStateMachine.TRANSITION_TABLE` is `byte[16][10]`; `ParentOrderState.registerTransitions()` must be called before any `AgentRunner` starts.

### Memory Layout

All orders — parent and child — are stored as contiguous 128-byte binary records in pre-allocated off-heap `UnsafeBuffer` instances. Field access is a direct memory read/write at a computed byte offset. No `Order` object is ever allocated on the hot path.

```
OrderLayout (128 bytes — 2 × 64-byte cache lines):

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

Prices are stored as `long` scaled by `10,000`. `£150.00` → `1_500_000L`. No `double`, no `BigDecimal` on the hot path.

### Failover and Recovery

Each node runs as an Aeron Cluster member. On failover, the new leader automatically:

1. Calls `onStart()` with the most recent snapshot image
2. Restores `OrderBook`, `ChildOrderRegistry`, and `ValidationEngine.seenClOrdIds` from the snapshot
3. Recomputes `nextOrderId` by scanning both `OrderBook` and `ChildOrderRegistry` for the max
4. Re-publishes all `NEW` / `ROUTING` / `PARTIALLY_FILLED` parent orders to algo-sor so in-flight routing resumes (`ChildOrderIntentValidator` rule 7 prevents over-slicing)
5. Replays the Raft log from the snapshot position to catch up committed messages after the snapshot
6. Begins accepting new client sessions

**Snapshot format (version 2):**
```
[0-3]   version (int) = 2
[4-7]   orderCount (int)
[8-11]  dedupCount (int)
[12-15] reserved = 0
[16 .. 16+orderCount×80]     parent order records (80 bytes each)
[.. + dedupCount×16]         dedup entries (clOrdId:8 + orderId:8)
[.. + 4 + childCount×128]    child registry (self-describing: int count + 128-byte records)
```

Max snapshot size ≈ 10.5 MB, pre-allocated at startup. Snapshots are triggered every 5 minutes via a Raft timer in `OmsClusteredService`, keeping log replay on recovery bounded to at most 5 minutes of committed messages.

**Archive state layout:**
- **Archive dir** (`OMS_ARCHIVE_DIR`) — Raft log recordings, archive catalog
- **Cluster dir** (`$OMS_ARCHIVE_DIR/cluster`) — `node-state.dat`, `recording.log`, cluster mark files

Both must be wiped together when doing a clean restart. `OMS_ARCHIVE_DELETE_ON_START=true` wipes both. Deleting only the archive dir while leaving the cluster dir causes the ConsensusModule to replay the full Raft log from `recording.log`.

---

## Design Documentation

All design facts are in `design/`. Load `design/INDEX.md` at the start of any task to find which file to read.

| File | Owns |
|------|------|
| `INDEX.md` | Task-to-file routing, file ownership table, post-change verification |
| `principles.md` | Design rules, module boundaries, zero-GC contract |
| `checklist.md` | Gate questions before any change |
| `state-model.md` | State constants, transition table, invariants |
| `wire-formats.md` | OrderLayout offsets, ChildOrderIntentFlyweight layout, FIX Binary format |
| `components-codec.md` | Contracts for oms-codec components |
| `components-core.md` | Contracts for oms-core and algo-sor components |
| `data-flows.md` | Aeron stream IDs, message flow diagrams |
| `sequences.md` | Idempotency mechanisms, sequencing boundaries, recovery sequence |
| `failover.md` | Snapshot wire format, recovery step sequence, archive durability |
| `decisions.md` | Design decision log |
| `glossary.md` | Domain terms and constant values |
| `resilience-review.md` | Structured resilience audit: data loss gaps, SPOF analysis, satisfied invariants |

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| JDK | 21 | OpenJDK 21 or GraalVM 21 |
| Gradle | wrapper | Use `./gradlew` — wrapper is committed |
| OS | Linux or macOS | See macOS note below |
| RAM | 4 GB minimum | 8 GB recommended (off-heap buffers + MediaDriver) |

**macOS.** `/dev/shm` does not exist on macOS. `OMS_AERON_DIR` defaults to `/dev/shm/oms-aeron-<nodeId>`. All scripts set `OMS_AERON_DIR=/tmp/oms-aeron-...` automatically. Latency figures will not reflect production.

**JVM flags.** All modules that use Agrona require these `--add-opens` flags (already set in each module's `build.gradle`):
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

# Verify module boundary: oms-codec must not depend on aeron-cluster
./gradlew :oms-codec:dependencies --configuration compileClasspath | grep aeron-cluster
# Expected: no output
```

---

## Running Locally

Use the provided shell scripts rather than `./gradlew run`. The scripts handle build, directory setup, startup ordering, and clean shutdown.

### Full stack (OmsNode + AlgoSorAgent)

```bash
./run-local.sh
```

Builds both modules, starts OmsNode, waits for the MediaDriver to be ready (polls for `cnc.dat`), then starts AlgoSorAgent. Ctrl-C shuts both down cleanly.

```
Building all modules...
Build OK.
Starting OmsNode (node 0)... logging to /tmp/oms-node.log
  Waiting for Aeron MediaDriver... ready.
Starting AlgoSorAgent... logging to /tmp/oms-launcher.log
================================================================
  OMS stack is running.
  Node log    : tail -f /tmp/oms-node.log
  Launcher log: tail -f /tmp/oms-launcher.log
  Press Ctrl-C to stop.
================================================================
```

Expected in `/tmp/oms-node.log` after leader election:
```
INFO  OmsNode - Starting OMS node 0 with 4 permitted symbols (deleteArchiveOnStart=false)
INFO  OmsNode - OMS node 0 is running. Waiting for shutdown signal...
INFO  OmsClusteredService - OMS cluster role changed: FOLLOWER → LEADER
INFO  OmsClusteredService - New leadership term: leaderMemberId=0 termId=0
```

### Components individually

```bash
# Terminal 1 — node first
./run-oms-node.sh

# Terminal 2 — algo-sor once node is ready
./run-algo-sor.sh
```

`run-algo-sor.sh` connects over Aeron IPC. The node must be running before the launcher starts.

### Configuration

#### How configuration works — `oms-config` module

All capacity and tuning values are resolved through the `oms-config` module rather than being read ad-hoc from environment variables in each entry point. The design has three layers:

```
ConfigSource (interface)
  ├── EnvVarConfigSource     — reads OS environment variables (default)
  ├── PropertiesFileConfigSource — reads a .properties file
  └── ChainedConfigSource    — tries sources in order; first non-null value wins
            │
            ▼
      OmsConfig              — immutable value object; validates all fields;
                               logs every resolved value at INFO with its source name
```

At startup each entry point calls `OmsConfig.load(ConfigSource)`, which:
1. Resolves every field from the supplied source
2. Throws `IllegalArgumentException` immediately for any invalid value (zero, negative, non-numeric, or blank symbol list) — the node never starts with a bad config
3. Logs each field at INFO so the active configuration is always auditable:
```
[OmsConfig] OMS_MAX_ORDERS = 65536  (source: env)
[OmsConfig] OMS_MAX_VENUES = 20     (source: file:config/oms-node.properties)
```

#### Option 1 — Environment variables (default)

No extra setup required. Set any subset of the variables below; unset variables use the built-in defaults.

#### Option 2 — Properties file

Create a `.properties` file (copy from `config/oms-node.properties.example`) and point `OMS_CONFIG_FILE` at it:

```bash
OMS_CONFIG_FILE=config/oms-node.properties ./run-oms-node.sh
```

File values take priority over OS environment variables via `ChainedConfigSource`. Keys in the file use the same names as the environment variables (`OMS_MAX_ORDERS=131072`, etc.).

---

#### OmsNode (`oms-core`) — all variables

**Aeron infrastructure** (read directly in `OmsNode`; not part of `OmsConfig`):

| Variable | Default | Description |
|----------|---------|-------------|
| `OMS_NODE_ID` | `0` | Integer node index within the cluster |
| `OMS_AERON_DIR` | `/dev/shm/oms-aeron-<nodeId>` | Aeron MediaDriver shared memory. Use `/tmp/...` on macOS |
| `OMS_ARCHIVE_DIR` | `~/oms-archive-<nodeId>` | Aeron Archive (Raft log + snapshots). Must survive reboots — never use `/tmp` in production |
| `OMS_CLUSTER_MEMBERS` | `0,localhost:9000,...` | Aeron Cluster member string — see format below |
| `OMS_CLUSTER_INGRESS_CHANNEL` | `aeron:udp?endpoint=localhost:9000` | Endpoint where the leader accepts client sessions |
| `OMS_ARCHIVE_CONTROL_CHANNEL` | `aeron:udp?endpoint=localhost:8010` | Archive bind channel (UDP) |
| `OMS_ARCHIVE_LOCAL_CONTROL_CHANNEL` | `aeron:ipc` | Archive request channel for in-process clients (must be IPC when archive is embedded) |
| `OMS_ARCHIVE_LOCAL_RESPONSE_CHANNEL` | `aeron:ipc` | Archive response channel for in-process clients |
| `OMS_ARCHIVE_REPLICATION_CHANNEL` | `aeron:udp?endpoint=localhost:0` | Endpoint peers use for log replication. Use a fixed port in multi-node |
| `OMS_ARCHIVE_DELETE_ON_START` | `false` | `true` wipes all archive and cluster state on restart. Use only for test environments |

**Capacity and tuning** (resolved via `OmsConfig`; also configurable via properties file):

| Variable | Default | Description |
|----------|---------|-------------|
| `OMS_CONFIG_FILE` | _(not set)_ | Path to a `.properties` file. File values take priority over env vars. See `config/oms-node.properties.example` |
| `OMS_MAX_ORDERS` | `65536` | Maximum parent orders held in `OrderBook` (off-heap pre-allocation). Requires `OMS_ARCHIVE_DELETE_ON_START=true` once after changing |
| `OMS_MAX_CHILDREN` | `32768` | Maximum child orders held in `ChildOrderRegistry` (off-heap pre-allocation). Same migration rule as `OMS_MAX_ORDERS` |
| `OMS_INTENT_FRAGMENT_LIMIT` | `20` | Maximum `ChildOrderIntent` fragments polled per work cycle by `OmsClusteredService` |
| `OMS_MAX_NOTIONAL` | `10000000` | Per-order notional limit in base currency units |
| `OMS_SYMBOLS` | `AAPL,MSFT,GOOG,AMZN` | Comma-separated permitted symbols whitelist |

> **Production archive dir.** The default `~/oms-archive-<nodeId>` survives reboots. For production, set `OMS_ARCHIVE_DIR` to a dedicated persistent mount (separate disk from the OS, monitored for space). Never point this at `/tmp` or any tmpfs path.

> **Snapshot migration.** Snapshot format is now version 3 (header extended to 24 bytes — embeds `snapshotMaxOrders` and `snapshotMaxChildren` at offsets 16 and 20). On first deployment from a pre-v3 build, or after changing `OMS_MAX_ORDERS`/`OMS_MAX_CHILDREN`: set `OMS_ARCHIVE_DELETE_ON_START=true` for one restart, then revert to `false`.

**`OMS_CLUSTER_MEMBERS` format:**
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

#### AlgoSorAgent (`oms-launcher`) — all variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OMS_AERON_DIR` | `/dev/shm/oms-aeron-launcher` | Aeron dir for the launcher process (must differ from OmsNode's dir) |
| `OMS_CONFIG_FILE` | _(not set)_ | Path to a `.properties` file; same semantics as for OmsNode above |
| `OMS_ALGO_FRAGMENT_LIMIT` | `10` | Maximum parent-order fragments polled per work cycle by `AlgoSorAgent` |
| `OMS_MAX_VENUES` | `10` | Maximum venues `SmartOrderRouter` can split across (off-heap arrays pre-allocated at this size) |

---

## Tests

There are no unit tests. Test coverage is provided exclusively by the black-box integration test harness in `oms-harness`.

### Quick start

```bash
./run-harness.sh
```

This script:
1. Builds all modules
2. Wipes Aeron and archive directories (`OMS_ARCHIVE_DELETE_ON_START=true`)
3. Starts OmsNode and waits up to 60 s for its MediaDriver
4. Starts AlgoSorAgent and waits 3 seconds for it to connect
5. Runs all harness scenarios in the foreground
6. Prints a pass/fail summary; exits 0 on full pass
7. Kills OmsNode and AlgoSorAgent cleanly on exit regardless of outcome

Expected output on success:
```
Building all modules...
Build OK.
Starting OmsNode (node 0)... logging to /tmp/oms-node-harness.log
  Waiting for OmsNode MediaDriver... ready.
Starting AlgoSorAgent... logging to /tmp/oms-launcher-harness.log

Running test harness...
================================================================
  HARNESS RESULTS:  5 passed,  0 failed
  Exec reports received: 55
  Filled: 0  Canceled: 0  Rejected: 2
================================================================
ALL SCENARIOS PASSED
```

### Harness scenarios

| # | Scenario | What it validates |
|---|----------|-----------------|
| 1 | Single order accepted | Valid `NEW_ORDER` reaches `state=NEW`; exec report arrives via cluster egress |
| 2 | Order rejected — zero qty | `qty=0` returns `state=REJECTED` by `ValidationEngine` |
| 3 | Order rejected — unknown symbol | Symbol not in `OMS_SYMBOLS` whitelist returns `state=REJECTED` |
| 4 | Cancel request | Order accepted then cancel reaches `state=PENDING_CANCEL` |
| 5 | Bulk injection (50 orders) | All 50 orders accepted and acknowledged within timeout |

### Harness environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HARNESS_AERON_DIR` | `/tmp/oms-aeron-harness` | Aeron dir for the harness process (must differ from node and launcher) |
| `HARNESS_CLUSTER_INGRESS` | `0=localhost:9000` | Cluster ingress in `nodeId=host:port` format (bare `host:port` is rejected by Aeron) |
| `HARNESS_TIMEOUT_MS` | `8000` | Per-scenario wait timeout in milliseconds |

### Running against a remote node

```bash
HARNESS_CLUSTER_INGRESS="0=10.0.0.5:9000" ./run-harness.sh
```

### Harness logs

| File | Content |
|------|---------|
| `/tmp/oms-node-harness.log` | OmsNode startup, leadership election, order processing |
| `/tmp/oms-launcher-harness.log` | AlgoSorAgent startup and intent publication |

---

## Production Deployment (Three-Node Cluster)

### Overview

Run three `OmsNode` processes on separate hosts. All three must share the same `OMS_CLUSTER_MEMBERS` string. Raft elects one leader; the other two replicate every committed log entry in real-time. A leader crash triggers a new election in under a second (configurable heartbeat/election timeouts).

### Per-node startup

Use `run-oms-node.sh` with node-specific overrides. Example for a 3-node cluster on hosts `10.0.1.1`, `10.0.1.2`, `10.0.1.3`:

```bash
# All three nodes share the same OMS_CLUSTER_MEMBERS value:
MEMBERS="0,10.0.1.1:9000,10.0.1.1:9001,10.0.1.1:9002,10.0.1.1:9003,10.0.1.1:8010|\
1,10.0.1.2:9000,10.0.1.2:9001,10.0.1.2:9002,10.0.1.2:9003,10.0.1.2:8010|\
2,10.0.1.3:9000,10.0.1.3:9001,10.0.1.3:9002,10.0.1.3:9003,10.0.1.3:8010"

# Node 0 (on host 10.0.1.1):
OMS_NODE_ID=0 \
OMS_AERON_DIR=/dev/shm/oms-aeron-0 \
OMS_ARCHIVE_DIR=/mnt/oms-data/archive-0 \
OMS_CLUSTER_MEMBERS="$MEMBERS" \
OMS_CLUSTER_INGRESS_CHANNEL="aeron:udp?endpoint=10.0.1.1:9000" \
OMS_ARCHIVE_CONTROL_CHANNEL="aeron:udp?endpoint=10.0.1.1:8010" \
OMS_ARCHIVE_REPLICATION_CHANNEL="aeron:udp?endpoint=10.0.1.1:8020" \
./run-oms-node.sh
```

Repeat for nodes 1 and 2, substituting `10.0.1.2` / `10.0.1.3` and `OMS_NODE_ID=1` / `2`.

### AlgoSorAgent

Start one `oms-launcher` process per physical host (or on the same host as the local OmsNode). `AlgoSorAgent` connects to its co-located OmsNode over Aeron IPC; it does not need network access to follower nodes.

```bash
OMS_AERON_DIR=/dev/shm/oms-aeron-launcher ./run-algo-sor.sh
```

### Verifying leadership

Watch the logs of all three nodes. The elected leader logs:
```
INFO  OmsClusteredService - OMS cluster role changed: FOLLOWER → LEADER
```
The other two log `FOLLOWER`.

### Client connectivity

The harness (or FIX bridge) connects to `OMS_CLUSTER_INGRESS_CHANNEL` of the current leader. Aeron Cluster sessions are automatically redirected if the leader changes — reconnect to the new leader's endpoint after the election completes.

### Operational checklist

- [ ] `OMS_ARCHIVE_DIR` points to a durable filesystem (not `/tmp`, not tmpfs)
- [ ] Archive dir has adequate disk space (Raft log + snapshots, at least 20 GB)
- [ ] `OMS_ARCHIVE_REPLICATION_CHANNEL` uses a fixed port reachable by peer nodes
- [ ] Three nodes are on separate physical hosts or failure domains
- [ ] `OMS_ARCHIVE_DELETE_ON_START=false` (default) — do not set to true in production
- [ ] `OMS_SYMBOLS` and `OMS_MAX_NOTIONAL` are identical on all three nodes
- [ ] Monitor logs for `WARN algo-sor disconnected` (P6 — algo-sor IPC image loss)

### Snapshot management

Snapshots are taken automatically every 5 minutes by a Raft timer. To force an immediate snapshot (e.g. before planned maintenance):

```bash
# Using the Aeron ClusterTool (in the aeron-cluster jar)
java -cp oms-core/build/install/oms-core/lib/aeron-cluster-*.jar \
     io.aeron.cluster.ClusterTool \
     $OMS_ARCHIVE_DIR/cluster snapshot
```

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
| `org.junit.jupiter:junit-jupiter` | 5.11.4 |

All versions are declared in `gradle/libs.versions.toml`.

---

## Key Design Decisions

### Why Aeron Cluster and not Kafka?

Kafka's consumer model is pull-based — polling latency adds on top of replication. Aeron Cluster's `onSessionMessage()` fires only after quorum commit: processing latency equals replication latency with nothing added. For a state machine that must guarantee ordering and exactly-once processing, Aeron Cluster's model is correct by construction.

### Why algo-sor is a separate process

Embedding algo engines in the `ClusteredService` would replicate ephemeral slice-scheduling state across Raft nodes unnecessarily and couple SOR algorithm changes to cluster restarts. The intent-based separation means `algo-sor` can be upgraded, restarted, or replaced independently. Its in-process state (IcebergAlgoEngine, TwapAlgoEngine) is deliberately non-durable; `ChildOrderIntentValidator` rule 7 and `republishRoutingOrders()` on leader promotion handle recovery transparently.

### Why fixed-point longs for prices

`double` arithmetic is non-deterministic across JVM implementations and CPU architectures — the same computation can produce different results on different nodes, corrupting the replicated state machine. `BigDecimal` allocates on every operation. A `long` scaled by `10,000` provides four decimal places of price precision (sufficient for equities), is deterministic everywhere, and adds zero GC pressure.

### Why ClusterMessageType framing on cluster ingress, not FIX Binary

`FIXMessageDecoder` operates on a compact FIX Binary format produced by an upstream FIX engine (stream 11). Using FIX Binary on the cluster ingress would couple the cluster wire protocol to a specific FIX engine's internal format. `ClusterMessageType` + `OrderLayout` is self-contained within `oms-codec` and lets any client (harness, FIX bridge, test tool) submit orders without FIX knowledge.

### Why ChildOrderRegistry is snapshotted separately from OrderBook

`OrderBook` uses `OrderLayout.MESSAGE_SIZE` (80 bytes) per parent record in the snapshot to minimise snapshot size. `ChildOrderRegistry` uses full `BLOCK_LENGTH` (128 bytes) per child record because children carry sibling-link fields (`nextSiblingSlot`, `parentOrFirstChildId`) that are essential for fill aggregation. Mixing them into one store would require padding all parent records to 128 bytes, adding ~3 MB to the snapshot for no benefit.

---

## Licence

Proprietary. All rights reserved.
