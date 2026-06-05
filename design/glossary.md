# Glossary
<!-- Load when: encountering an unfamiliar term, class name, or constant.
     Add new terms whenever a new concept is introduced by a task. -->

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
| MAX_ORDERS | 65_536 default — overridden by `OMS_MAX_ORDERS` env var; passed to `OrderBook(int)` constructor | `OrderBook.MAX_ORDERS` |
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
| FRAGMENT_LIMIT | 10 default — overridden by `OMS_ALGO_FRAGMENT_LIMIT`; max parent orders polled per `AlgoSorAgent.doWork()` cycle | `AlgoSorAgent` |
| INTENT_FRAGMENT_LIMIT | 20 default — overridden by `OMS_INTENT_FRAGMENT_LIMIT`; max intents drained per `pollIntents()` call | `OmsClusteredService` |
| OMS_MAX_CHILDREN | Env var: default 32_768 — `ChildOrderRegistry` capacity | `OmsNode`, `ChildOrderRegistry.DEFAULT_CAPACITY` |
| OMS_MAX_VENUES | Env var: default 10 — `SmartOrderRouter` + `AlgoSorAgent` venue array size | `OmsLauncher`, `SmartOrderRouter.MAX_VENUES` |
| OmsConfig | Immutable value object holding all capacity and tuning values; validated and logged at startup by `OmsConfig.load(ConfigSource)` | `oms-config` module |
| ConfigSource | Pluggable interface for raw string value resolution (`EnvVarConfigSource`, `PropertiesFileConfigSource`, `ChainedConfigSource`) | `oms-config` module |
| OMS_CONFIG_FILE | Optional env var: path to a `.properties` file; when set, file values take priority over OS env vars via `ChainedConfigSource` | `OmsNode.loadOmsConfig()`, `OmsLauncher.loadOmsConfig()` |
| PerfConfig | Immutable, validated snapshot of all `PERF_*` performance-harness configuration; mirrors OmsConfig pattern; lives in `oms-harness`, never depends on `oms-config` | `com.cobain.oms.harness.perf.PerfConfig` |
| LatencyHistogram | Zero-allocation `long[]`-backed histogram for nanosecond latency recording; `record()` is allocation-free; `print()` allocates for formatting only | `com.cobain.oms.harness.perf.LatencyHistogram` |
| PerfHarnessLauncher | Entry point for the performance test harness; runs all six benchmarks in sequence and prints the consolidated results table | `com.cobain.oms.harness.perf.PerfHarnessLauncher` |
| PERF_SAMPLE_COUNT | Env var: number of orders to send in the latency measurement phase; default 100 000 | `PerfConfig.DEFAULT_SAMPLE_COUNT` |
| OMS_MAX_ICEBERG_ORDERS | Env var: default 4_096 — max concurrent `IcebergAlgoEngine` orders whose state is tracked in `remainingQty`/`peakQty` maps; maps pre-sized to this × 2 at 0.65f to eliminate hot-path rehashing | `OmsLauncher`, `IcebergAlgoEngine` |
| OMS_MAX_TWAP_ORDERS | Env var: default 512 — max concurrent TWAP handles in `TwapAlgoEngine`; all per-handle arrays pre-allocated at this size; when exhausted, `onSlice()` falls back to single-slice and logs WARN | `OmsLauncher`, `TwapAlgoEngine.MAX_TWAP_ORDERS` |
