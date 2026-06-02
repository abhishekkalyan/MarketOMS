# Binary Wire Formats
<!-- Load when: touching OrderLayout, OrderFlyweight,
     ChildOrderIntentFlyweight, FIXMessageDecoder, or FIXMessageEncoder. -->

## Section 7 — Binary Wire Formats

### 7.1 OrderLayout (128-byte order record)

| Offset | Size | Type | Constant | Applies To | Semantics |
|--------|------|------|----------|-----------|-----------|
| 0 | 8 | long | `ACCOUNT_ID_OFFSET` | both | Internal account identifier |
| 8 | 8 | long | `CL_ORD_ID_OFFSET` | both | Client-assigned order ID (FIX tag 11) |
| 16 | 8 | long | `ORDER_ID_OFFSET` | both | Venue-assigned order ID (FIX tag 37); `NULL_ID` until `EXEC_NEW` |
| 24 | 8 | long | `ORIG_CL_ORD_ID_OFFSET` | both | Original ClOrdID for cancel/replace (FIX tag 41) |
| 32 | 8 | long | `SYMBOL_OFFSET` | both | ASCII ticker packed as long, big-endian, left-aligned, space-padded |
| 40 | 8 | long | `PRICE_OFFSET` | both | Limit price × `PRICE_MULTIPLIER (10_000)` |
| 48 | 8 | long | `QTY_OFFSET` | both | Total order quantity |
| 56 | 8 | long | `FILLED_QTY_OFFSET` | both | Cumulative filled quantity (FIX tag 14) |
| 64 | 8 | long | `LEAVES_QTY_OFFSET` | both | Remaining open quantity (FIX tag 151) |
| 72 | 1 | byte | `SIDE_OFFSET` | both | `Side` constants |
| 73 | 1 | byte | `TIME_IN_FORCE_OFFSET` | both | `TimeInForce` constants |
| 74 | 1 | byte | `ORDER_STATE_OFFSET` | both | `OrderState` constants |
| 75 | 1 | byte | `RESERVED_OFFSET` | both | Future flags (zero-fill) |
| 76 | 4 | int | `VENUE_ID_OFFSET` | both | Routing target (maps to Aeron publication stream) |
| 80 | 8 | long | `TRANSACT_TIME_OFFSET` | both | Last state-change timestamp (nanos from `System.nanoTime()`) |
| 88 | 8 | long | `RESERVED1_OFFSET` | both | Reserved (zero-fill) |
| 96 | 8 | long | `RESERVED2_OFFSET` | both | Reserved (zero-fill) |
| 104 | 8 | long | `RESERVED3_OFFSET` | both | Reserved (zero-fill) |
| 112 | 4 | int | `OFFSET_CHILD_COUNT` | parent | Number of live children; 0 on child orders |
| 116 | 4 | int | `OFFSET_NEXT_SIBLING_SLOT` | child | Slot index of next sibling; -1 = end of list |
| 120 | 8 | long | `OFFSET_PARENT_OR_FIRST_CHILD_ID` | both | Parent: orderId of first child (0=none). Child: orderId of its parent |
| **128** | — | — | `BLOCK_LENGTH` / `MESSAGE_SIZE` | — | End of record; static assert enforces this |

`NULL_ID = Long.MIN_VALUE`; `PRICE_MULTIPLIER = 10_000L`

### 7.2 ChildOrderIntentFlyweight (56-byte intent record)

| Offset | Size | Type | Field |
|--------|------|------|-------|
| 0 | 8 | long | `parentOrderId` |
| 8 | 8 | long | `parentClOrdId` |
| 16 | 8 | long | `sliceQty` |
| 24 | 8 | long | `limitPrice` (fixed-point × 10_000) |
| 32 | 4 | int | `venueId` |
| 36 | 1 | byte | `algoType` (1=SOR, 2=ICEBERG, 3=TWAP) |
| 37 | 1 | byte | `sliceIndex` (0-based, wraps at 255) |
| 38 | 2 | short | `_pad` (alignment) |
| 40 | 8 | long | `intentTimestampNanos` |
| 48 | 8 | long | `_reserved` (zero-fill) |
| **56** | — | — | `BLOCK_LENGTH` |

### 7.3 Price encoding

- `PRICE_MULTIPLIER = 10_000L` (from `OrderLayout`)
- `£12.3456` stored as `123456L`
- Maximum representable: `Long.MAX_VALUE / 10_000 = 922_337_203_685_477`
- Why not `double`: non-deterministic across CPU architectures — identical computations produce different results on different Raft replicas
- Why not `BigDecimal`: every operation allocates a new `BigDecimal` on the heap
- Notional check: `price > (maxNotional * PRICE_MULTIPLIER) / qty` (overflow-safe integer division)

### 7.4 Symbol encoding

From `OrderFlyweight.encodeSymbol(String)`:
- Initialized to `0x2020202020202020L` (8 ASCII spaces)
- Left-aligned, right-padded with `0x20`; big-endian byte packing
- Each character packed: `encoded = (encoded & ~(0xFFL << ((7-i)*8))) | (ch << ((7-i)*8))`
- Hot-path comparison: single `long` equality check — no String allocation
- Off-hot-path only: called at startup (`OmsNode.parseSymbols()`) and at order creation setup

### 7.5 FIX Binary inbound format (76 bytes)

From `FIXMessageDecoder` field offsets:

| Offset | Size | Field | FIX Tag |
|--------|------|-------|---------|
| 0 | 1 | `msgType` | MsgType (35); `'D'=NOS, 'F'=Cancel, 'G'=Replace, '8'=ExecReport` |
| 1 | 1 | `side` | 54 |
| 2 | 1 | `timeInForce` | 59 |
| 3 | 1 | `execType` | 150 (ExecReport only) |
| 4 | 8 | `clOrdId` | 11 |
| 12 | 8 | `origClOrdId` | 41 |
| 20 | 8 | `orderId` | 37 |
| 28 | 8 | `accountId` | 1 |
| 36 | 8 | `symbol` | 55 (packed ASCII long) |
| 44 | 8 | `price` | 44 (fixed-point × 10_000) |
| 52 | 8 | `orderQty` | 38 |
| 60 | 8 | `lastQty` | 32 |
| 68 | 8 | `leavesQty` | 151 |
| **76** | — | — | `FIX_BINARY_SIZE` |

### 7.6 Cluster ingress/egress wire format (129 bytes)

```
[0]      msgType  : byte         — ClusterMessageType constant (1=NEW_ORDER, 2=CANCEL_ORDER, 3=REPLACE_ORDER)
[1..128] payload  : OrderLayout  — 128-byte order record (OrderLayout field offsets)
```
Total: `IPC_MESSAGE_SIZE = 129`.
