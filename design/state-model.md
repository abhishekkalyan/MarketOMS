# Order State Model
<!-- Load when: touching OrderStateMachine, ParentOrderState, OrderState,
     ChildOrderIntentValidator, or any orderState() call. -->

## Section 6 — Order State Model

### 6.1 Complete state inventory

| State | Byte Value | Applies To | Terminal? | Description |
|-------|-----------|-----------|----------|-------------|
| `PENDING_NEW` | 0 | child | no | Child order created, NOS dispatched to venue, awaiting EXEC_NEW |
| `NEW` | 1 | parent, child | no | Venue confirmed order is live |
| `PARTIALLY_FILLED` | 2 | parent, child | no | One or more partial fills received |
| `PENDING_CANCEL` | 3 | parent, child | no | Cancel request sent, awaiting venue ack |
| `PENDING_REPLACE` | 4 | parent | no | Replace request sent, awaiting venue ack |
| `REPLACED` | 5 | parent | no | Vendor confirmed replace; order is live again |
| `FILLED` | 6 | parent, child | **yes** | All qty filled |
| `CANCELED` | 7 | parent, child | **yes** | Cancel confirmed |
| `REJECTED` | 8 | parent, child | **yes** | New/replace rejected by venue |
| `EXPIRED` | 9 | parent | **yes** | Expired (DAY, GTD TIF) |
| `ROUTING` | 10 | **parent only** | no | Accepted; ≥ 1 children dispatched, awaiting fills |

`NUM_STATES = 16`, `NUM_EVENTS = 10`. Terminal check: `OrderState.isTerminal(state)` ≡ `state >= FILLED` (≥ 6).

### 6.2 Legal transition table

| From State | Event | To State | Registered In |
|-----------|-------|---------|--------------|
| `PENDING_NEW` | `EXEC_NEW` | `NEW` | `OrderStateMachine` static block |
| `PENDING_NEW` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PENDING_NEW` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PENDING_NEW` | `EXEC_REJECTED` | `REJECTED` | `OrderStateMachine` static block |
| `PENDING_NEW` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `NEW` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `NEW` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `NEW` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `NEW` | `REPLACE_REQUEST` | `PENDING_REPLACE` | `OrderStateMachine` static block |
| `NEW` | `EXEC_EXPIRED` | `EXPIRED` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `PARTIALLY_FILLED` | `REPLACE_REQUEST` | `PENDING_REPLACE` | `OrderStateMachine` static block |
| `PENDING_CANCEL` | `EXEC_CANCELED` | `CANCELED` | `OrderStateMachine` static block |
| `PENDING_CANCEL` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PENDING_CANCEL` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_REPLACED` | `REPLACED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `PENDING_REPLACE` | `EXEC_REJECTED` | `REJECTED` | `OrderStateMachine` static block |
| `REPLACED` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `OrderStateMachine` static block |
| `REPLACED` | `EXEC_FILL` | `FILLED` | `OrderStateMachine` static block |
| `REPLACED` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `OrderStateMachine` static block |
| `REPLACED` | `REPLACE_REQUEST` | `PENDING_REPLACE` | `OrderStateMachine` static block |
| `REPLACED` | `EXEC_EXPIRED` | `EXPIRED` | `OrderStateMachine` static block |
| `ROUTING` | `EXEC_PARTIAL_FILL` | `PARTIALLY_FILLED` | `ParentOrderState.registerTransitions()` |
| `ROUTING` | `EXEC_FILL` | `FILLED` | `ParentOrderState.registerTransitions()` |
| `ROUTING` | `CANCEL_REQUEST` | `PENDING_CANCEL` | `ParentOrderState.registerTransitions()` |
| `ROUTING` | `EXEC_REJECTED` | `REJECTED` | `ParentOrderState.registerTransitions()` |

All other (state, event) combinations return `INVALID_TRANSITION (-1)`. Terminal states (FILLED, CANCELED, REJECTED, EXPIRED) have no outbound transitions.

### 6.3 State invariants

- **PENDING_NEW:** `filledQty == 0`, `leavesQty == qty`, order exists in `ChildOrderRegistry`
- **NEW:** `filledQty == 0`, `leavesQty == qty`, `orderId` may still be `NULL_ID` until `EXEC_NEW` arrives
- **ROUTING:** `childCount >= 1`, `leavesQty > 0`, `parentOrFirstChildId != 0`, transition was via `OrderStateMachine.transitionToRouting()` which requires prior state == `NEW`
- **PARTIALLY_FILLED:** `filledQty > 0`, `leavesQty > 0`, `filledQty + leavesQty == qty`
- **PENDING_CANCEL:** cancel NOS sent via `FIXMessageEncoder.sendCancelRequest()`; child orders in `PENDING_CANCEL` via `childRegistry.cancelAllChildren()`
- **PENDING_REPLACE:** replace NOS sent via `FIXMessageEncoder.sendCancelReplace()`
- **REPLACED:** new price/qty applied in `handleCancelReplace()` before transition
- **FILLED:** `leavesQty == 0`, `filledQty == qty`; `OrderBook.unindex()` + `freeSlot()` called
