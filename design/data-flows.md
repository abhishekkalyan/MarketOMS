# Data Flow Diagrams
<!-- Load when: changing Aeron stream IDs, IPC channel direction,
     message routing, or any component that publishes or subscribes. -->

## Section 5 — Data Flow Diagrams

### Diagram 5.1: Inbound new order — client to venue

```
FIX Client (TCP)
       │ FIX tag=value ASCII
       ▼
FIX Connectivity Engine (as Aeron Cluster client)
  • sends ClusterMessageType-framed message (129 bytes)
       │ Aeron Cluster ingress (UDP 9000) → Raft quorum commit
       ▼
OmsClusteredService.onSessionMessage()
  ┌─────────────────────────────────────────────────────────┐
  │  1. pollIntents() — drain any pending ChildOrderIntents │
  │  2. msgType = buffer.getByte(offset + OFFSET_MSG_TYPE)  │
  │  3. handleNewOrderSingle()                              │
  │     a. orderBook.allocateSlot() → slot (or -1 if full) │
  │     b. orderBook.wrapFlyweight(slot) — populate fields  │
  │        from buffer at OFFSET_PAYLOAD                    │
  │     c. order.orderId(nextOrderId++), state=NEW          │
  │     d. validationEngine.validateNewOrder() → int VALID  │
  │     e. orderBook.index(slot, clOrdId, orderId)          │
  │     f. validationEngine.registerAccepted(clOrdId,ordId) │
  │     g. algoSorPublication.offer(algoOutbound, 0, 129)   │
  │        stream 30: NEW_ORDER(1) + 128-byte OrderLayout   │
  │     h. sendEgressExecReport(session, NEW_ORDER, order)  │
  └─────────────────────────────────────────────────────────┘
       │ Aeron IPC stream 30
       ▼
AlgoSorAgent.doWork() → parentOrderSub.poll(this, 10)
  onFragment():
    payloadBuffer.wrap(buffer, offset + OFFSET_PAYLOAD, 128)
    parentView.wrap(payloadBuffer, 0)
    SmartOrderRouter.route(parentView, venueIds, venuePrices, venueQtys, count, this::publishIntent)
      → for each venue: intent.set*() + sink.onIntent(intent)
    publishIntent(intent):
      intent.copyTo(intentOfferBuffer, HEADER_LENGTH=8)
      intentPub.offer(intentOfferBuffer, 0, 64)  [8 header + 56 intent]
       │ Aeron IPC stream 12
       ▼
OmsClusteredService.handleIntentFragment() [called from pollIntents()]
  ┌─────────────────────────────────────────────────────────┐
  │  1. intentView.wrapReadOnly(buffer, offset+HEADER_LENGTH)│
  │  2. parentOrderId = intentView.getParentOrderId()       │
  │  3. parentSlot = orderBook.slotByOrderId(parentOrderId) │
  │  4. parent = orderBook.wrapFlyweight(parentSlot)        │
  │  5. liveChildQty = childRegistry.computeLiveChildQty()  │
  │  6. intentValidator.validate() → byte PASS=0            │
  │  7. childOrderId = nextOrderId++                        │
  │  8. child = childRegistry.createChild(intent, parent, …)│
  │  9. if parent.state==NEW: OrderStateMachine             │
  │        .transitionToRouting(parent)                     │
  │ 10. fixEncoder.sendNewOrderSingle(child) → stream 10   │
  └─────────────────────────────────────────────────────────┘
       │ Aeron IPC stream 10
       ▼
FIX Connectivity Engine → Venue / Exchange
```

### Diagram 5.2: Venue fill → client execution report

```
Venue / Exchange
       │ FIX ExecReport (35=8)
       ▼
FIX Connectivity Engine
       │ FIX Binary ExecReport (76 bytes), Aeron IPC stream 11
       ▼
OmsClusteredService.handleExecReport()
  execType = buffer.getByte(offset + EXEC_TYPE_OFFSET_IN)
  event = FIXMessageDecoder.mapExecTypeToEvent(execType)
  clOrdId = buffer.getLong(offset + CL_ORD_ID_OFFSET_IN)
  child = childRegistry.getByClOrdId(clOrdId)
  if child != null:
    lastQty = FIXMessageDecoder.getLastQty(buffer, offset)
    parent = childRegistry.applyFillAndAggregate(clOrdId, lastQty, 0, orderBook)
      1. clOrdIdToOrderId.get(childClOrdId) → childOrderId
      2. orderIdToSlot.get(childOrderId)    → childSlot
      3. sharedFlyweight.wrap(store, childSlot * 128)
      4. sharedFlyweight.applyFill(lastQty)
      5. if leavesQty==0: child.orderState(FILLED)
      6. parentOrderId = sharedFlyweight.parentOrFirstChildId()
      7. parentSlot = orderBook.slotByOrderId(parentOrderId)
      8. secondFlyweight.wrap(parentBook.buffer(), parentBook.offsetForSlot(parentSlot))
      9. secondFlyweight.applyFill(lastQty)
     10. update parent state: FILLED if leavesQty==0, else PARTIALLY_FILLED
    fixEncoder.sendExecReport(parent, execType, lastQty) → stream 10 → client FIX engine
```

### Diagram 5.3: Aeron Cluster node topology

```
FIX Client ──[UDP 9000]──► OmsNode (node 0 — leader)
                                │  ConsensusModule (Raft)
                                │  ◄──────────────────────►  OmsNode (node 1 — follower)
                                │  ◄──────────────────────►  OmsNode (node 2 — follower)
                                │
                           OmsClusteredService
                           (onSessionMessage after Raft quorum commit)
                                │
                    ┌───────────┴────────────┐
                    │                        │
               stream 30                stream 10
            (parent orders)          (NOS to FIX bridge)
                    │
              AlgoSorAgent
              (AgentRunner, dedicated thread)
                    │
               stream 12
            (ChildOrderIntents)
                    │
               OmsClusteredService
            (pollIntents → onChildOrderIntent)
```
