# Design Analysis Checklist
<!-- Load for every task. Work through all applicable questions before
     writing any code. A NO answer requires a documented exception. -->

## Section 12 — Design Analysis Checklist

Before implementing any change, answer every question YES / NO / N/A with a one-sentence justification. A NO answer requires documented exception.

**Zero-GC compliance**
- [ ] Does the change introduce any `new` expression in `onSessionMessage()`, `ValidationEngine.validateNewOrder()`, `ChildOrderIntentValidator.validate()`, `ChildOrderRegistry.createChild()`, or `AlgoSorAgent.onFragment()`?
- [ ] Does the change call `String.format()`, `toString()`, or any varargs method that creates an `Object[]` on the hot path?
- [ ] If new pre-allocated buffers are needed, are they allocated in the relevant constructor and documented in components.md?

**Golden source integrity**
- [ ] Does the change mutate order state anywhere other than `OmsClusteredService.onSessionMessage()` or `OmsClusteredService.onChildOrderIntent()`?
- [ ] If a new order record type is introduced, is it stored in `ChildOrderRegistry` or `OrderBook` and included in `SnapshotManager.takeSnapshot()`?
- [ ] Does the change preserve the invariant that `AlgoSorAgent` never calls `ChildOrderRegistry.createChild()` or any `OrderBook` mutation?

**State model correctness**
- [ ] If a new state byte constant is added, does it fit within `NUM_STATES = 16`?
- [ ] If a new state is added, is it registered via a `registerTransitions()`-style call in both `OmsLauncher.main()` and `OmsClusteredService.onStart()` before `AgentRunner` starts?
- [ ] If a new state is terminal, is it handled by `OrderState.isTerminal()` (currently `state >= FILLED`, i.e. ≥ 6)?
- [ ] Does the change preserve all invariants in state-model.md Section 6.3?

**Module boundary compliance**
- [ ] Does the change add a new compile dependency to `algo-sor` on `oms-core`?
- [ ] Does the change add Aeron Cluster to `oms-codec`?
- [ ] Will `./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core` still return no output?
- [ ] Will `./gradlew :oms-codec:dependencies --configuration compileClasspath | grep aeron-cluster` still return no output?

**Sequencing and idempotency**
- [ ] If a new message type is added to `ClusterMessageType`, does it have a corresponding dedup mechanism in `ValidationEngine` or `ChildOrderIntentValidator`?
- [ ] If a new state mutation is added to `onSessionMessage()`, is it safe when replayed by Raft on a follower that already has the state?
- [ ] If a new Aeron stream is added, is its ID declared in both `AeronTransport` and `ClusterMessageType`, and recorded in principles.md Section 3.2?

**Failover safety**
- [ ] Is all new durable state serialized in `SnapshotManager.takeSnapshot()`?
- [ ] Is the corresponding deserialization added to `SnapshotManager.handleSnapshotFragment()` and the relevant `reset()` + restore calls?
- [ ] Is `MAX_SNAPSHOT_BYTES` updated if the snapshot size grows?
