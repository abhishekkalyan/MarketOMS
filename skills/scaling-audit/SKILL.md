---
name: scaling-audit
description: >
  Run a full scaling audit on the MarketOMS codebase: find every hard-coded
  capacity limit, fixed-size pre-allocation, and tuning constant that creates
  a scaling wall; propose and implement remediations that respect the zero-GC
  hot path; bump the snapshot version if serialised layouts change; update all
  design docs; run tests; and merge to master. Use this skill whenever the user
  asks about scaling, capacity limits, MAX_ORDERS, hard-coded sizes, or wants
  to make any capacity constant configurable. Also trigger for any task that
  touches OrderBook, ChildOrderRegistry, SnapshotManager, AlgoSorAgent fragment
  limits, or ValidationEngine map sizing.
---

# Scaling Audit Skill

## Purpose
End-to-end agent workflow: audit → remediate → test → document → merge.

## Mandatory first step
Before touching any code, read these design files:
design/principles.md
design/decisions.md
design/resilience-review.md
design/components-core.md
design/components-codec.md
design/failover.md
design/checklist.md

---

## Phase 1 — Audit

Scan the entire codebase for hard-coded capacity limits, fixed-size
pre-allocations, and tuning constants that create scaling walls.

For every finding produce a structured entry:

FINDING <id>
Location    : <class>#<field or constant> (<module>)
Value       : <current value>
Wall        : <what breaks or fills up first, and at what load>
Snapshot    : yes/no — does this drive MAX_SNAPSHOT_BYTES or affect
the snapshot wire format?
Remediation : <proposed fix in one sentence>

Seed list (do not stop here — find others):

F1  OrderBook.MAX_ORDERS = 65_536
F2  ChildOrderRegistry.DEFAULT_CAPACITY = 32_768
F3  SnapshotManager.MAX_SNAPSHOT_BYTES  (derived from F1 + F2)
F4  AlgoSorAgent.FRAGMENT_LIMIT = 10
F5  OmsClusteredService.INTENT_FRAGMENT_LIMIT = 20
F6  SmartOrderRouter.MAX_VENUES = 10
F7  ValidationEngine.seenClOrdIds Long2LongHashMap initial capacity
F8  ParentOrderState.TRANSITION_TABLE dimensions (NUM_STATES, NUM_EVENTS)
F9  Hard-coded stream IDs in AeronTransport / OmsLauncher
F10 Hard-coded byte array sizes for egress/ingress buffers in
OmsClusteredService (algoOutbound, egressBuffer — 129 bytes)

Present the full findings table before proceeding to Phase 2.

---

## Phase 2 — Implement

Apply remediations in dependency order. After each logical unit:
(a) run ./gradlew test
(b) fix any failures before proceeding
(c) make a focused git commit with the message shown below

### R1 — OrderBook capacity configurable  (F1, F3)
- Add OrderBook(int maxOrders) constructor; no-arg delegates to it
- env var: OMS_MAX_ORDERS  default: 65536
- Read in OmsLauncher / OmsNode; pass to OrderBook and SnapshotManager
- SnapshotManager accepts maxOrders + maxChildren; allocates buffer at runtime
- Zero-GC: UnsafeBuffer still allocated once in constructor
  Commit: "feat(order-book): make MAX_ORDERS configurable via OMS_MAX_ORDERS"

### R2 — ChildOrderRegistry capacity configurable  (F2, F3)
- env var: OMS_MAX_CHILDREN  default: 32768
  Commit: "feat(child-registry): make DEFAULT_CAPACITY configurable via OMS_MAX_CHILDREN"

### R3 — Bump snapshot version to 3  (F3)
- SNAPSHOT_VERSION = 3
- Extend header to 24 bytes; add snapshotMaxOrders + snapshotMaxChildren at [16] and [20]
- takeSnapshot() writes both values; handleSnapshotFragment() reads them
- Add migration note to design/resilience-review.md
  Commit: "feat(snapshot): bump to version 3; embed capacity metadata in header"

### R4 — AlgoSorAgent.FRAGMENT_LIMIT configurable  (F4)
- env var: OMS_ALGO_FRAGMENT_LIMIT  default: 10
  Commit: "feat(algo-sor): make FRAGMENT_LIMIT configurable via OMS_ALGO_FRAGMENT_LIMIT"

### R5 — INTENT_FRAGMENT_LIMIT configurable  (F5)
- env var: OMS_INTENT_FRAGMENT_LIMIT  default: 20
  Commit: "feat(oms-core): make INTENT_FRAGMENT_LIMIT configurable via OMS_INTENT_FRAGMENT_LIMIT"

### R6 — SmartOrderRouter.MAX_VENUES configurable  (F6)
- env var: OMS_MAX_VENUES  default: 10
- Venue arrays still pre-allocated once in constructor
  Commit: "feat(algo-sor): make MAX_VENUES configurable via OMS_MAX_VENUES"

### R7 — ValidationEngine seenClOrdIds sized from maxOrders  (F7)
- Constructor parameter; initial capacity = maxOrders * 2, load factor 0.65
  Commit: "feat(validation): size seenClOrdIds initial capacity from maxOrders"

### R8 — Verify constants; replace any remaining literals  (F8, F9, F10)
- Confirm TRANSITION_TABLE uses NUM_STATES / NUM_EVENTS, not literals
- Confirm stream IDs are named constants in AeronTransport
- Confirm buffer sizes use ClusterMessageType.IPC_MESSAGE_SIZE
- Replace any literals found; document sharding note in design/decisions.md
  Commit: "refactor(constants): replace any remaining literal sizes with named constants"

---

## Hard constraints (never violate)

• No `new` expressions in onSessionMessage(), validateNewOrder(), validate(),
createChild(), onFragment(), publishIntent()
• No synchronized, AtomicReference, or locks on any hot-path class
• All pre-allocated buffers allocated once in constructor, reused every message
• Existing deployments with no env vars must behave identically to today
• Snapshot version must be bumped if any serialised layout changes
• ./gradlew test must be green after every individual commit

---

## Phase 3 — Update design docs

After all green commits, update in one doc commit:

design/resilience-review.md
• L1 status → FIXED (OrderBook now uses configurable capacity)
• Add snapshot version 3 migration note
• Add satisfied invariants for R1–R7

design/components-core.md  +  design/components-codec.md
• Update OrderBook, ChildOrderRegistry, SnapshotManager, AlgoSorAgent,
SmartOrderRouter, ValidationEngine contracts with new signatures and
env var defaults

design/decisions.md
• New entry: capacity constants are env-var-configurable; changing them
post-deployment requires OMS_ARCHIVE_DELETE_ON_START=true once

design/glossary.md
• MAX_ORDERS: "65_536 default; overridden by OMS_MAX_ORDERS"
• FRAGMENT_LIMIT: "10 default; overridden by OMS_ALGO_FRAGMENT_LIMIT"
• Add: OMS_MAX_CHILDREN, OMS_INTENT_FRAGMENT_LIMIT, OMS_MAX_VENUES

Run the size/lint check from design/INDEX.md before committing:
for f in design/*.md; do n=$(wc -l < "$f"); \
[ "$n" -gt 250 ] && echo "$f is $n lines — OVER LIMIT"; done

Commit: "docs(design): update component contracts and decisions for configurable capacities"

---

## Phase 4 — Final verification and merge

1. ./gradlew test              — must be green, zero failures
2. git log --oneline -12       — review all commits from this session
3. git checkout master
4. git merge --no-ff <branch> -m \
   "feat: make all capacity limits configurable via env vars; snapshot v3"
5. git log --oneline -3        — confirm merge commit is on master