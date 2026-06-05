---
name: scaling-audit-ARCHIVED
description: >
  ARCHIVED — do not load. Superseded by skills/scaling-audit/SKILL.md.
  This file is kept for historical reference only. It contains
  pre-seeded findings that are already resolved in the codebase.
  Loading it will produce stale, already-closed findings.
---

> **ARCHIVED — DO NOT LOAD.**
> Superseded by `../SKILL.md` (self-scanning version, 2026-06).
> Pre-seeded findings F1–F10 are already resolved. Loading this
> file will produce stale work.

# Scaling Audit Skill

## Purpose
End-to-end agent workflow: audit → remediate → test → document → merge.

---

## Mandatory first step

Before touching any code, read these design files:

```
design/principles.md
design/decisions.md
design/resilience-review.md
design/components-core.md
design/components-codec.md
design/failover.md
design/checklist.md
```

---

## Current baseline — what is already configurable

Decisions 12 and 13 (oms-config module, snapshot v3) resolved all original
capacity walls. Do NOT re-implement any of these. Phase 1 should verify they
are still in place and move on.

| Env Var                  | Default | Owned by                  | Verified by              |
|--------------------------|---------|---------------------------|--------------------------|
| `OMS_MAX_ORDERS`         | 65_536  | `OrderBook`               | `OrderBook(int)`constructor present |
| `OMS_MAX_CHILDREN`       | 32_768  | `ChildOrderRegistry`      | `ChildOrderRegistry(int)` constructor present |
| `OMS_INTENT_FRAGMENT_LIMIT` | 20   | `OmsClusteredService`     | instance field, not static |
| `OMS_ALGO_FRAGMENT_LIMIT`| 10      | `AlgoSorAgent`            | instance field, not static |
| `OMS_MAX_VENUES`         | 10      | `SmartOrderRouter`        | `SmartOrderRouter(int)` constructor present |
| `OMS_MAX_NOTIONAL`       | 10_000_000 | `ValidationEngine`     | passed via `OmsConfig` |
| `OMS_SYMBOLS`            | AAPL,MSFT,GOOG,AMZN | `ValidationEngine` | `LongHashSet permittedSymbols` |
| Snapshot version         | 3       | `SnapshotManager`         | header bytes [16–23] embed maxOrders + maxChildren |

**Quick baseline check — run before any audit work:**

```bash
# Confirm no static capacity walls remain from the original F1-F10 findings
grep -rn "static final int MAX_ORDERS\b"        oms-codec/src/
grep -rn "static final int DEFAULT_CAPACITY\b"  oms-core/src/
grep -rn "MAX_SNAPSHOT_BYTES"                   oms-core/src/
grep -rn "static.*FRAGMENT_LIMIT"               algo-sor/src/
grep -rn "static.*INTENT_FRAGMENT_LIMIT"        oms-core/src/
grep -rn "static final int MAX_VENUES\b"        algo-sor/src/
grep -n  "SNAPSHOT_VERSION"                     oms-core/src/main/java/com/cobain/oms/cluster/SnapshotManager.java

# Expected: MAX_ORDERS and DEFAULT_CAPACITY exist only as named defaults
# (not as the live sizing variable); FRAGMENT_LIMIT is instance-only;
# SNAPSHOT_VERSION = 3; MAX_SNAPSHOT_BYTES does not exist.
```

If any check fails, treat the regressed item as a new finding and remediate
it before continuing.

---

## Phase 1 — Audit

Scan the entire codebase for hard-coded capacity limits, fixed-size
pre-allocations, and tuning constants that create scaling walls **beyond
what is already configurable above**.

For every finding produce a structured entry:

```
FINDING <id>
Location    : <class>#<field or constant> (<module>)
Value       : <current value>
Wall        : <what breaks or fills up first, and at what load>
Snapshot    : yes/no — does this affect the snapshot wire format?
Remediation : <proposed fix in one sentence>
```

### Open seed list — start here, do not stop here

```
F1  ChildOrderIntentFlyweight.sliceIndex — byte field, range 0–255
    Formula: childClOrdId = parentOrderId * 10_000L + (sliceIndex & 0xFF)
    Wall: a parent order cannot have more than 256 child slices; the 257th
    slice silently collides with slice 1 in clOrdIdToOrderId.
    Location: ChildOrderRegistry#createChild() + ChildOrderIntentFlyweight#sliceIndex

F2  OrderState.NUM_STATES = 16 — fixed TRANSITION_TABLE first dimension
    Wall: only 6 reserved slots remain (states 10–15 are ROUTING + 5 unused).
    Adding a new state ≥ 16 causes ArrayIndexOutOfBoundsException.
    Location: OrderState#NUM_STATES (oms-codec)

F3  Single-shard topology — all symbols on one Aeron Cluster (3-node Raft)
    Wall: throughput ceiling is one Raft commit thread; no horizontal scale
    path is implemented despite being documented in decisions.md §10.2–10.3.
    Location: AeronTransport#IPC_CHANNEL + OmsLauncher (oms-launcher)

F4  OrderBook backing array is on-heap (byte[])
    Wall: not a throughput wall but an 8 MB+ on-heap array creates GC old-gen
    pressure inconsistent with the off-heap contract of ChildOrderRegistry.
    Location: OrderBook constructor — new byte[maxOrders * MESSAGE_SIZE]
    Note: LOW severity per resilience-review.md L1; deferred — confirm still deferred.
```

**Also scan for any new literals not in the seed list**, particularly in:
- Any new modules or classes added since Decision 13
- Any buffer sizes expressed as integer literals rather than named constants
- Any new `Long2LongHashMap` or `LongHashSet` with a hardcoded initial capacity

Present the full findings table before proceeding to Phase 2.

---

## Phase 2 — Implement

Apply remediations in dependency order. After each logical unit:

- (a) run `./gradlew test`
- (b) fix any failures before proceeding
- (c) make a focused git commit with the message shown below

### R1 — Child slice index widened from byte to short  (F1)

**Context:** `ChildOrderIntentFlyweight.sliceIndex` is currently a `byte` (1 byte at
offset 37, range 0–255). The child clOrdId formula `parentOrderId * 10_000L +
(sliceIndex & 0xFF)` silently wraps at slice 256, colliding with slice 0 in
`clOrdIdToOrderId`. A high-venue-count parent routed via TWAP or ICEBERG across
many cycles can exceed 255 slices.

**Wire format change:** `sliceIndex` grows from 1 byte to 2 bytes (short). The 2
bytes of alignment padding at offset 38–39 are consumed; total `BLOCK_LENGTH`
stays 56 bytes (no wire break for other fields).

**Steps:**
- Change `OFFSET_SLICE_INDEX` type in `ChildOrderIntentFlyweight` from `byte` to `short`
- Change `getSliceIndex()` / `setSliceIndex()` to use `buffer.getShort` / `putShort`
- Update `AlgoSorAgent` and `SmartOrderRouter` to pass `short sliceIndex`
- Update child clOrdId formula: `parentOrderId * 100_000L + (sliceIndex & 0xFFFF)`
  (multiplier grows from 10_000 to 100_000 to accommodate 65,535 slices; this is still
  FIX-safe for any `parentOrderId` ≤ `Long.MAX_VALUE / 100_000`)
- Add `OMS_MAX_SLICES` env var (default 65_535) to `OmsConfig`; `ChildOrderIntentValidator`
  rejects intents with `sliceIndex >= maxSlices`
- Update `design/decisions.md` Decision 9 (child clOrdId formula)
- **Snapshot impact:** child clOrdId values stored in snapshot are plain longs; the formula
  change means old snapshots decode clOrdIds differently. Bump `SNAPSHOT_VERSION` to 4 and
  add migration note to `design/resilience-review.md`.

```
Commit: "feat(child-intent): widen sliceIndex to short; update clOrdId formula; snapshot v4"
```

### R2 — Document NUM_STATES headroom; add guard  (F2)

**Context:** `NUM_STATES = 16` leaves 5 unused slots above ROUTING (state 10).
No remediation requires changing the value today, but the absence of an
explicit guard means a new state byte constant can be added without a
compile-time check — `ArrayIndexOutOfBoundsException` surfaces only at runtime.

**Steps:**
- Add a static assert in `OrderStateMachine` (or `ParentOrderState`) that fires if any
  registered state byte ≥ `NUM_STATES`:
  ```java
  static {
      assert Arrays.stream(registeredStates).allMatch(s -> s < NUM_STATES)
          : "State byte exceeds NUM_STATES=" + NUM_STATES;
  }
  ```
- Add a note to `design/components-codec.md` under `OrderState` documenting the 5
  remaining reserved slots and the assertion.
- No env-var needed; this is a structural guard, not a capacity wall.

```
Commit: "refactor(state-model): add static assert guard for NUM_STATES headroom"
```

### R3 — Document sharding prerequisites; add OMS_SHARD_ID placeholder  (F3)

**Context:** `decisions.md` §10.2–10.3 documents the horizontal sharding path but it
is not implemented. The current single-shard topology is the throughput ceiling.
This remediation does not implement sharding but makes the path explicit and
testable so it is not accidentally blocked by future changes.

**Steps:**
- Add `OMS_SHARD_ID` (default 0) to `OmsConfig` and `oms-node.properties.example`
- Add `OMS_SHARD_COUNT` (default 1) to `OmsConfig`
- Pass both to `AeronTransport`; assert `OMS_SHARD_ID < OMS_SHARD_COUNT` at startup
- When `OMS_SHARD_COUNT == 1` behaviour is identical to today (backward compatible)
- Update `design/decisions.md` §10.2–10.3 with the env-var names and wiring points
- Add to `design/glossary.md`: `OMS_SHARD_ID`, `OMS_SHARD_COUNT`
- **No snapshot impact** (shard identity is not part of snapshot state)

```
Commit: "feat(sharding): add OMS_SHARD_ID + OMS_SHARD_COUNT stubs; document wiring points"
```

### R4 — OrderBook backing array off-heap  (F4)

**Context:** `resilience-review.md` L1 flags `new byte[maxOrders * MESSAGE_SIZE]` as
deferred. Confirm the deferral is still appropriate before implementing. If the team
has decided to proceed:

**Steps:**
- Replace `new byte[maxOrders * MESSAGE_SIZE]` with
  `ByteBuffer.allocateDirect(maxOrders * MESSAGE_SIZE)` wrapped in `UnsafeBuffer`
- Pattern is identical to `ChildOrderRegistry.store` — use that as the reference
- No change to public API; no snapshot impact (snapshot reads via `UnsafeBuffer`,
  not the backing array type)
- Update `design/resilience-review.md` L1 status → FIXED
- Update `design/components-codec.md` `OrderBook` zero-allocation contract

```
Commit: "feat(order-book): move backing array off-heap via ByteBuffer.allocateDirect"
```

If the deferral stands, document the current decision date and reasoning in
`design/resilience-review.md` L1 and skip this commit.

### R5 — Sweep for any new literals  (catch-all)

Scan for any new capacity literals introduced since Decision 13:

```bash
# New Long2LongHashMap or LongHashSet with bare integer capacity
grep -rn "new Long2LongHashMap([0-9]" --include="*.java" .
grep -rn "new LongHashSet([0-9]"      --include="*.java" .

# Any new bare buffer sizes
grep -rn "new byte\[[0-9]"            --include="*.java" .
grep -rn "allocateDirect([0-9]"       --include="*.java" .
```

Replace any found literals with named constants or constructor parameters.
Document any intentional fixed sizes with a comment explaining why they are
not scaling walls (e.g. fixed-protocol message sizes).

```
Commit: "refactor(constants): replace any new literal sizes found in sweep"
```

---

## Hard constraints — never violate

These apply to every remediation, no exceptions:

- No `new` expressions in `onSessionMessage()`, `validateNewOrder()`, `validate()`,
  `createChild()`, `onFragment()`, `publishIntent()`
- No `synchronized`, `AtomicReference`, or locks on any hot-path class
- All pre-allocated buffers allocated once in constructor; reused every message
- Existing deployments with no env vars must behave identically to today
- Snapshot version must be bumped whenever any serialised layout changes
- `./gradlew test` must be green after every individual commit

---

## Phase 3 — Update design docs

After all green commits, update in one doc commit:

**`design/resilience-review.md`**
- Add satisfied invariants for each remediation applied
- Update L1 status if R4 was implemented

**`design/components-core.md`**
- Update `ChildOrderRegistry` contract: new sliceIndex range, new clOrdId formula
- Update `ChildOrderIntentValidator` contract: new `OMS_MAX_SLICES` check

**`design/components-codec.md`**
- Update `ChildOrderIntentFlyweight` layout: sliceIndex is now `short` at offset 37–38
- Update `OrderState` / `OrderStateMachine` contracts with static assert note

**`design/decisions.md`**
- Update Decision 9: child clOrdId formula (`× 100_000`, sliceIndex range 0–65_535)
- Update §10.2–10.3: `OMS_SHARD_ID` / `OMS_SHARD_COUNT` wiring points

**`design/failover.md`**
- Update snapshot format block: version 4 header (if R1 was implemented)

**`design/glossary.md`**
- Add: `OMS_MAX_SLICES`, `OMS_SHARD_ID`, `OMS_SHARD_COUNT`
- Update: child clOrdId formula entry

Run the size/lint check before committing:

```bash
for f in design/*.md; do
  n=$(wc -l < "$f")
  [ "$n" -gt 250 ] && echo "$f is $n lines — OVER LIMIT"
done
grep -rn "TODO\|FIXME\|TBD" design/
```

```
Commit: "docs(design): update contracts and decisions for slice-index widening and sharding stubs"
```

---

## Phase 4 — Final verification and merge

```bash
./gradlew test                     # must be green, zero failures
git log --oneline -12              # review all commits from this session
git checkout master
git merge --no-ff <branch> -m \
  "feat: widen sliceIndex to short; snapshot v4; sharding stubs; off-heap OrderBook"
git log --oneline -3               # confirm merge commit is on master
```

---

## Appendix — Closed findings (do not re-implement)

The following were resolved by Decisions 12 and 13. They are kept here as
historical context and to explain why the codebase looks the way it does.
Phase 1 confirms they are still closed; it does not reopen them.

| ID  | Was                                          | Resolution                                      |
|-----|----------------------------------------------|-------------------------------------------------|
| F1  | `OrderBook.MAX_ORDERS = 65_536` (static)     | Configurable via `OMS_MAX_ORDERS`; instance field |
| F2  | `ChildOrderRegistry.DEFAULT_CAPACITY = 32_768` | Configurable via `OMS_MAX_CHILDREN`; instance field |
| F3  | `SnapshotManager.MAX_SNAPSHOT_BYTES` (static) | Runtime-computed from `maxOrders + maxChildren` |
| F4  | `AlgoSorAgent.FRAGMENT_LIMIT = 10` (static)  | Configurable via `OMS_ALGO_FRAGMENT_LIMIT`; instance field |
| F5  | `OmsClusteredService.INTENT_FRAGMENT_LIMIT = 20` (static) | Configurable via `OMS_INTENT_FRAGMENT_LIMIT`; instance field |
| F6  | `SmartOrderRouter.MAX_VENUES = 10` (static)  | Configurable via `OMS_MAX_VENUES`; instance field |
| F7  | `ValidationEngine.seenClOrdIds` fixed capacity | Pre-sized to `maxOrders × 2` at load factor 0.65 |
| F8  | `TRANSITION_TABLE` literal dimensions `[16][10]` | Confirmed using `NUM_STATES` / `NUM_EVENTS` — no change needed |
| F9  | Hard-coded stream IDs in `AeronTransport`    | Named constants (`STREAM_OMS_TO_ALGO = 30`, etc.) — no change needed |
| F10 | Hard-coded buffer sizes `129` in `OmsClusteredService` | Uses `ClusterMessageType.IPC_MESSAGE_SIZE` — no change needed |

Corresponding remediations R1–R8 that implemented the above are archived with
their git commits and are searchable in `git log`.