---
name: scaling-audit-skill
description: >
  Run a full scaling and resilience audit on the MarketOMS codebase.
  Discovers every hard-coded capacity limit, fixed-size pre-allocation,
  tuning constant, and resilience gap by scanning the live source — no
  pre-seeded findings. Proposes and implements remediations that respect
  the zero-GC hot path and Raft replication invariants; bumps the snapshot
  version if any serialised layout changes; updates all design docs; runs
  tests; and merges to master.
  Trigger for any question about: scaling, capacity ceilings, hard-coded
  sizes, throughput walls, data loss on failover, snapshot gaps, SPOF,
  recovery correctness, or making any limit configurable.
---

# Scaling and Resilience Audit Skill

## Purpose

End-to-end agent workflow that discovers and remediates its own findings:

```
Read design context → Scan codebase → Classify findings → Present to user
→ [USER APPROVES] → Implement fixes → Test → Update docs
→ Present diff → [USER APPROVES] → Merge
```

No findings are pre-seeded. The agent reads the codebase on every invocation
and derives findings from what it actually finds.

**Two mandatory confirmation gates:**
- Gate 1 — after Phase 1: user reviews and approves the findings table
  before any code is changed.
- Gate 2 — after Phase 3: user reviews the full diff before anything
  is merged to master.

---

## Step 0 — Read design context first (mandatory)

Before scanning a single source file, read every design document listed here.
This gives the agent the invariants it must preserve and the vocabulary to
classify findings correctly.

```
design/INDEX.md          — file ownership table; read first to orient
design/principles.md     — zero-GC rules, module boundaries, hot-path methods
design/decisions.md      — prior architectural decisions; know what is intentional
design/resilience-review.md — all known resilience findings and their status
design/components-core.md   — component contracts: constructors, invariants, failure modes
design/components-codec.md  — wire-format and constant class contracts
design/failover.md          — snapshot wire format, restore step sequence
design/checklist.md         — gate questions; use as a cross-check for each finding
CLAUDE.md                   — resilience principles P1–P6; engineering rules
```

**Key invariants to hold in mind while scanning (from the above):**

- Zero-GC hot path: no `new` in `onSessionMessage()`, `validateNewOrder()`,
  `validate()`, `createChild()`, `onFragment()`, `publishIntent()`
- All pre-allocated buffers allocated once in constructor and reused
- No `synchronized`, `AtomicReference`, or locks on any hot-path class
- Every stateful component must be serialized in `SnapshotManager.takeSnapshot()`
  and restored in `loadSnapshot()` (CLAUDE.md P1)
- Snapshot version must be bumped whenever the serialised layout changes
- `algo-sor` must never depend on `oms-core` at compile time
- `OmsConfig` is the single source of truth for all capacity constants;
  components receive values as constructor parameters — they never read env vars directly
- Existing deployments with no env vars must behave identically after any change

---

## Phase 1 — Discover findings

### 1a — Scan for scaling walls

A **scaling wall** is any point in the code where a fixed number creates a
ceiling on load that cannot be raised without a code change or redeployment.

Read every `.java` file under `oms-codec/src`, `oms-core/src`, `algo-sor/src`,
`oms-launcher/src`, and `oms-config/src`. For each file look for:

**S-type: Static capacity constants**
Any `static final int/long` that sizes a data structure, buffer, or loop bound.
Ask: can this value be raised without a code change? If no → scaling wall.
Specific patterns to look for:
```
static final int   MAX_*     =
static final int   DEFAULT_* =
static final int   LIMIT_*   =
static final int   NUM_*     =   (only those used to size arrays, not enumerations)
new byte[<literal>]
new int[<literal>]
ByteBuffer.allocateDirect(<literal>)
new Long2LongHashMap(<literal>
new LongHashSet(<literal>
```

**W-type: Wire format hard limits**
Field widths in flyweight classes that constrain value ranges.
Ask: what is the maximum value this field can hold, and what breaks when
that maximum is reached?
Specific patterns to look for — in any flyweight `getXxx()`/`setXxx()` pair:
```
buffer.getByte  / putByte   → range 0–127 (signed) or 0–255 (masked)
buffer.getShort / putShort  → range 0–32767 (signed) or 0–65535 (masked)
buffer.getInt   / putInt    → check if used as a counter or index, not an ID
```
For each, trace whether the value feeds a formula (e.g. clOrdId derivation),
an index into an array, or a loop bound, and what overflows or collides first.

**T-type: Topology ceilings**
Single-process or single-shard constraints that cannot be scaled horizontally.
Look in `OmsLauncher`, `OmsNode`, `AeronTransport` for:
- IPC channels with no shard parameterisation
- Stream IDs that are global rather than shard-local
- Single `Aeron.Context` with a single `aeronDirectoryName`

**C-type: Configurable constants that have lost their env-var wiring**
Check that every field in `OmsConfig` is actually passed through to the
component that uses it. A constant that was made configurable but whose
wiring was dropped in a later refactor is a hidden wall.
```bash
# For each field in OmsConfig, verify it is passed as a constructor arg:
grep -n "public final" oms-config/src/main/java/com/cobain/oms/config/OmsConfig.java
# Then for each field name, verify it reaches its target constructor:
grep -rn "<fieldName>" oms-launcher/src oms-core/src
```

---

### 1b — Scan for resilience gaps

A **resilience gap** is any state that can be lost on a leader failover, any
single point of failure that halts processing, or any recovery path that is
missing, incomplete, or incorrect.

**G-type: Snapshot completeness gaps**
Find every class that holds durable order state — state that must survive a
leader failover. Cross-reference against what `SnapshotManager.takeSnapshot()`
actually serializes.
```bash
# What does takeSnapshot() actually serialize?
grep -n "snapshot\|putBytes\|putLong\|putInt" \
  oms-core/src/main/java/com/cobain/oms/cluster/SnapshotManager.java

# What classes hold order or dedup state (field type Long2LongHashMap, LongHashSet,
# UnsafeBuffer used as an order store)?
grep -rn "Long2LongHashMap\|LongHashSet\|UnsafeBuffer" \
  oms-core/src/main/java/ oms-codec/src/main/java/
```
For each stateful class found: is its state fully captured in `takeSnapshot()`
and fully restored in `loadSnapshot()`?

**P-type: Post-restore correctness gaps**
Read `OmsClusteredService.onStart()` end-to-end. Verify:
- `ParentOrderState.registerTransitions()` is called before snapshot load
- `recomputeNextOrderId()` scans all registries that hold order IDs
- `republishRoutingOrders()` is called to re-hydrate algo-sor after restore
- All subscriptions that are SPOFs have `availableImageHandler` /
  `unavailableImageHandler` attached (CLAUDE.md P6)

**D-type: Archive durability gaps**
Check the default value of `OMS_ARCHIVE_DIR` in `OmsNode`. Is it ephemeral
(`/tmp`, `/dev/shm`) or durable?

**V-type: Version and migration gaps**
Read `SnapshotManager.SNAPSHOT_VERSION`. Read `handleSnapshotFragment()` and
verify it throws a clear `IllegalStateException` for mismatched versions rather
than silently corrupting state.
If any remediation in this session changes the snapshot wire format, the version
must be bumped and a migration note added to `design/resilience-review.md`.

---

### 1c — Produce the findings table

For every finding, produce one structured entry.

```
FINDING <id>
Type        : S | W | T | C | G | P | D | V
Severity    : CRITICAL | HIGH | MEDIUM | LOW
Location    : <class>#<field, method, or constant> (<module>)
Current     : <exact current value or behaviour>
Wall/Gap    : <what breaks, fills, or fails and at what load or event>
Snapshot    : YES | NO — does this affect the snapshot wire format?
Intentional : YES | NO | UNKNOWN
             (YES = documented in decisions.md or resilience-review.md as deferred/accepted)
Remediation : <one-sentence proposed fix>
```

**Severity classification:**

| Severity | Meaning |
|----------|---------|
| CRITICAL | Silent data loss on failover, or unrecoverable crash under normal load |
| HIGH     | Scaling wall reachable under expected peak load; or SPOF with no detection |
| MEDIUM   | Wall reachable only at 10× expected load; or gap with partial mitigation |
| LOW      | Inconsistency, tech debt, or documented deferral that should be reviewed |

**Before classifying a finding as open**, check:
1. Is it listed in `design/resilience-review.md` with status FIXED or SATISFIED? → skip
2. Is it documented in `design/decisions.md` as an intentional trade-off? → mark `Intentional: YES`
3. Is it controlled by an `OmsConfig` field that is correctly wired? → skip

**Only open findings** (not FIXED, not intentionally accepted) proceed to Phase 2.

---

### !! GATE 1 — Mandatory stop before any code changes !!

After completing the findings table, STOP IMMEDIATELY.

Present the complete findings table to the user, then output exactly this
message:

```
─────────────────────────────────────────────────
GATE 1 — Findings review

X open finding(s) discovered (CRITICAL: N, HIGH: N, MEDIUM: N, LOW: N).

Review the table above. Then reply with one of:

  "proceed with all"
      → implement every open finding

  "proceed with <IDs>"
      → implement only the listed finding IDs, e.g. "proceed with S-01, G-02"

  "skip <IDs>, proceed with the rest"
      → skip the listed IDs, implement everything else

  "cancel"
      → stop the audit; make no changes

Intentional/deferred findings (marked Intentional: YES) are listed for
visibility only and will not be implemented unless you explicitly include
them in your reply.
─────────────────────────────────────────────────
```

Do not write a single line of code, create any file, or run any command
other than read-only checks until the user has replied to Gate 1.
Do not interpret any message other than the approved forms above as
permission to proceed.

---

## Phase 2 — Implement remediations

Process the approved findings only, in dependency order. A finding that
affects the snapshot wire format must be grouped with its `SNAPSHOT_VERSION`
bump into a single atomic commit.

**For each finding (or logically related group of findings):**

1. Read the component contract in `design/components-core.md` or
   `design/components-codec.md` before changing anything.
2. Apply the remediation.
3. Run `./gradlew test` — must be green before committing.
4. Commit with the conventional format:

```
<type>(<scope>): <description>

Body: technical context only — what changed and why.
```

**Remediation patterns by finding type:**

**S-type (static capacity constant):**
- Add a parameterised constructor; no-arg delegates to the existing default constant
- Add env var `OMS_<NAME>` to `OmsConfig` (`KEY_*` constant, `DEFAULT_*` constant, field)
- Add to `oms-node.properties.example` with a comment
- Wire through entry points (`OmsNode.loadOmsConfig()`, `OmsLauncher.loadOmsConfig()`)
  passing to the component as a constructor argument — the component never reads the
  env var directly
- Pre-allocate all arrays in the constructor at the new runtime size
- Zero-GC: no allocation on the hot path as a result of the change

**W-type (wire format field width):**
- Widen the field in the flyweight (e.g. `byte` → `short`), consuming adjacent
  padding bytes if available to avoid changing `BLOCK_LENGTH`
- Update the formula or index that consumes the field value
- If `BLOCK_LENGTH` changes or any snapshot-stored value changes:
  bump `SNAPSHOT_VERSION` and update the snapshot format table in
  `design/failover.md` and `design/resilience-review.md`
- Update `design/decisions.md` if the change affects a documented formula

**T-type (topology ceiling):**
- Add `OMS_SHARD_ID` (default 0) and `OMS_SHARD_COUNT` (default 1) to `OmsConfig`
- Pass to `AeronTransport`; assert `shard_id < shard_count` at startup
- When `shard_count == 1` behaviour must be identical to before
- Do not implement full sharding in a single audit pass; wire stubs and document
  the remaining work in `design/decisions.md`

**C-type (broken env-var wiring):**
- Trace the field from `OmsConfig` through entry-point → constructor injection
- Re-connect the broken link; add a test asserting the non-default value is
  actually used by the component

**G-type (snapshot gap):**
- Add `snapshot()` and `restore()` methods to the affected class if absent
- Add the call to `SnapshotManager.takeSnapshot()` and `loadSnapshot()`
- Bump `SNAPSHOT_VERSION`
- Add a satisfied invariant to `design/resilience-review.md`

**P-type (post-restore gap):**
- Add the missing step to `OmsClusteredService.onStart()`
- Add or extend the missing image handler
- Update `design/components-core.md` `OmsClusteredService` contract
- Add a satisfied invariant to `design/resilience-review.md`

**D-type (archive durability):**
- Change the default to a durable path; document the production override
  requirement in `design/resilience-review.md`

**V-type (version/migration gap):**
- Ensure `handleSnapshotFragment()` throws `IllegalStateException` with a
  clear message including the wipe instruction on version mismatch
- Add a migration note to `design/resilience-review.md`

---

### Hard constraints — never violate regardless of finding type

```
• No `new` in onSessionMessage(), validateNewOrder(), validate(),
  createChild(), onFragment(), publishIntent()
• No synchronized, AtomicReference, or locks on any hot-path class
• All pre-allocated buffers allocated once in constructor; reused every message
• Existing deployments with no env vars must behave identically to today
• Snapshot version must be bumped if any serialised layout changes
• algo-sor must never gain a compile-time dependency on oms-core
• Components never read OmsConfig or env vars directly — values injected
  as constructor args by OmsNode / OmsLauncher
• ./gradlew test must be green after every individual commit
```

---

## Phase 3 — Update design docs

After all green commits, make one final documentation commit.

**For every finding that was fixed, update the file that owns the relevant fact:**

| If the fix touched...                          | Update this file                               |
|------------------------------------------------|------------------------------------------------|
| A component's constructor or invariants        | `design/components-core.md` or `design/components-codec.md` |
| The snapshot wire format                       | `design/failover.md` snapshot format block     |
| A capacity constant or env var                 | `design/glossary.md`                           |
| An architectural decision or trade-off         | `design/decisions.md`                          |
| A resilience finding (new fix or new deferral) | `design/resilience-review.md`                  |
| A wire-format field offset or width            | `design/wire-formats.md`                       |

**For every finding that was intentionally deferred:**
Add a dated entry to `design/resilience-review.md` with:
- Finding ID and description
- Reason for deferral
- Conditions that would trigger re-evaluation
- Status: DOCUMENTED (deferred)

**Run the size/lint check before committing:**

```bash
for f in design/*.md; do
  n=$(wc -l < "$f")
  [ "$n" -gt 250 ] && echo "OVER LIMIT: $f ($n lines)"
done
grep -rn "TODO\|FIXME\|TBD" design/   # expected: no output
```

```
Commit: "docs(design): update contracts, resilience review, and decisions for <audit-branch-name>"
```

---

## Phase 4 — Final verification and merge

### 4a — Run tests and prepare the diff

```bash
# All tests must be green
./gradlew test

# Review all commits produced in this session
git log --oneline -20

# Produce the full diff against master for user review
git diff master..HEAD
```

### !! GATE 2 — Mandatory stop before merge !!

After running the commands above, STOP.

Present to the user:
1. The test result (pass / fail with failure count)
2. The list of commits from `git log --oneline -20`
3. The full output of `git diff master..HEAD`

Then output exactly this message:

```
─────────────────────────────────────────────────
GATE 2 — Pre-merge review

All tests: PASSED / FAILED (see above)
Commits on branch: N
Files changed: N

Review the diff and commit list above. Then reply with one of:

  "merge"
      → run the merge commit to master

  "cancel"
      → stop here; branch remains open, no merge performed

Do not run the merge command until you have replied "merge".
─────────────────────────────────────────────────
```

Do not run `git merge`, `git checkout master`, or any other destructive
command until the user has replied "merge".

### 4b — Merge (only after user replies "merge")

```bash
git checkout master
git merge --no-ff <branch> -m \
  "feat(audit): <one-line summary of all findings fixed>"

# Confirm merge commit is on master
git log --oneline -3
```

---

## Appendix A — Scan cheat-sheet

Quick grep commands to run at the start of Phase 1 to surface candidates fast.
These are starting points — the agent reads the matched files in full to
classify correctly.

```bash
# Static capacity integers (S-type candidates)
grep -rn "static final int\|static final long" \
  --include="*.java" \
  oms-codec/src oms-core/src algo-sor/src oms-launcher/src oms-config/src \
  | grep -v "//.*static final"   # exclude commented-out lines

# Bare new-array allocations (possible hot-path violations or fixed buffers)
grep -rn "new byte\[\|new int\[\|new long\[" --include="*.java" .

# Bare ByteBuffer.allocateDirect with a literal size
grep -rn "allocateDirect([0-9]" --include="*.java" .

# Agrona collections with literal initial capacity
grep -rn "new Long2LongHashMap([0-9]\|new LongHashSet([0-9]" --include="*.java" .

# Byte or short flyweight fields (W-type candidates — check for masking)
grep -rn "getByte\|putByte\|getShort\|putShort" --include="*.java" .

# Snapshot version
grep -rn "SNAPSHOT_VERSION" --include="*.java" .

# takeSnapshot method — check what is serialized
grep -n "snapshot\|putBytes\|putInt\|putLong" \
  oms-core/src/main/java/com/cobain/oms/cluster/SnapshotManager.java

# OmsClusteredService.onStart — check post-restore steps
grep -n "onStart\|republish\|recompute\|registerTransitions" \
  oms-core/src/main/java/com/cobain/oms/cluster/OmsClusteredService.java

# Archive directory default value
grep -n "OMS_ARCHIVE_DIR\|/tmp\|user.home" \
  oms-launcher/src/main/java/com/cobain/oms/launcher/OmsNode.java

# Image handlers on Aeron subscriptions
grep -rn "addSubscription\|availableImageHandler\|unavailableImageHandler" \
  --include="*.java" oms-core/src oms-launcher/src

# Verify algo-sor does not depend on oms-core
./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core
# Expected: no output
```

---

## Appendix B — What good remediation output looks like

A well-structured findings table entry:

```
FINDING S-01
Type        : S
Severity    : HIGH
Location    : WidgetRegistry#MAX_WIDGETS (oms-core)
Current     : static final int MAX_WIDGETS = 1_000
Wall/Gap    : Registry fills at 1,000 simultaneous widgets; allocateSlot() returns
              -1 silently; ops has no alert. Expected peak load is 2,000 widgets
              by Q3 2026.
Snapshot    : YES — MAX_SNAPSHOT_BYTES derived from MAX_WIDGETS
Intentional : NO
Remediation : Add OmsConfig.maxWidgets (env OMS_MAX_WIDGETS, default 1_000);
              pass to WidgetRegistry constructor; bump SNAPSHOT_VERSION.
```

A well-structured commit message:

```
feat(widget-registry): make MAX_WIDGETS configurable via OMS_MAX_WIDGETS

MAX_WIDGETS was a static compile-time constant. Under Q3 peak load projections
(2,000 widgets) the registry fills and silently drops new allocations.

- Add WidgetRegistry(int maxWidgets) constructor; no-arg delegates to MAX_WIDGETS
- Add OmsConfig.KEY_MAX_WIDGETS / DEFAULT_MAX_WIDGETS (1_000)
- Wire through OmsNode.loadOmsConfig() → OmsClusteredService constructor
- SnapshotManager accepts maxWidgets; buffer sized at runtime
- Bump SNAPSHOT_VERSION to N; add migration note to resilience-review.md
```