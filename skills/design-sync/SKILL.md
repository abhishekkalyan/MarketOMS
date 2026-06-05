# Design Sync Skill

## Purpose

Bring every file in `design/` into alignment with the live source code.
Finds every fact in a design doc that does not match what the source
actually shows, proposes the precise in-place edits needed, then applies
them after user approval.

```
Read all design docs → Scan source → Diff each owned fact against source
→ Build per-file change plan → [USER APPROVES] → Apply edits
→ Run post-change verification → Commit
```

One confirmation gate: user reviews the complete per-file change plan
before any design file is touched.

This skill does not change source code. It does not add new architectural
decisions. It corrects stale facts to match what the code already does.

---

## Step 0 — Read the ownership map first (mandatory)

Read `design/INDEX.md` in full. The File Ownership table defines which
file owns which category of fact. Every correction must land in the file
that owns the fact — never duplicated across two files.

Then read every design file listed in the ownership table:

```
design/principles.md
design/checklist.md
design/state-model.md
design/wire-formats.md
design/components-codec.md
design/components-core.md
design/data-flows.md
design/sequences.md
design/failover.md
design/decisions.md
design/glossary.md
design/resilience-review.md
CLAUDE.md
```

Hold every stated fact in memory. You will compare each one against
source in Step 1.

---

## Step 1 — Scan source

Read every `.java` file under:

```
oms-codec/src/main/java
oms-core/src/main/java
algo-sor/src/main/java
oms-launcher/src/main/java
oms-harness/src/main/java
oms-config/src/main/java
```

Also read:
```
config/oms-node.properties.example
```

While reading, extract the ground-truth value for every category of fact
that a design file owns. Use the extraction targets below as your guide —
do not stop at these; extract any fact that a design doc makes a claim about.

**Constants and defaults**
```bash
grep -rn "static final int\|static final long\|static final byte\|static final String" \
  --include="*.java" .
```

**Constructor signatures**
```bash
grep -rn "public [A-Z][a-zA-Z]*(" --include="*.java" .
```

**Method signatures on public API**
```bash
grep -rn "public [a-z].*(" --include="*.java" . | grep -v "//"
```

**Field offsets in flyweights and layout classes**
```bash
grep -rn "OFFSET_\|_OFFSET\s*=" --include="*.java" .
```

**Stream IDs**
```bash
grep -rn "STREAM_\|= 10\b\|= 11\b\|= 12\b\|= 30\b\|= 20\b\|= 21\b\|= 22\b" \
  --include="*.java" .
```

**Snapshot version and header layout**
```bash
grep -rn "SNAPSHOT_VERSION\|HEADER_SIZE\|DEDUP_ENTRY_SIZE" --include="*.java" .
```

**OmsConfig env var keys and defaults**
```bash
grep -n "KEY_\|DEFAULT_" \
  oms-config/src/main/java/com/cobain/oms/config/OmsConfig.java
```

**Thread model — find synchronized, locks, AtomicReference**
```bash
grep -rn "synchronized\|AtomicReference\|ReentrantLock" --include="*.java" .
```

**Hot-path allocation — find new in hot-path methods**
```bash
grep -rn "new " --include="*.java" . \
  | grep -E "onSessionMessage|validateNewOrder|validate\(|createChild|onFragment|publishIntent"
```

**Module boundaries**
```bash
./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core
./gradlew :oms-codec:dependencies --configuration compileClasspath | grep aeron-cluster
./gradlew :oms-harness:dependencies --configuration compileClasspath | grep -E "oms-core|algo-sor"
```

---

## Step 2 — Diff design docs against source

For every fact stated in a design file, compare it against source truth.

### What to check per design file

**`design/principles.md`**
- Module dependency table (Section 3.1): verify each "Depends On" and
  "Must Never Depend On" against actual Gradle dependencies
- Aeron IPC channel map (Section 3.2): verify every stream ID, publisher
  class, subscriber class, and payload size against source
- Module responsibility matrix: verify "Owns" and "Must Not Own" columns
  against source imports and class locations

**`design/components-codec.md` and `design/components-core.md`**
For every component entry, check each line of its contract block:
- **Class:** correct fully-qualified class name?
- **Module:** correct Gradle module?
- **Constructor:** signature matches source exactly?
- **Zero-allocation contract:** does source honour the stated contract?
  Check hot-path methods for `new` expressions.
- **Inputs/Outputs:** method names exist in source with stated signatures?
- **Invariants:** every stated constant value matches source?
- **Failure mode:** condition described is accurate given source logic?

**`design/state-model.md`**
- Every state byte constant value matches `OrderState` source
- Every event byte constant value matches `OrderEvent` source
- `NUM_STATES` and `NUM_EVENTS` match source
- Transition table entries for key transitions match
  `ParentOrderState.registerTransitions()` in source
- `isTerminal()` threshold matches source implementation

**`design/wire-formats.md`**
- Every field offset in `OrderLayout` matches source constants
- `BLOCK_LENGTH` / `MESSAGE_SIZE` values match source
- `ChildOrderIntentFlyweight` layout matches source field offsets
  and `BLOCK_LENGTH`
- `FIX_BINARY_SIZE` matches `FIXMessageDecoder` source
- Price multiplier `PRICE_MULTIPLIER` matches source

**`design/data-flows.md`**
- Every Aeron stream ID matches `AeronTransport` and `ClusterMessageType`
- Publisher and subscriber class names match source
- Message type codes (e.g. `NEW_ORDER = 1`, `CHILD_ORDER_INTENT = 40`)
  match `ClusterMessageType` source

**`design/sequences.md`**
- `ChildOrderIntentValidator` rule count and order match source
- Dedup mechanism description matches `ValidationEngine` source
- Over-allocation guard description matches
  `ChildOrderIntentValidator` rule 7 in source

**`design/failover.md`**
- `SNAPSHOT_VERSION` matches `SnapshotManager` source
- `HEADER_SIZE` matches source
- Snapshot buffer size formula matches `SnapshotManager` constructor
- Restore step sequence matches `OmsClusteredService.onStart()` in source
- `takeSnapshot` step sequence matches `SnapshotManager.takeSnapshot()` source

**`design/glossary.md`**
- Every env var name matches `OmsConfig.KEY_*` constants in source
- Every default value matches `OmsConfig.DEFAULT_*` constants in source
- Every stream ID alias matches `AeronTransport` source
- Every constant value (e.g. `FRAGMENT_LIMIT`, `INTENT_FRAGMENT_LIMIT`)
  matches current source

**`design/resilience-review.md`**
- Every finding marked FIXED: verify the stated fix is actually present
  in source
- Every finding marked SATISFIED: verify the invariant holds in source
- Snapshot format table: verify version number, header byte layout,
  and buffer size formula against source

**`design/decisions.md`**
- Every decision that references a concrete value (formula, constant,
  class name): verify the value is still accurate in source
- Decisions describing "not yet implemented" features: verify they are
  still absent from source (not accidentally implemented)

**`CLAUDE.md`**
- Resilience principles P1–P6: verify each "Currently:" clause matches
  source (e.g. P1 lists `OrderBook`, `ValidationEngine.seenClOrdIds`,
  `ChildOrderRegistry` — are these still the only snapshotted components?)
- Engineering rules: verify no `synchronized` or `AtomicReference` on
  hot path, verify zero-GC contract in hot-path methods

---

## Step 3 — Build the change plan

For every discrepancy found, produce one structured entry:

```
DIFF <id>
Design file : design/<filename>.md  (or CLAUDE.md)
Section     : <heading or line reference>
Current text: <exact text in the design doc>
Source truth: <class#field or method> shows: <exact value or behaviour>
Proposed fix: <exact replacement text — the new wording for that line or block>
Impact      : NONE | LOW | MEDIUM | HIGH
              NONE  = cosmetic (class name typo, whitespace)
              LOW   = value update (constant changed, default changed)
              MEDIUM = contract change (new parameter, changed method sig)
              HIGH  = structural change (new invariant, removed guarantee,
                      security or data-loss implication)
```

**Do not propose:**
- Rewriting sections that are correct
- Adding new content that does not exist in source
  (new decisions, new rationale — that is authoring, not syncing)
- Changing `decisions.md` entries that describe rationale or trade-offs —
  these are not factual claims about source, they are recorded reasoning
- Changing `resilience-review.md` DEFERRED or DOCUMENTED items —
  these are intentional states, not errors

**Do propose:**
- Correcting any constant value that differs from source
- Updating any constructor signature that changed
- Updating any method name that was renamed
- Correcting any stream ID that changed
- Updating snapshot version, header size, buffer formula
- Correcting any invariant that no longer holds in source
- Marking any FIXED finding in `resilience-review.md` whose fix is not
  actually present in source as OPEN again, with a note

After building the full list, group entries by target design file.

---

## !! GATE — Mandatory stop before editing any design file !!

After completing Steps 0–3, STOP.

Present the complete change plan grouped by file. For each file show:
- How many changes proposed
- The highest impact level among them
- The full list of DIFF entries for that file

Then output exactly this message:

```
─────────────────────────────────────────────────
GATE — Design sync change plan review

N design files need updates.
N total changes (HIGH: N, MEDIUM: N, LOW: N, NONE: N).

Review the change plan above. Then reply with one of:

  "proceed"
      → apply all proposed changes

  "proceed with <IDs>"
      → apply only the listed DIFF IDs, e.g. "proceed with D-01, D-04"

  "skip <IDs>, proceed with the rest"
      → skip the listed IDs, apply everything else

  "cancel"
      → stop here; no design file is touched

Do not edit any file until you have received one of the above replies.
─────────────────────────────────────────────────
```

---

## Step 4 — Apply approved changes

For each approved DIFF entry, apply the proposed fix as a precise
in-place edit to the target design file. Do not rewrite surrounding
text. Do not reformat sections not being changed.

After every individual file edit, re-read the file and verify:
- The changed line now matches the proposed fix exactly
- No surrounding text was accidentally altered
- The file still makes sense as a whole (no broken references
  to the just-corrected value elsewhere in the same file)

If the same value appears in multiple places within the same file,
update all occurrences in the same edit.

---

## Step 5 — Post-change verification

Run these checks after all edits are applied:

```bash
# No design file oversized
for f in design/*.md; do
  n=$(wc -l < "$f")
  [ "$n" -gt 250 ] && echo "OVER LIMIT: $f ($n lines)"
done

# No placeholder text left behind
grep -rn "TODO\|FIXME\|placeholder\|TBD" design/
# Expected: no output

# All required files still present
for f in INDEX.md principles.md checklist.md state-model.md wire-formats.md \
          components-codec.md components-core.md data-flows.md sequences.md \
          failover.md decisions.md glossary.md resilience-review.md; do
  [ -f "design/$f" ] && echo "OK: $f" || echo "MISSING: $f"
done

# No fact duplicated — spot-check the constants that appear in
# both glossary.md and components files
grep -rn "OMS_MAX_ORDERS\|SNAPSHOT_VERSION\|FRAGMENT_LIMIT" design/ \
  | grep -v "^design/glossary"   # glossary is allowed to reference these
# Expected: each constant appears in at most one non-glossary file
```

If any check fails, fix it before proceeding to Step 6.

---

## Step 6 — Commit

Stage and commit all changed design files with this message format:

```
docs(design): sync design docs to source HEAD <git sha>

Summary of changes:
- <design/file.md>: <one-line description of what changed>
- <design/file.md>: <one-line description of what changed>
...

N files updated, N facts corrected.
High-impact changes: <list DIFF IDs with HIGH impact, or "none">
```

Do not commit source files. Do not commit `docs/functional-spec.md`
if it exists — it must be regenerated separately after this commit.

---

## Step 7 — Post-commit output

After committing, output:

```
Design sync complete.

Commit: <git sha>
Files updated : N
Changes applied: N (HIGH: N, MEDIUM: N, LOW: N, NONE: N)
Changes skipped: N (if any were skipped at Gate)

If docs/functional-spec.md exists, it is now stale.
Regenerate it by running skills/spec-generator/SKILL.md.
```