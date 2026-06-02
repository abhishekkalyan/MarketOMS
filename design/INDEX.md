# MarketOMS Design Index

Read this file at the start of every task. Then load only the files
listed for your task type. Do not load files not listed.

## Task → Files to Load

| Task type | Load these files |
|-----------|-----------------|
| Any task (always load these) | `principles.md`, `checklist.md` |
| State machine or order state change | + `state-model.md` |
| New or modified component | + `components.md` |
| Wire format, buffer, or flyweight change | + `wire-formats.md` |
| Aeron IPC channel or stream change | + `components.md`, `data-flows.md` |
| Algo, SOR, or intent protocol change | + `components.md`, `data-flows.md` |
| Snapshot, failover, or recovery change | + `failover.md`, `sequences.md` |
| New design decision | + `decisions.md` |
| Unfamiliar term, class name, or constant | + `glossary.md` |
| Full architecture review | All files |

## File Ownership

Each fact lives in exactly one file. Update the file that owns it.
Never add the same fact to two files.

| File | Owns |
|------|------|
| `principles.md` | Design rules, module boundaries, zero-GC contract, boundary check commands |
| `checklist.md` | Gate questions before any change; self-check before reporting done |
| `state-model.md` | State byte constants, transition table, ROUTING registration, state invariants |
| `wire-formats.md` | OrderLayout field offsets, ChildOrderIntentFlyweight layout, price encoding, symbol encoding, FIX Binary format |
| `components.md` | Component contracts: class name, module, thread model, zero-allocation contract, inputs, outputs, invariants, failure mode |
| `data-flows.md` | Aeron stream IDs, message flow diagrams, method call sequences |
| `sequences.md` | Idempotency mechanisms, seenClOrdIds dedup, over-allocation guard, five sequencing boundaries |
| `failover.md` | Snapshot wire format, SnapshotManager constants, onTakeSnapshot and onLoadSnapshot step sequences |
| `decisions.md` | Design decision log: what was decided, alternatives rejected, consequence of reversing |
| `checklist.md` | Design analysis gate questions (zero-GC, golden source, state model, boundaries, sequencing, failover) |
| `glossary.md` | Domain terms, constant values, class references, package names |

## Post-Change Verification

After any task that updates a design file, run:

```bash
# No file oversized
awk 'END {if (NR > 150) print FILENAME " is " NR " lines — OVER LIMIT"}' design/*.md

# No placeholder text
grep -rn "TODO\|FIXME\|placeholder\|TBD" design/
# Expected: no output

# All files present
for f in INDEX.md principles.md checklist.md state-model.md wire-formats.md \
          components.md data-flows.md sequences.md failover.md decisions.md \
          glossary.md; do
  [ -f "design/$f" ] && echo "OK: $f" || echo "MISSING: $f"
done
```
