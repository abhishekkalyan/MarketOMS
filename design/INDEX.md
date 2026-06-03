# MarketOMS Design Index

Read this file at the start of every task. Then load only the files
listed for your task type. Do not load files not listed.

## Task → Files to Load

| Task type | Load these files |
|-----------|-----------------|
| Any task (always load these) | `principles.md`, `checklist.md` |
| State machine or order state change | + `state-model.md` |
| New or modified oms-codec component | + `components-codec.md` |
| New or modified oms-core or algo-sor component | + `components-core.md` |
| Wire format, buffer, or flyweight change | + `wire-formats.md` |
| Aeron IPC channel or stream change | + `components-core.md`, `data-flows.md` |
| Algo, SOR, or intent protocol change | + `components-core.md`, `data-flows.md` |
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
| `components-codec.md` | Component contracts for oms-codec: OrderLayout, OrderFlyweight, ChildOrderIntentFlyweight, ClusterMessageType, OrderState+OrderEvent, OrderBook, FIXMessageDecoder |
| `components-core.md` | Component contracts for oms-core and algo-sor: AeronTransport, ParentOrderState, OrderStateMachine, ValidationEngine, ChildOrderIntentValidator, ChildOrderRegistry, SnapshotManager, OmsClusteredService, FIXMessageEncoder, AlgoSorAgent, SmartOrderRouter |
| `data-flows.md` | Aeron stream IDs, message flow diagrams, method call sequences |
| `sequences.md` | Idempotency mechanisms, seenClOrdIds dedup, over-allocation guard, five sequencing boundaries |
| `failover.md` | Snapshot wire format, SnapshotManager constants, onTakeSnapshot and onLoadSnapshot step sequences |
| `decisions.md` | Design decision log: what was decided, alternatives rejected, consequence of reversing |
| `glossary.md` | Domain terms, constant values, class references, package names |
| `resilience-review.md` | Structured resilience findings: data loss gaps, SPOF analysis, satisfied invariants |
| `../skills/` | Reusable agent workflows (SKILL.md files) |

## Post-Change Verification

After any task that updates a design file, run:

```bash
# No file oversized
for f in design/*.md; do n=$(wc -l < "$f"); [ "$n" -gt 250 ] && echo "$f is $n lines — OVER LIMIT"; done

# No placeholder text
grep -rn "TODO\|FIXME\|placeholder\|TBD" design/
# Expected: no output

# All files present
for f in INDEX.md principles.md checklist.md state-model.md wire-formats.md \
          components-codec.md components-core.md data-flows.md sequences.md \
          failover.md decisions.md glossary.md resilience-review.md; do
  [ -f "design/$f" ] && echo "OK: $f" || echo "MISSING: $f"
done
```
