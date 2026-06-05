# Functional Specification Generator Skill

## Purpose

Derive a complete functional specification document from the live codebase
and design docs. Writes to docs/functional-spec.md. Never invents
capabilities not evidenced in source or design docs.

```
Read design docs → Scan source → Resolve conflicts → Draft section summaries
→ [USER APPROVES] → Write docs/functional-spec.md
```

One confirmation gate: user reviews the section plan and any conflict
report before the file is written.

---

## Step 0 — Read design context first (mandatory)

Read every file listed below before scanning a single source file.

```
design/INDEX.md             — file ownership; read first to orient
design/principles.md        — system identity, module boundaries, invariants
design/components-core.md   — component contracts for oms-core and algo-sor
design/components-codec.md  — component contracts for oms-codec
design/decisions.md         — architectural decisions, trade-offs, reversals
design/resilience-review.md — known gaps, fixes, deferred items, satisfied invariants
design/state-model.md       — order state constants, transition table
design/wire-formats.md      — field offsets, message layouts, FIX Binary format
design/sequences.md         — idempotency, sequencing boundaries, recovery sequence
design/failover.md          — snapshot wire format, restore step sequence
CLAUDE.md                   — resilience principles P1–P6, engineering rules
```

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

While reading, extract:

- Every public method on every class that represents an externally
  observable behaviour (order submission, fill receipt, cancel, reject,
  state transition, snapshot, restore, failover)
- Every validation rule (error codes and conditions in ValidationEngine
  and ChildOrderIntentValidator)
- Every configuration parameter (OmsConfig fields, env var names, defaults)
- Every hard limit or ceiling (static final constants, array sizes,
  wire format field widths)
- Every external interface (Aeron stream endpoints, FIX message types,
  cluster ingress)
- Every item marked as planned or deferred in source comments
  (TODO, FIXME, or inline notes)

---

## Step 2 — Resolve conflicts between design docs and source

A conflict exists when a design doc and the source code describe the
same fact differently. This step must be completed before classification
or spec writing.

### What counts as a conflict

- A constant value in a design doc does not match the value in source
  (e.g. `design/components-codec.md` says `SNAPSHOT_VERSION = 2` but
  source shows `SNAPSHOT_VERSION = 3`)
- A method signature in a design doc does not match the actual method
  in source (e.g. design doc shows a no-arg constructor but source has
  only a parameterised one)
- A behaviour described in a design doc is not present in source
  (e.g. design doc says `republishRoutingOrders()` is called in
  `onStart()` but the method does not exist)
- A constraint or invariant in a design doc is violated in source
  (e.g. design doc says no `new` in `onFragment()` but source contains
  one)
- A component listed in a design doc does not exist in source, or
  vice versa

### What does not count as a conflict

- Design docs describing intent or rationale that has no direct source
  equivalent (this is normal — design docs explain why, source shows how)
- Design docs describing planned or deferred items explicitly marked
  as such — these are not conflicts, they are known gaps
- Naming differences where the concept is clearly the same
  (e.g. "snapshot buffer" in a design doc vs `snapshotBuffer` field in source)

### Resolution rule — source always wins for facts

For every conflict found:

1. Record it in a conflicts table (see format below)
2. Use the source code value as the fact in the spec
3. Do not silently resolve — every conflict is surfaced to the user
   at the Gate

**Conflicts table format:**

```
CONFLICT <id>
Design doc  : <file and line or section>
States      : <what the design doc says>
Source      : <class#field or method>
Shows       : <what the source actually shows>
Impact      : <which spec section(s) this affects>
Recommended : Update <design doc file> to match source before next
              regeneration of this spec.
```

### After recording all conflicts

Continue to Step 2b — do not stop for conflicts at this point. All
conflicts are presented together at the Gate alongside the section plan.
The user decides whether to fix the design docs first or proceed with
the spec reflecting source truth.

---

## Step 2b — Classify each item

For each extracted item assign one of:

- **SHALL** — fully implemented in source, observable, has test coverage
- **SHOULD** — implemented in source but no test found, or partially implemented
- **[PLANNED]** — documented in design docs as intended but not present in source
- **[DEFERRED]** — explicitly deferred in resilience-review.md or decisions.md
  with a documented reason

---

## !! GATE — Mandatory stop before writing the file !!

After completing Steps 0–2b, STOP.

Present two things to the user:

**1. Conflicts report**
If any conflicts were found, list the full conflicts table.
If no conflicts were found, state: "No conflicts found between design
docs and source."

**2. Section plan**
For each section in Step 3, write one paragraph describing what it
will contain, how many requirements or items it will include, and
which source or design doc each draws from.

Then output exactly this message:

```
─────────────────────────────────────────────────
GATE — Conflicts and specification plan review

Review the conflicts report and section plan above.

If conflicts are listed, you may want to fix the design docs first
and re-run this skill. Or proceed and the spec will reflect source truth,
with each conflict noted inline.

Reply with one of:

  "proceed"
      → write docs/functional-spec.md as planned; inline all conflicts

  "proceed, expand <section name>"
      → write the document but add more depth to the named section

  "proceed, skip <section name>"
      → write the document omitting the named section

  "cancel"
      → stop here; no file is written; fix design docs and re-run

Do not write any file until you have received one of the above replies.
─────────────────────────────────────────────────
```

---

## Step 3 — Write docs/functional-spec.md

Create the `docs/` directory if it does not exist.

Start the file with this header block:

```
# MarketOMS — Functional Specification

Generated: <date>
Source: design/ docs + source as of master HEAD <git sha>

> This document is derived from design/ and source. Do not edit directly.
> To update: change the relevant design/ file or source, then re-run
> skills/spec-generator/SKILL.md.

## Conflicts with design docs

<insert conflicts table here, or "None detected." if clean>
```

Then write these sections in order.

---

### Section 1 — System Overview

Two to three paragraphs. Answer:
- What is MarketOMS and what problem does it solve?
- Who operates it and who are the end clients?
- What is its role in a sell-side trading infrastructure?

Derive from: `design/principles.md` system identity block, README.md.
Do not use implementation details here — business language only.

---

### Section 2 — Actors and External Interfaces

A table for each external interface:

| Actor | Interface | Protocol | Direction | Notes |
|-------|-----------|----------|-----------|-------|

Actors to identify (look for evidence of each in source):
- Buy-side client (order submitter)
- Trading venues (execution destinations)
- FIX connectivity engine (bridge between OMS and venues)
- Cluster operator (deployment, configuration)
- Monitoring / ops tooling

For each interface include: message types exchanged, stream ID or port,
and the class that owns the endpoint.

Where a conflict exists between the channel map in `design/principles.md`
and the actual stream IDs in `AeronTransport`, note it inline:
> ⚠ Conflict C-N: spec reflects source value.

Derive from: `design/principles.md` Section 3.2 channel map,
`AeronTransport`, `OmsClusteredService`, `FIXMessageEncoder`,
`FIXMessageDecoder`.

---

### Section 3 — Order Lifecycle

Narrative description of the complete journey of a parent order from
receipt to terminal state. Written in plain English for a non-developer
reader. Includes:

- How a new order enters the system (ingress path, validation steps)
- How routing decisions are made (algo engine selection, venue selection)
- How child orders are created and dispatched to venues
- How fills are received and aggregated back to the parent
- How terminal states are reached (FILLED, CANCELED, REJECTED, EXPIRED)
- What happens on a leader failover mid-lifecycle

Reference the state machine transitions by state name (not byte value).
Where a conflict exists between `design/state-model.md` and source
state constants, note it inline and use source values.

Derive from: `design/state-model.md`, `design/sequences.md`,
`OmsClusteredService`, `ChildOrderRegistry`, `AlgoSorAgent`.

---

### Section 4 — Child Order Lifecycle

Describe the intent-based slicing protocol:
- How the algo engine decides to slice a parent order
- What a ChildOrderIntent contains and what constraints it must satisfy
- The seven validation rules that gate child order creation
- How child clOrdId uniqueness is guaranteed
- How the over-allocation guard prevents double-slicing
- What happens to open child orders on failover

Where the number of validation rules in `design/sequences.md` differs
from the actual rule count in `ChildOrderIntentValidator` source, note
the conflict inline and use the source count.

Derive from: `design/sequences.md`, `ChildOrderIntentValidator`,
`ChildOrderRegistry`, `ChildOrderIntentFlyweight`.

---

### Section 5 — Validation Rules

Two sub-sections. Source is authoritative for both — design docs
describe these at a high level only.

**5.1 New Order Validation (parent orders)**
Derive entirely from `ValidationEngine.validateNewOrder()` source.
For each rule:

| Rule ID | Condition | Error Code | Business Meaning |

**5.2 Intent Validation (child order creation)**
Derive entirely from `ChildOrderIntentValidator.validate()` source.
For each rule:

| Rule ID | Condition | Result | Business Meaning |

Use business language in the Business Meaning column.
If the rule count differs from any design doc description, note the
conflict inline and use the source count.

---

### Section 6 — Non-Functional Requirements

Number each NFR-001 onwards.

**6.1 Latency**
State the latency target and how the codebase enforces it (zero-GC,
busy-spin, off-heap storage, single-threaded components).

**6.2 Availability and Fault Tolerance**
State the replication model (3-node Raft), what a leader failover
looks like from a client perspective, and the RTO given the snapshot
interval found in source.
Where the snapshot interval in `design/failover.md` differs from
the timer value in `OmsClusteredService` source, note the conflict
and use the source value.

**6.3 Consistency**
State the consistency guarantee: what the Raft commit path ensures,
what deterministic replay provides, and what the dedup mechanism
protects against.

**6.4 Durability**
State what survives a leader crash (snapshotted state), what does not
(algo-sor in-process state), and the archive durability requirement.

Derive from: `CLAUDE.md` resilience principles, `design/principles.md`,
`design/resilience-review.md`, `design/failover.md`.

---

### Section 7 — Configuration Reference

A table of every configurable parameter:

| Env Var | Default | Component | Effect | Constraint |

Source is authoritative for default values — use values from `OmsConfig`
source, not from `design/glossary.md`. Where they differ, note the
conflict inline and use the source value.

Include all fields from `OmsConfig` plus any env vars read in `OmsNode`
or `OmsLauncher` that are not in `OmsConfig`.

For `OMS_MAX_ORDERS` and `OMS_MAX_CHILDREN` note the archive-wipe
requirement when changing after a snapshot has been taken.

Derive from: `OmsConfig`, `oms-node.properties.example`, `OmsNode`,
`OmsLauncher`.

---

### Section 8 — Hard Limits and Ceilings

A table of every capacity ceiling currently enforced in the codebase:

| Limit | Current Value | Location | What Happens When Reached | Configurable? |

Source is authoritative for all values. Where a value in a design doc
differs from source, note the conflict inline and use the source value.

Include: order book capacity, child registry capacity, max venues,
fragment limits, wire format field widths that cap value ranges,
state machine table dimensions.

Derive from: `OmsConfig` defaults, `OrderState.NUM_STATES`,
`ChildOrderIntentFlyweight` field widths, `OrderLayout.BLOCK_LENGTH`.

---

### Section 9 — Out of Scope and Known Limitations

Two sub-sections.

**9.1 Not Implemented (Planned)**
Items documented in design docs as intended future work but absent
from source.
For each: description, source reference (design doc and section).

**9.2 Known Gaps (Deferred)**
Items in `design/resilience-review.md` with status DOCUMENTED (deferred)
or MEDIUM/LOW findings not yet resolved.
For each: description, severity, reason for deferral, conditions that
would trigger re-evaluation.

Design docs are authoritative for this section — it describes what is
not in source, so source cannot contradict it.

---

## Step 4 — Post-write output

After writing the file, output:

```
docs/functional-spec.md written.

Generated from: master HEAD <git sha>
Date: <date>

Sections       : 9
Conflicts found: N  (see top of spec for detail)
SHALL items    : N
SHOULD items   : N
[PLANNED] items: N
[DEFERRED] items: N
Config params  : N
Hard limits    : N

Next steps:
- Review the Conflicts section at the top of the spec.
- For each conflict: update the named design/ file to match source,
  then re-run this skill to get a clean spec.
- Do not edit functional-spec.md directly.
- Do not commit this file until conflicts are resolved or accepted.
```

Do not commit the file. Do not modify any source file or design doc.