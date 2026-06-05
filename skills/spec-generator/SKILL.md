# Functional Specification Generator Skill

## Purpose

Derive a complete functional specification document from the live codebase
and design docs. Writes to docs/functional-spec.md. Never invents
capabilities not evidenced in source or design docs.

```
Read design docs → Scan source → Draft section summaries
→ [USER APPROVES] → Write docs/functional-spec.md
```

One confirmation gate: user reviews the section plan before the file
is written.

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
- Every item marked as planned or deferred in design docs or comments

---

## Step 2 — Classify each item

For each extracted item assign one of:

- **SHALL** — fully implemented, observable, tested
- **SHOULD** — implemented but not tested, or partially implemented
- **[PLANNED]** — documented in design docs as intended but not yet built
- **[DEFERRED]** — explicitly deferred in resilience-review.md or decisions.md
  with a documented reason

---

## !! GATE — Mandatory stop before writing the file !!

After completing Steps 0–2, STOP.

Present a section plan to the user: for each section listed in Step 3,
write one paragraph describing what it will contain and how many
requirements, constraints, or items it will include.

Then output exactly this message:

```
─────────────────────────────────────────────────
GATE — Specification plan review

Review the section plan above. Then reply with one of:

  "proceed"
      → write docs/functional-spec.md as planned

  "proceed, expand <section name>"
      → write the document but add more depth to the named section

  "proceed, skip <section name>"
      → write the document omitting the named section

  "cancel"
      → stop here; no file is written

Do not write any file until you have received one of the above replies.
─────────────────────────────────────────────────
```

---

## Step 3 — Write docs/functional-spec.md

Create the `docs/` directory if it does not exist.
Write the file with exactly these sections in this order.

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

Derive from: `design/sequences.md`, `ChildOrderIntentValidator`,
`ChildOrderRegistry`, `ChildOrderIntentFlyweight`.

---

### Section 5 — Validation Rules

Two sub-sections.

**5.1 New Order Validation (parent orders)**
For each rule in `ValidationEngine.validateNewOrder()`, state:

| Rule ID | Condition | Error Code | Business Meaning |

**5.2 Intent Validation (child order creation)**
For each of the seven rules in `ChildOrderIntentValidator.validate()`,
state:

| Rule ID | Condition | Result | Business Meaning |

Use business language, not code references, in the Business Meaning column.
Derive from: `ValidationEngine`, `ChildOrderIntentValidator`.

---

### Section 6 — Non-Functional Requirements

Number each NFR-001 onwards.

Sub-sections:

**6.1 Latency**
State the latency target and how the codebase enforces it (zero-GC,
busy-spin, off-heap storage, single-threaded components).

**6.2 Availability and Fault Tolerance**
State the replication model (3-node Raft), what a leader failover
looks like from a client perspective, and the RTO given the 5-minute
snapshot interval.

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

Include: order book capacity, child registry capacity, max venues,
fragment limits, wire format field widths that cap value ranges,
state machine table dimensions.

Derive from: `OmsConfig` defaults, `OrderState.NUM_STATES`,
`ChildOrderIntentFlyweight` field widths, `OrderLayout.BLOCK_LENGTH`.

---

### Section 9 — Out of Scope and Known Limitations

Two sub-sections.

**9.1 Not Implemented (Planned)**
Items documented in design docs as intended future work.
For each: description, source reference (which design doc and section).

**9.2 Known Gaps (Deferred)**
Items in `design/resilience-review.md` with status DOCUMENTED (deferred)
or MEDIUM/LOW findings not yet resolved.
For each: description, severity, reason for deferral, conditions that
would trigger re-evaluation.

---

## Step 4 — Post-write instructions

After writing the file, output:

```
docs/functional-spec.md written.

Word count: ~N words
Sections: 9
Requirements: FR count
NFRs: NFR count
Configuration parameters: N
Known limitations: N

Do not commit this file without reviewing it first. It was derived
from code and design docs as of the date of generation — update it
when either changes.
```

Do not commit the file. Do not modify any source file or design doc.