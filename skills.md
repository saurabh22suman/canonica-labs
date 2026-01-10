# skills.md – Required Skills for canonic-labs

## Purpose

This document defines the **human skills** required to build, review, and evolve canonic-labs.

These skills are **non-negotiable**.  
Tooling (including AI models like Opus 4.5) augments these skills but never replaces them.

---

## Core Engineering Skills (Mandatory)

### 1. Distributed Systems Fundamentals
Engineers must understand:
- control plane vs data plane separation
- idempotency
- deterministic behavior
- failure modes over happy paths

Reason:
canonic-labs coordinates systems it does not control.

---

### 2. SQL Semantics & Query Planning
Engineers must:
- understand SQL logical planning
- reason about query intent vs execution
- recognize unsafe SQL patterns

This system enforces **semantics**, not syntax.

---

### 3. Lakehouse & Table Format Knowledge
Required familiarity:
- Delta Lake (commit logs, time travel)
- Apache Iceberg (snapshots, manifests)
- Parquet (schema, statistics)

Depth requirement:
- conceptual and operational, not implementation-level

---

### 4. Go (Golang) Proficiency
Engineers must:
- write idiomatic Go
- use interfaces intentionally
- avoid hidden state
- prefer explicit error handling

Concurrency knowledge is required.

---

### 5. Test-Driven Development (Red / Green)
Engineers must:
- write failing tests first
- reason about *why* a system must fail
- treat tests as design artifacts

Green-Flag tests without Red-Flag tests are invalid.

---

## Platform & Infra Skills (Strongly Recommended)

- API design
- AuthN / AuthZ basics
- Observability (logs, metrics, traces)
- CI/CD discipline
- GitOps workflows

---

## AI-Assisted Development Skills (Opus 4.5 Context)

When using **Opus 4.5**:

Engineers must:
- provide precise, bounded prompts
- reject speculative or confident-sounding guesses
- validate all generated logic against invariants

Opus is used to:
- accelerate boilerplate
- explore alternatives
- draft documentation

Opus must **never** define system behavior.

---

## Anti-Skills (Explicitly Rejected)

- Blind trust in AI-generated logic
- Over-abstracting early
- Premature optimization
- Feature-first thinking

Correctness-first thinking is mandatory.

---

## Final Statement

canonic-labs is built by engineers who:
- value refusal over guesswork
- prefer boring systems
- treat correctness as a product feature

Skills are enforced through review, not assumption.
