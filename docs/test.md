# Test Strategy – Red-Flag / Green-Flag TDD

## Purpose

This document defines the **mandatory testing strategy** for canonic-labs.

Testing is not a safety net.
Testing **is the design**.

All development follows **Red-Flag / Green-Flag TDD**, inspired by best practices documented in the *awesome GitHub Copilot* repository:
- explicit intent
- deterministic behavior
- human-owned correctness
- AI-assisted scaffolding, never AI-owned logic

---

## Core Philosophy

> If the system is unsure, it must **fail**.

There is no “best effort” execution.
There is no silent fallback.
There is no undefined behavior.

Every feature must prove:
- when it must fail (Red-Flag)
- when it is allowed to succeed (Green-Flag)

---

## Red-Flag vs Green-Flag (Definitions)

### Red-Flag Tests
Red-Flag tests assert that the system **refuses** unsafe, ambiguous, or unsupported behavior.

They validate:
- invariant enforcement
- capability boundaries
- semantic correctness
- trust guarantees

A passing Red-Flag test means:
> “The system correctly said NO.”

---

### Green-Flag Tests
Green-Flag tests assert that the system **successfully executes** behavior that is explicitly declared safe.

They validate:
- happy paths
- deterministic routing
- correct explanations
- predictable output

A passing Green-Flag test means:
> “The system did exactly what it promised.”

---

## Mandatory TDD Order

For **every feature**:

1. Write Red-Flag test(s)
2. Observe failure
3. Write Green-Flag test(s)
4. Observe failure
5. Implement minimal code
6. All tests pass

Skipping steps is a process violation.

---

## GitHub Copilot Usage Rules (Enforced)

Aligned with *awesome-copilot* guidelines:

### Allowed
- Test scaffolding
- Table-driven test boilerplate
- Mock generation
- Repetitive assertion patterns

### Forbidden
- Business logic generation
- Planner decision logic
- Capability enforcement rules
- Authorization logic

Copilot may assist.
Humans decide correctness.

---

## Test Categories

```
/tests
├── redflag
│   ├── gateway
│   ├── table
│   ├── planner
│   ├── engine
│   └── security
├── greenflag
│   ├── gateway
│   ├── table
│   ├── planner
│   ├── engine
│   └── cli
```

No test may exist outside these directories.

---

## Red-Flag Test Cases (Required)

### Gateway – Red Flags

- Unsupported SQL syntax
- Write operation in read-only mode
- Query referencing unknown virtual table
- Ambiguous table resolution

Expected behavior:
- query rejected
- explicit error message
- no engine execution

---

### Table Abstraction – Red Flags

- Missing capability for operation
- Conflicting physical sources
- Unsupported format declaration
- Constraint violation

Expected behavior:
- table rejected
- registration blocked
- human-readable explanation

---

### Planner / Router – Red Flags

- No compatible engine available
- Conflicting routing rules
- Snapshot request without snapshot-capable engine

Expected behavior:
- planning fails
- no fallback attempted

---

### Engine Adapter – Red Flags

- Engine returns partial failure
- Engine times out
- Engine returns schema mismatch

Expected behavior:
- failure propagated
- no retries with altered semantics

---

### Security – Red Flags

- Unauthorized table access
- Expired token
- Role without permission

Expected behavior:
- immediate failure
- no metadata leakage

---

## Green-Flag Test Cases (Required)

### Gateway – Green Flags

- Valid SELECT query
- Deterministic parsing
- Stable query ID generation

---

### Table Abstraction – Green Flags

- Valid virtual table registration
- Capability-aligned operations
- Correct constraint inheritance

---

### Planner / Router – Green Flags

- SELECT routed to Trino
- AS OF routed to snapshot-capable engine
- Deterministic engine choice

---

### Engine Adapter – Green Flags

- Query executes successfully
- Result set returned unchanged
- Execution metadata collected

---

### CLI – Green Flags

- canonic table validate passes
- canonic query explain is accurate
- canonic doctor reports healthy system

---

## Assertion Standards

All tests must assert:
- error type
- error message
- error cause (when applicable)

String matching on error messages is allowed and encouraged.

---

## Anti-Patterns (Explicitly Forbidden)

- Tests that only assert “no panic”
- Tests without explicit expected failure
- Tests that mock business logic behavior
- Tests that assume engine correctness

If a test cannot explain *why* it fails, it is invalid.

---

## Definition of Test Completion

A feature is considered **tested** only if:
- at least one Red-Flag test exists
- at least one Green-Flag test exists
- both fail before implementation
- both pass after implementation

Anything else is incomplete.

---

## Final Rule

> **Trust is built by refusal, not success.**

If a test suite is mostly green-path tests,
the system is unsafe.

Red-Flag tests come first.
Always.
