# Phase 3 Specification – Make the System Coherent

## Status
**Authoritative**
This document is binding for Phase 3 work.

Phase 3 exists to eliminate **structural inconsistency**.
After Phase 3, canonic-labs must feel like a single system, not a set of correct but disconnected parts.

This phase is about **coherence**, not new power.

---

## Phase 3 Goals

Phase 3 addresses the following problems:

1. PostgreSQL registry exists but is not clearly authoritative
2. CLI operates locally instead of interacting with the control plane
3. SQL parser failures are opaque and confusing to users

Phase 3 explicitly does **not** change:
- safety guarantees
- authorization semantics
- planner rules

It connects existing pieces into a unified experience.

---

## In-Scope Work (MANDATORY)

### 7. Decide and Enforce a Single Metadata Authority

#### Objective
There must be exactly **one source of truth** for system metadata.

Partial registries are forbidden.

---

#### Decision (MANDATED)

**PostgreSQL is the authoritative registry.**

All of the following MUST be persisted in PostgreSQL:
- virtual tables
- capabilities
- constraints
- engine declarations
- role-to-table mappings

In-memory or file-backed registries may exist **only for tests**.

---

#### Enforcement Rules

- Any metadata read by the gateway MUST come from PostgreSQL
- Any metadata written via CLI or API MUST persist to PostgreSQL
- No shadow registries are allowed in production code paths

---

#### Required Actions

- Remove unused or dead repository layers
- Wire PostgreSQL repository into:
  - table registration
  - query planning
  - authorization checks
- Add startup checks to verify database connectivity

---

#### Red-Flag Tests (Required)

- Gateway starts without PostgreSQL → must fail
- Metadata mutation without persistence → must fail
- Two conflicting metadata sources detected → must fail

---

#### Green-Flag Tests (Required)

- Table registered via API is visible to gateway
- Restart does not lose metadata
- Concurrent reads observe consistent state

---

### 8. Connect CLI to Gateway (Read-Only)

#### Objective
The CLI must reflect **real system behavior**, not local simulation.

The CLI becomes a **client**, not an emulator.

---

#### CLI Role (Phase 3)

The CLI MUST:
- authenticate to the gateway
- issue requests via HTTP / API
- display real responses

The CLI MUST NOT:
- bypass the gateway
- maintain its own metadata
- make planning decisions

---

#### Supported CLI Commands (Phase 3)

Read-only commands only:

- `canonic table list`
- `canonic table describe`
- `canonic query`
- `canonic query explain`
- `canonic query validate`
- `canonic doctor`

Mutating commands may exist behind a feature flag but are not required.

---

#### Failure Behavior

If the gateway is unreachable:
- the CLI MUST fail
- error must clearly state connectivity failure

Local fallbacks are forbidden.

---

#### Red-Flag Tests (Required)

- CLI invoked without gateway → must fail
- CLI output diverges from gateway state → must fail
- CLI attempts local planning → must fail

---

#### Green-Flag Tests (Required)

- CLI reflects gateway metadata accurately
- CLI explain output matches gateway explain
- CLI errors propagate unchanged

---

### 9. Explicit Unsupported Syntax Errors

#### Objective
Users must never guess **why** a query failed.

Parser rejections must be explicit, stable, and human-readable.

---

#### Required Parser Behavior

When encountering unsupported syntax, the system MUST:
- identify the unsupported construct
- reject the query immediately
- return a clear error message

Generic parse failures are forbidden where classification is possible.

---

#### Error Message Standards

Error messages MUST include:
- unsupported construct
- example of supported alternative (when possible)
- reference to documentation (future-safe)

Example:
```
Unsupported SQL construct: WINDOW FUNCTION
Supported: simple SELECT without OVER() clauses
```

---

#### Red-Flag Tests (Required)

- WINDOW functions
- CTEs (WITH clauses)
- Vendor-specific hints
- Multiple statements

Each must fail with a **specific**, non-generic error.

---

#### Green-Flag Tests (Required)

- Supported SELECT syntax parses cleanly
- Error classification is deterministic
- Same query always produces same error message

---

## Out-of-Scope (Explicit)

Phase 3 does NOT include:
- New SQL features
- Query rewriting
- Performance tuning
- CLI write operations
- UI / GUI work

Any PR adding the above will be rejected.

---

## Acceptance Criteria

Phase 3 is complete only if:

1. PostgreSQL is the sole metadata authority
2. CLI reflects live gateway state exclusively
3. Unsupported SQL syntax errors are explicit and consistent
4. No dead or shadow registries exist
5. All Phase 3 Red-Flag tests fail before implementation and pass after

---

## Failure Policy

If system state is ambiguous:
**The system MUST fail.**

Coherence is more important than availability at this stage.

---

## Final Statement

Phase 3 does not add features.
It removes confusion.

A coherent system is one users can trust
without reading the source code.
