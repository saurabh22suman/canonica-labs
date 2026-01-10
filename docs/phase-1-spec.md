# Phase 1 Specification – Make the System Honest

## Status
**Authoritative**
This document is binding for Phase 1 work.

Phase 1 exists to eliminate false guarantees, unsafe assumptions, and misleading signals.
After Phase 1, the system must be *honest* even if it is still incomplete.

---

## Phase 1 Goals

Phase 1 addresses the following problems:

1. SQL parsing is unsafe and incomplete (regex-based)
2. SNAPSHOT_CONSISTENT is declared but not enforced
3. Red-Flag tests are polluted with non-invariant failures

Phase 1 explicitly does **not** add new features.
It removes ambiguity and enforces truth.

---

## In-Scope Work (MANDATORY)

### 1. Replace Regex SQL Parsing with AST Parsing

#### Objective
Replace all regex-based SQL inspection with a real SQL Abstract Syntax Tree (AST) parser.

#### Parser Choice
- **Vitess SQL Parser**
- Postgres-compatible dialect
- Read-only semantics only

#### Requirements
- All queries MUST be parsed into an AST
- If parsing fails, the query MUST be rejected
- No fallback to regex or string inspection is allowed

#### Enforcement Scope
The AST parser must reliably identify:
- Statement type (SELECT, DELETE, UPDATE, etc.)
- Referenced tables (including aliases)
- Subqueries and nested selects
- AS OF / time-travel clauses
- Unsupported or ambiguous syntax

#### Explicit Rejections
The parser MUST reject:
- Multiple statements
- DDL statements
- Vendor-specific extensions not explicitly supported
- Queries where table resolution is ambiguous

#### Red-Flag Tests (Required)
- Nested SELECT bypassing regex (must fail before fix)
- Alias-based table masking
- Quoted identifiers
- Multi-statement SQL

#### Green-Flag Tests (Required)
- Valid SELECT with aliases
- Nested subqueries resolved correctly
- Deterministic table extraction

---

### 2. Implement SNAPSHOT_CONSISTENT (Fully)

#### Objective
SNAPSHOT_CONSISTENT must be a real invariant, not a documented fiction.

#### Definition
A SNAPSHOT_CONSISTENT query guarantees:
- All referenced tables are read from a single logical snapshot
- Snapshot identity is known and verifiable
- Engine chosen must support snapshot isolation semantics

#### Enforcement Rules
- Queries referencing SNAPSHOT_CONSISTENT tables MUST:
  - declare snapshot intent explicitly (AS OF)
  - be routed only to snapshot-capable engines
- If multiple tables are involved:
  - snapshots must be compatible
  - otherwise the query MUST fail

#### Engine Capability Declaration
Each engine adapter must declare:
- supports_snapshot = true | false

#### Failure Behavior
If SNAPSHOT_CONSISTENT is requested but cannot be guaranteed:
- the query MUST be rejected
- error message must explain why consistency cannot be ensured

#### Red-Flag Tests (Required)
- SNAPSHOT_CONSISTENT table queried without AS OF
- Mixed snapshot-capable and non-capable engines
- Snapshot mismatch across tables

#### Green-Flag Tests (Required)
- Valid AS OF query on snapshot-capable engine
- Single-table snapshot-consistent query

---

### 3. Clean Red-Flag Test Signal

#### Objective
Red-Flag tests must represent **invariant enforcement**, not infrastructure failure.

#### Required Actions
- Reclassify tests that fail due to:
  - missing adapters
  - unimplemented engines
  - stubbed components

These tests must move to:
- integration
- skipped
- or explicit TODO

#### Red-Flag Test Definition (Reaffirmed)
A Red-Flag test MUST:
- represent a semantic or safety violation
- fail regardless of engine availability
- fail before implementation
- pass after invariant enforcement

#### Forbidden Red-Flag Tests
- “engine not available”
- “adapter not implemented”
- “service not running”

These are not invariants.

---

## Out-of-Scope (Explicit)

Phase 1 does NOT include:
- RBAC / authorization
- JDBC support
- Performance optimizations
- New SQL features
- New engines

Any PR adding the above will be rejected.

---

## Acceptance Criteria

Phase 1 is complete only if:

1. No regex-based SQL logic exists in the codebase
2. SNAPSHOT_CONSISTENT is fully enforced
3. Red-Flag tests exclusively represent invariant violations
4. All Phase 1 Red-Flag tests fail before implementation and pass after
5. Error messages are explicit and human-readable

---

## Failure Policy

If a query cannot be proven safe:
**It MUST fail.**

Silence, fallback, or best-effort behavior is forbidden.

---

## Final Statement

Phase 1 does not make the system powerful.
It makes the system **truthful**.

Only truthful systems earn trust.
