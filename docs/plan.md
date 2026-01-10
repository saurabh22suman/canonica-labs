# Unified Lakehouse Access Layer (Canonica-labs) – Implementation Plan

> **Authoritative build contract**
> This document defines WHAT we build, HOW we build it, and WHAT WE DO NOT BUILD.
> Deviations require explicit justification.

---

## 1. Purpose

This project implements a **Unified Lakehouse Access Layer**.

It is a **control plane**, not a data plane.

The system provides:
- Unified Query Gateway
- Table Abstraction Layer
- Planner + Router

The system explicitly does **NOT**:
- replace query engines
- move data
- create a new storage format
- optimize execution plans

---

## 2. Non‑Negotiable Principles

1. **Control Plane Only**
   - No file IO
   - No Parquet readers
   - No commit log writers

2. **Read‑First MVP**
   - Writes are blocked unless explicitly enabled
   - Unsafe operations must fail fast

3. **Format‑Agnostic Behavior**
   - Users never reference Delta, Iceberg, Hudi
   - Users see capabilities, not formats

4. **Adoptable Without Migration**
   - No data movement
   - No engine replacement
   - Safe to remove

5. **Correctness > Performance > Features**
   - Silent corruption is unacceptable
   - Blocking is a feature

---

## 3. Development Methodology

### 3.1 Green‑Flag / Red‑Flag TDD (MANDATORY)

All features MUST follow this order:

1. Write **Red‑Flag tests**
   - Prove the system fails on unsafe behavior

2. Write **Green‑Flag tests**
   - Prove the system succeeds only when semantics are guaranteed

3. Implement minimal code to pass tests

No feature is complete without BOTH test types.

#### Examples

Red‑Flag:
- DELETE on read‑only table
- AS OF query without time‑travel capability
- Query routed to incompatible engine

Green‑Flag:
- SELECT on valid virtual table
- Deterministic engine routing
- Correct error messaging

---

### 3.2 GitHub Copilot Usage Policy

Inspired by **awesome‑copilot** best practices.

Copilot is:
✅ Allowed for:
- boilerplate
- adapters
- test scaffolding
- repetitive glue code

❌ Forbidden for:
- planner rules
- capability enforcement
- authorization logic
- semantic validation

Humans own correctness.

Copilot output MUST:
- compile
- be readable
- pass Red/Green tests

---

## 4. Technology Stack (FINAL)

### 4.1 Language
- **Go 1.22+**
  - control plane services
  - gateway
  - planner
  - adapters

### 4.2 External Query Engines
- Trino (primary read engine)
- Apache Spark (fallback / future write support)
- DuckDB (local / demo / dev)

### 4.3 Metadata Store
- PostgreSQL
  - virtual tables
  - capabilities
  - constraints
  - routing rules
  - audit logs

### 4.4 Interfaces
- HTTP API
- JDBC (Postgres‑compatible SQL)
- CLI

### 4.5 Infrastructure
- Docker (mandatory)
- Docker Compose (MVP)
- Kubernetes (future)

---

## 5. Repository Structure

```
/cmd
  /gateway
  /planner
/internal
  /sql
  /tables
  /capabilities
  /planner
  /router
  /adapters
    /trino
    /spark
    /duckdb
  /auth
  /errors
/pkg
  /api
  /models
/tests
  /redflag
  /greenflag
/docs
  plan.md
```

---

## 6. Component Architecture

### 6.1 Unified Query Gateway

Responsibilities:
- Accept SQL
- Authenticate requests
- Parse SQL into logical plan
- Resolve virtual tables
- Forward to planner

Explicitly does NOT:
- execute queries
- optimize plans
- access storage

Failure is preferred over ambiguity.

---

### 6.2 Table Abstraction Layer (CORE)

This is the product boundary.

#### Virtual Table Model

```go
type VirtualTable struct {
  Name         string
  Sources      []PhysicalSource
  Capabilities []Capability
  Constraints  []Constraint
}
```

Capabilities:
- READ
- TIME_TRAVEL

Constraints:
- READ_ONLY
- SNAPSHOT_CONSISTENT

Rules:
- Missing capability → operation blocked
- Constraint overrides capability

---

### 6.3 Planner + Router

Purpose:
- Choose execution engine
- Choose compatible physical source

MVP rules:
- SELECT → Trino
- AS OF → engine with snapshot support
- Fallback → Spark
- Local dev → DuckDB

Planner is:
- rule‑based
- deterministic
- explainable

---

### 6.4 Engine Adapters

Adapters:
- translate logical plan → engine SQL
- submit query
- collect results
- collect metadata

Adapters are:
- stateless
- replaceable
- thin

No silent retries.
No hidden fallbacks.

---

## 7. Error Philosophy

Errors must be:
- explicit
- human readable
- actionable

Bad:
```
Query failed
```

Good:
```
DELETE forbidden on analytics.sales_orders
Reason: READ_ONLY constraint
```

---

## 8. Security Model (MVP)

- Token‑based auth
- Role → table mapping
- No column‑level security

Security violations are Red‑Flag failures.

---

## 9. Observability (REQUIRED)

Every query must emit:
- query_id
- user
- tables referenced
- engine selected
- execution time
- error (if any)

Structured logging only.

---

## 10. MVP Scope

### Included
- Read‑only SQL
- Delta / Iceberg / Parquet
- Trino + Spark routing
- Virtual tables
- Capability enforcement

### Explicitly Excluded
- Writes
- Streaming
- Cross‑engine joins
- Cost‑based optimization
- AI features

---

## 11. Definition of Done

A feature is DONE only if:
- Red‑Flag tests fail before implementation
- Green‑Flag tests pass after implementation
- Unsafe behavior is blocked
- Errors are understandable

If unsure:
**FAIL THE QUERY**.

---

## 12. Final Warning

This system survives on trust.

Trust is earned by:
- blocking unsafe behavior
- being boring
- never lying

Correctness is the product.
