# Tracker.md — Deferred Features & Technical Debt

## Purpose

This document tracks **deferred features, future improvements, and technical debt** for canonica-labs.

All TODOs in code MUST have a corresponding entry here.
Before implementing a deferred feature, check this document for context.

---

## Deferred Features

### [T001] JWT Authentication
- **Status**: Deferred
- **Priority**: P1 (High)
- **Rationale**: MVP uses static token auth for simplicity; JWT required for production
- **Context**: Current implementation uses static tokens in config. Replace with full JWT flow including token refresh, expiration, and role claims.
- **Related**: `internal/auth/auth.go`, `docs/plan.md` §4.3

---

### [T002] Trino Engine Adapter
- **Status**: Deferred
- **Priority**: P1 (High)
- **Rationale**: MVP starts with DuckDB only; Trino is the primary production engine
- **Context**: Implement `internal/adapters/trino/adapter.go` following the `EngineAdapter` interface. Trino handles distributed queries across lakehouse formats.
- **Related**: `docs/plan.md` §2.4, `internal/adapters/duckdb/adapter.go`

---

### [T003] Spark Engine Adapter
- **Status**: Deferred
- **Priority**: P2 (Medium)
- **Rationale**: Spark is fallback engine; not required for read-only MVP
- **Context**: Implement `internal/adapters/spark/adapter.go`. Spark required for future write support and complex transformations.
- **Related**: `docs/plan.md` §2.4

---

### [T004] Write Support
- **Status**: Deferred
- **Priority**: P2 (Medium)
- **Rationale**: MVP is read-only by design; writes require careful capability enforcement
- **Context**: Enable INSERT, UPDATE, DELETE operations with explicit capability checks. Requires `WRITE` capability and engine support.
- **Related**: `docs/plan.md` §MVP Scope

---

### [T005] Cross-Engine Joins
- **Status**: Deferred
- **Priority**: P3 (Low)
- **Rationale**: Complex feature; not in MVP scope
- **Context**: Allow queries that join tables across different engines. Requires query decomposition and result merging.
- **Related**: `docs/plan.md` §Exclusions

---

### [T006] Kubernetes Deployment
- **Status**: Deferred
- **Priority**: P2 (Medium)
- **Rationale**: MVP uses Docker Compose; Kubernetes for production scalability
- **Context**: Create Helm charts or Kubernetes manifests for production deployment.
- **Related**: `docs/plan.md` §Tech Stack

---

### [T007] Cost-Based Query Optimization
- **Status**: Deferred
- **Priority**: P3 (Low)
- **Rationale**: MVP uses rule-based routing; cost optimization is future enhancement
- **Context**: Collect query statistics and use them for smarter engine selection.
- **Related**: `docs/plan.md` §Exclusions

---

### [T008] Streaming Support
- **Status**: Deferred
- **Priority**: P3 (Low)
- **Rationale**: MVP focuses on batch queries; streaming is separate use case
- **Context**: Support for streaming queries and real-time data processing.
- **Related**: `docs/plan.md` §Exclusions

---

## Technical Debt

_(No entries yet)_

---

## Completed

_(Move items here when implemented)_
