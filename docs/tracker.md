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
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: MVP starts with DuckDB only; Trino is the primary production engine
- **Context**: Implemented in `internal/adapters/trino/adapter.go` with full `EngineAdapter` interface support, configurable connection (host, port, catalog, schema, user, SSL).
- **Related**: `docs/plan.md` §2.4, `internal/adapters/duckdb/adapter.go`
- **Completed**: 2026-01-10

---

### [T003] Spark Engine Adapter
- **Status**: Completed ✅
- **Priority**: P2 (Medium)
- **Rationale**: Spark is fallback engine; not required for read-only MVP
- **Context**: Implemented in `internal/adapters/spark/adapter.go` with TIME_TRAVEL capability support, TCP connectivity validation, HiveServer2 protocol ready.
- **Related**: `docs/plan.md` §2.4
- **Completed**: 2026-01-10

---

### [T004] Write Support
- **Status**: Deferred
- **Priority**: P2 (Medium)
- **Rationale**: MVP is read-only by design; writes require careful capability enforcement
- **Context**: Enable INSERT, UPDATE, DELETE operations with explicit capability checks. Requires `WRITE` capability and engine support.
- **Related**: `docs/plan.md` §MVP Scope

---

### [T005] Cross-Engine Joins
- **Status**: Completed ✅
- **Priority**: P3 (Low)
- **Rationale**: Phase 9 spec implemented with federation support
- **Context**: Cross-engine query federation is now supported. Implementation includes:
  - `ErrCrossEngineQuery` error type for explicit detection
  - Planner cross-engine detection based on table format/engine preferences
  - Gateway integration with `FederatedExecutor` when `EnableFederation=true`
  - `GatewayAdapterBridge` to bridge gateway adapters to federation interface
  - Red-Flag tests: `TestFederatedExecutor_MissingAdapter`, `TestFederatedExecutor_EngineUnavailable`, `TestPlanner_CrossEngineQueryRejected`
  - Green-Flag test: `TestFederatedExecutor_CrossEngineSuccess`
- **Related**: `docs/phase-9-spec.md`, `internal/federation/`, `internal/planner/planner.go`
- **Completed**: 2025-01-15

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

### [T009] Replace Regex SQL Parser with Vitess
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Current regex-based parsing is a placeholder; production requires proper AST traversal
- **Context**: Replaced regex patterns in `internal/sql/parser.go` with vitess/sqlparser for robust SQL parsing. Handles JOINs, subqueries, UNION, schema-qualified names. Does NOT support CTEs (see T013).
- **Related**: `internal/sql/parser.go`, `tests/redflag/parser_ast_test.go`
- **Completed**: 2025-01-15

---

### [T010] CLI-to-Gateway Integration for Tables
- **Status**: Deferred
- **Priority**: P1 (High)
- **Rationale**: CLI currently operates locally; needs HTTP integration with gateway
- **Context**: Connect CLI table commands to gateway HTTP API. Affects `table register`, `table get`, `table list`, and `table delete` commands.
- **Related**: `internal/cli/table.go:79,178,213,261`, `internal/gateway/gateway.go`

---

### [T011] CLI Query Execution via Gateway
- **Status**: Deferred
- **Priority**: P1 (High)
- **Rationale**: CLI query command needs HTTP integration
- **Context**: Send queries from CLI to gateway `/query` endpoint instead of local execution.
- **Related**: `internal/cli/query.go:62`

---

### [T012] CLI Server Version Check
- **Status**: Completed ✅
- **Priority**: P3 (Low)
- **Rationale**: Enables debugging version mismatches between CLI and gateway
- **Context**: CLI `canonica version` command now queries gateway `/health` endpoint to display server version and status. Supports both text and JSON output formats.
- **Related**: `internal/cli/version.go`, `internal/cli/gateway_client.go:GetHealthInfo()`
- **Completed**: 2026-01-11
- **Implementation**: Added `GetHealthInfo()` method to GatewayClient; version command displays server info when gateway is configured

---

## Completed

### [T002] Trino Engine Adapter
- **Completed**: 2026-01-10
- **Implementation**: `internal/adapters/trino/adapter.go`
- **Tests**: `tests/redflag/trino_test.go`, `tests/greenflag/trino_test.go`

---

### [T003] Spark Engine Adapter
- **Completed**: 2026-01-10
- **Implementation**: `internal/adapters/spark/adapter.go`
- **Tests**: `tests/redflag/spark_test.go`, `tests/greenflag/spark_test.go`

---

### [T013] CTE (WITH clause) Support in SQL Parser
- **Status**: Completed ✅
- **Priority**: P2 (Medium)
- **Rationale**: Migrated from xwb1989/sqlparser to dolthub/vitess parser which fully supports CTEs
- **Context**: Common Table Expressions (WITH clause) are now fully supported. The parser extracts underlying tables from CTE definitions and correctly excludes CTE aliases from the table list.
- **Related**: `internal/sql/parser.go`, `tests/redflag/parser_ast_test.go:TestParser_ExtractsCTETables`
- **Completed**: 2026-01-11
- **Implementation**: `extractTablesFromSelectWithAsOf()` handles `Select.With` field

---

### [T014] FOR SYSTEM_TIME AS OF Support in SQL Parser
- **Status**: Completed ✅
- **Priority**: P2 (Medium)
- **Rationale**: Migrated to dolthub/vitess parser which supports AS OF syntax via `AliasedTableExpr.AsOf` field
- **Context**: Time travel syntax is now parsed via AST. The parser extracts `AsOf.Time` from table expressions and sets `HasTimeTravel` and `TimeTravelTimestamp` on the LogicalPlan.
- **Related**: `internal/sql/parser.go`, `tests/greenflag/parser_ast_test.go:TestParser_DetectsTimeTravel`
- **Completed**: 2026-01-11
- **Implementation**: `extractTablesFromTableExprWithAsOf()` handles `AliasedTableExpr.AsOf`

---

### [T015] Per-Table AS OF Syntax Support
- **Status**: Completed ✅
- **Priority**: P2 (Medium)
- **Rationale**: Per-table time travel enables snapshot mismatch detection for SNAPSHOT_CONSISTENT tables
- **Context**: Parser now extracts per-table timestamps via `LogicalPlan.TimeTravelPerTable` map. Planner validates that all SNAPSHOT_CONSISTENT tables use the same snapshot timestamp. Test `TestSnapshotConsistent_RejectsSnapshotMismatch` now enabled and passing.
- **Related**: `internal/sql/parser.go`, `internal/planner/planner.go:checkSnapshotConsistency()`
- **Completed**: 2026-01-11
- **Implementation**: Added `TimeTravelPerTable map[string]string` to LogicalPlan; extraction functions propagate per-table timestamps; planner enforces timestamp consistency

---

### [T016] SNAPSHOT_CONSISTENT Enforcement
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Phase 1 specification requires full SNAPSHOT_CONSISTENT enforcement
- **Context**: Implemented in `internal/planner/planner.go` via `checkSnapshotConsistency()`. Enforces: (1) queries on SNAPSHOT_CONSISTENT tables must include AS OF clause, (2) cannot mix SNAPSHOT_CONSISTENT with non-SNAPSHOT_CONSISTENT tables in same query.
- **Related**: `docs/plan.md` §Phase 1, `tests/redflag/snapshot_consistent_test.go`, `tests/greenflag/snapshot_consistent_test.go`
- **Completed**: 2025-01-15

---

### [T017] Role → Table Authorization (Deny-by-Default)
- **Status**: Completed ✅
- **Priority**: P0 (Critical)
- **Rationale**: Phase 2 specification requires deny-by-default authorization
- **Context**: Implemented in `internal/auth/authorization.go` via `AuthorizationService`. Features: (1) Role → Table → Capability mapping, (2) Deny-by-default - no implicit access, (3) Multi-table authorization requires permission on ALL tables, (4) Authorization checked BEFORE planning (no table existence leakage).
- **Related**: `docs/phase-2-spec.md` §4, `tests/redflag/authorization_test.go`, `tests/greenflag/authorization_test.go`
- **Completed**: 2026-01-10

---

### [T018] Schema-Qualified Table Names Enforcement
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Phase 2 specification requires fully-qualified table names
- **Context**: Implemented in `internal/sql/parser.go` via `ValidateTableName()`. Enforces format `<schema>.<table>`. Unqualified names are rejected with clear error message explaining required format. Gateway validates all table references before authorization and planning.
- **Related**: `docs/phase-2-spec.md` §6, `tests/redflag/table_naming_test.go`, `tests/greenflag/table_naming_test.go`
- **Completed**: 2026-01-10

---

### [T019] PostgreSQL as Single Metadata Authority
- **Status**: Completed ✅
- **Priority**: P0 (Critical)
- **Rationale**: Phase 3 specification requires PostgreSQL as sole source of truth for metadata
- **Context**: Implemented via `NewGatewayWithDB()` and `NewGatewayWithRepository()` constructors in `internal/gateway/gateway.go`. Features: (1) Gateway fails to start without database connection, (2) Connectivity checked at startup, (3) `ProductionMode` rejects in-memory registries, (4) Metadata mutations require persistence, (5) `DetectMetadataConflict()` rejects conflicting sources. MockRepository enhanced with `SetConnectivityFailure()`, `SetPersistenceFailure()`, `CheckConnectivity()` for testing.
- **Related**: `docs/phase-3-spec.md` §7, `tests/redflag/metadata_authority_test.go`, `tests/greenflag/metadata_authority_test.go`
- **Completed**: 2026-01-15

---

### [T020] CLI Gateway Connection (Read-Only)
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Phase 3 specification requires CLI to connect to gateway, not operate locally
- **Context**: Implemented `GatewayClient` in `internal/cli/gateway_client.go`. Features: (1) CLI is a client, not an emulator, (2) All operations proxy to gateway - `ListTables()`, `DescribeTable()`, `ExplainQuery()`, `ValidateQuery()`, `ExecuteQuery()`, `CheckHealth()`, (3) No local fallbacks - operations fail cleanly when gateway unavailable, (4) Authentication token included in all requests, (5) Errors propagate unchanged from gateway.
- **Related**: `docs/phase-3-spec.md` §8, `tests/redflag/cli_gateway_test.go`, `tests/greenflag/cli_gateway_test.go`
- **Completed**: 2026-01-15

---

### [T021] Explicit Unsupported Syntax Errors
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Phase 3 specification requires clear, consistent errors for unsupported SQL constructs
- **Context**: Implemented via `detectUnsupportedSyntax()`, `detectVendorHints()`, `classifyParseError()` in `internal/sql/parser.go`. Features: (1) WINDOW functions detected via OVER clause patterns with specific error, (2) CTEs (WITH clause) detected with explicit message, (3) Vendor hints (USE INDEX, FORCE INDEX, optimizer hints) rejected with `ErrVendorHint`, (4) Multiple statements rejected, (5) Error messages include construct name and alternative suggestion. New error types: `ErrUnsupportedSyntax`, `ErrVendorHint` in `internal/errors/errors.go`.
- **Related**: `docs/phase-3-spec.md` §9, `tests/redflag/unsupported_syntax_test.go`, `tests/greenflag/supported_syntax_test.go`
- **Completed**: 2026-01-15
---

### [T022] Readiness and Liveness Endpoints
- **Status**: Completed ✅
- **Priority**: P0 (Critical)
- **Rationale**: Phase 4 specification requires operational observability via Kubernetes-style health endpoints
- **Context**: Implemented `/healthz` and `/readyz` endpoints in `internal/gateway/gateway.go`. Features: (1) `/healthz` reports process liveness only (always 200 if process running), (2) `/readyz` reports component readiness (database, engines, metadata), (3) Gateway refuses queries when not ready (503 status), (4) Component-level details in JSON response. New response types: `HealthzResponse`, `ReadyzResponse`, `ReadyzComponentsMap`, `ReadyzComponent`.
- **Related**: `docs/phase-4-spec.md` §4, `tests/redflag/readiness_test.go`, `tests/greenflag/readiness_test.go`
- **Completed**: 2026-01-10

---

### [T023] Enhanced Operational Logging
- **Status**: Completed ✅
- **Priority**: P0 (Critical)
- **Rationale**: Phase 4 specification requires structured logging for every request with comprehensive fields
- **Context**: Extended `QueryLogEntry` in `internal/observability/logger.go` with Phase 4 required fields. New fields: (1) `Role` for user role, (2) `AuthorizationDecision` for auth outcome, (3) `PlannerDecision` for routing logic, (4) `Outcome` for success/error status, (5) `InvariantViolated` for failure diagnosis. All fields logged in JSON output. Silent failures are now structurally forbidden.
- **Related**: `docs/phase-4-spec.md` §5, `tests/redflag/logging_test.go`, `tests/greenflag/logging_test.go`
- **Completed**: 2026-01-10

---

### [T024] CLI Commands Wire to GatewayClient
- **Status**: In Progress
- **Priority**: P1 (High)
- **Rationale**: Phase 4 specification requires CLI commands to use GatewayClient exclusively, not local logic
- **Context**: `GatewayClient` exists in `internal/cli/gateway_client.go` but CLI commands in `table.go`, `query.go` still have local implementations with TODO comments. Need to refactor to delete local logic and use `GatewayClient` for all operations.
- **Related**: `docs/phase-4-spec.md` §3, `internal/cli/table.go`, `internal/cli/query.go`

---

### [T025] Wire PostgresRepository to Gateway
- **Status**: In Progress  
- **Priority**: P1 (High)
- **Rationale**: Phase 4 specification requires PostgreSQL to be wired to all control paths
- **Context**: `PostgresRepository` is implemented in `internal/storage/postgres_repository.go` but `NewGatewayWithRepository()` still uses `InMemoryTableRegistry` instead of the repository for table operations. Need to wire repository through planner.
- **Related**: `docs/phase-4-spec.md` §2, `internal/gateway/gateway.go`

---

### [T026] Canonical Configuration System
- **Status**: Completed ✅
- **Priority**: P0 (Critical)
- **Rationale**: Phase 5 specification requires YAML-based configuration with strict validation
- **Context**: Implemented in `internal/bootstrap/config.go`. Features: (1) Strict unknown field rejection using yaml.DisallowUnknownFields(), (2) Required section validation for gateway, database, tables, (3) Schema-qualified table name enforcement, (4) Capability and constraint validation using existing packages, (5) Config round-trips via Save(). New type `Config` with `LoadConfig()`, `Validate()`, `Apply()`, `ApplyToRepository()`.
- **Related**: `docs/phase-5-spec.md` §1, `tests/redflag/bootstrap_test.go`, `tests/greenflag/bootstrap_test.go`
- **Completed**: 2026-01-15

---

### [T027] Bootstrap CLI Commands
- **Status**: Completed ✅
- **Priority**: P0 (Critical)
- **Rationale**: Phase 5 specification requires ergonomic bootstrap workflow via CLI
- **Context**: Implemented in `internal/cli/bootstrap.go`. Commands: (1) `bootstrap init` generates example config, (2) `bootstrap validate` checks config without side effects, (3) `bootstrap apply` provisions metadata to repository. All commands use `internal/bootstrap` package.
- **Related**: `docs/phase-5-spec.md` §2, `tests/greenflag/bootstrap_test.go`
- **Completed**: 2026-01-15

---

### [T028] EXPLAIN CANONIC
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Phase 5 specification requires transparent query routing inspection
- **Context**: Implemented `ExplainCanonic()` in `internal/gateway/gateway.go`. Features: (1) Returns `ExplainCanonicResult` with tables, route, reason, and will_refuse flag, (2) Uses same planner path as execute, (3) Does not reveal authorization rejection reasons for security. HTTP endpoint `/query/explain-canonic`.
- **Related**: `docs/phase-5-spec.md` §3, `tests/redflag/explain_canonic_test.go`, `tests/greenflag/explain_canonic_test.go`
- **Completed**: 2026-01-15

---

### [T029] Status and Audit CLI Commands
- **Status**: Completed ✅
- **Priority**: P1 (High)
- **Rationale**: Phase 5 specification requires minimal operator insight without raw data exposure
- **Context**: Implemented in `internal/cli/bootstrap.go` and `internal/status/status.go`. Commands: (1) `status` shows gateway status with engine readiness and version info, (2) `audit summary` shows aggregated query statistics. No raw data exposed per spec. HTTP endpoints `/audit/summary`. `GetAuditSummary()` implemented in `internal/observability/logger.go`.
- **Related**: `docs/phase-5-spec.md` §4, `tests/redflag/status_test.go`, `tests/greenflag/status_test.go`
- **Completed**: 2026-01-15