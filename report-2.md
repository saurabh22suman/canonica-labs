# Technical Audit Report — canonic-labs

**Date**: January 11, 2026  
**Scope**: Forensic design-level analysis of canonica-labs codebase  
**Purpose**: Identify gaps, risks, inconsistencies, and technical debt for senior review

---

## 1. Project Snapshot

### What the System Currently Does

canonica-labs is a **control plane** for unified lakehouse access. It provides:

1. **HTTP Gateway** (`internal/gateway/`) — Accepts SQL queries, authenticates requests, validates syntax, enforces authorization, plans execution, and dispatches to engine adapters
2. **SQL Parser** (`internal/sql/`) — Uses vitess/sqlparser to extract tables, detect time-travel syntax, reject unsupported constructs
3. **Table Abstraction Layer** (`internal/tables/`, `internal/capabilities/`) — Virtual tables with capabilities (READ, TIME_TRAVEL) and constraints (READ_ONLY, SNAPSHOT_CONSISTENT)
4. **Planner** (`internal/planner/`) — Resolves tables, checks capability requirements, selects execution engine
5. **Router** (`internal/router/`) — Deterministic priority-based engine selection
6. **Engine Adapters** (`internal/adapters/`) — DuckDB (functional), Trino, Spark (connectivity only, not integrated)
7. **Authorization** (`internal/auth/authorization.go`) — Role → Table → Capability deny-by-default access control
8. **CLI** (`internal/cli/`) — Commands for table, query, bootstrap, status, audit operations
9. **Configuration System** (`internal/bootstrap/`) — YAML-based config with strict validation

### What the System Explicitly Does NOT Do

- Execute queries on actual remote Trino/Spark clusters (adapters exist but not wired)
- Move or transform data
- Read Parquet/Delta/Iceberg files directly
- Support writes (INSERT, UPDATE, DELETE blocked)
- Cross-engine joins
- Cost-based optimization
- Streaming queries

### Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| Gateway HTTP API | Complete | All endpoints functional |
| SQL Parser | Complete | vitess/sqlparser, lacks CTE support |
| Table Abstraction | Complete | Capability/constraint model working |
| Planner | Complete | Rule-based, deterministic |
| Router | Complete | Priority-based engine selection |
| DuckDB Adapter | **Functional** | Executes real queries |
| Trino Adapter | **Stub** | Code exists, not wired to gateway |
| Spark Adapter | **Stub** | Code exists, not wired to gateway |
| PostgreSQL Repository | **Partial** | Implemented but not wired to gateway operations |
| CLI → Gateway | **Partial** | GatewayClient exists, CLI commands still have local logic |
| Authorization | Complete | Deny-by-default working |
| Readiness/Liveness | Complete | /healthz and /readyz endpoints |
| Bootstrap Config | Complete | YAML loading, validation, apply |
| Audit Summary | Complete | Aggregated statistics |

---

## 2. Repository Map

```
canonica-labs/
├── cmd/
│   ├── canonic/main.go      # CLI entry point (minimal)
│   └── gateway/main.go      # Gateway entry point (minimal)
├── internal/
│   ├── adapters/            # Engine adapters
│   │   ├── adapter.go       # Interface + Registry (COMPLETE)
│   │   ├── duckdb/          # DuckDB adapter (COMPLETE, functional)
│   │   ├── trino/           # Trino adapter (STUB, not integrated)
│   │   └── spark/           # Spark adapter (STUB, not integrated)
│   ├── auth/
│   │   ├── auth.go          # Authentication (COMPLETE, static tokens)
│   │   └── authorization.go # Authorization (COMPLETE, deny-by-default)
│   ├── bootstrap/           # Configuration system (COMPLETE)
│   │   ├── config.go        # YAML config loading/validation
│   │   └── mock_repository.go
│   ├── capabilities/        # Capability/Constraint model (COMPLETE)
│   ├── cli/                 # CLI commands (PARTIAL)
│   │   ├── cli.go           # Root command
│   │   ├── gateway_client.go # HTTP client for gateway (COMPLETE)
│   │   ├── table.go         # Table commands (LOCAL LOGIC, not gateway)
│   │   ├── query.go         # Query commands (LOCAL LOGIC, not gateway)
│   │   └── bootstrap.go     # Bootstrap commands (COMPLETE)
│   ├── config/              # Config struct (MINIMAL)
│   ├── errors/              # Error types (COMPLETE, well-structured)
│   ├── gateway/             # HTTP gateway (COMPLETE)
│   ├── observability/       # Query logging (COMPLETE)
│   ├── planner/             # Query planner (COMPLETE)
│   ├── router/              # Engine router (COMPLETE)
│   ├── sql/                 # SQL parser (COMPLETE, lacks CTEs)
│   ├── status/              # Status checker (COMPLETE)
│   ├── storage/             # Repository layer (PARTIAL, not wired)
│   │   ├── repository.go    # Interface
│   │   ├── postgres_repository.go # PostgreSQL implementation
│   │   └── mock_repository.go
│   └── tables/              # Virtual table model (COMPLETE)
├── pkg/
│   ├── api/                 # Public API types (MINIMAL)
│   └── models/              # Shared models (MINIMAL)
├── tests/
│   ├── redflag/             # 120+ Red-Flag tests (COMPREHENSIVE)
│   └── greenflag/           # 90+ Green-Flag tests (COMPREHENSIVE)
├── migrations/              # PostgreSQL schema (EXISTS, not verified against code)
├── docs/                    # Specifications (plan.md, phase-1 through phase-5-spec.md)
└── examples/                # YAML examples (minimal)
```

### Critical Observations

1. **`internal/storage/postgres_repository.go`** exists but is NOT wired to `gateway.go` — the gateway uses `InMemoryTableRegistry` even with `NewGatewayWithRepository()`
2. **`internal/cli/table.go` and `query.go`** contain LOCAL logic with `// TODO` comments — they do not use `GatewayClient`
3. **Trino and Spark adapters** are implemented but never instantiated or registered anywhere
4. **`migrations/` directory** exists but there's no migration runner in the codebase

---

## 3. Control Plane Flow (End-to-End)

### Query Lifecycle

```
CLI/API Request
    │
    ▼
┌──────────────────┐
│ 1. HTTP Handler  │ ← gateway.handleQuery()
│    (auth check)  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 2. SQL Parser    │ ← sql.Parser.Parse()
│    (vitess)      │ ← detectUnsupportedSyntax(), detectVendorHints()
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 3. Table Name    │ ← sql.ValidateTableName()
│    Validation    │ ← Enforces schema.table format
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 4. Authorization │ ← auth.AuthorizationService.Authorize()
│    (pre-plan)    │ ← Deny-by-default, checks ALL tables
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 5. Planner       │ ← planner.Planner.Plan()
│                  │ ← Resolves tables from registry
│                  │ ← Checks SNAPSHOT_CONSISTENT
│                  │ ← Validates capabilities
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 6. Engine        │ ← router.Router.SelectEngine()
│    Selection     │ ← Priority-based, capability-matched
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 7. Adapter       │ ← adapters.AdapterRegistry.Get()
│    Execution     │ ← adapter.Execute(plan)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 8. Response      │ ← Structured JSON with query_id
│    + Logging     │ ← observability.LogQuery()
└──────────────────┘
```

### Per-Step Analysis

| Step | Code Location | Assumptions | Failure Mode | Handling |
|------|---------------|-------------|--------------|----------|
| 1. HTTP Handler | `gateway.handleQuery()` | Token in `Authorization` header | Invalid/missing token | 401 Unauthorized |
| 2. SQL Parser | `sql.Parse()` | vitess can parse the SQL | Syntax error, unsupported construct | 400 with classified error |
| 3. Table Validation | `sql.ValidateTableName()` | All table refs extracted | Unqualified name | 400 with format explanation |
| 4. Authorization | `authz.Authorize()` | User in context, tables resolved | Permission denied | 403 with table/capability info |
| 5. Planner | `planner.Plan()` | Tables exist in registry | Table not found, capability missing | 400/403 with explicit error |
| 6. Engine Selection | `router.SelectEngine()` | At least one engine available | No compatible engine | 500 with capability list |
| 7. Adapter Execution | `adapter.Execute()` | Adapter connected, query valid | Query execution failure | 500 with engine error |
| 8. Response | `writeJSON()` | Result serializable | Encoding failure | Internal error |

### Critical Gap: Step 5 uses InMemoryTableRegistry

The planner queries `gateway.tableRegistry` which is always an `InMemoryTableRegistry`. Even when `NewGatewayWithRepository()` is used, the repository is only stored for health checks — table lookups bypass PostgreSQL.

**Impact**: Tables registered via bootstrap/config are not visible to the planner. This breaks the Phase 3 specification requiring PostgreSQL as the single metadata authority.

---

## 4. Table Abstraction Layer Analysis

### Virtual Table Representation

```go
type VirtualTable struct {
    Name         string
    Description  string
    Sources      []PhysicalSource
    Capabilities []capabilities.Capability
    Constraints  []capabilities.Constraint
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```

### Capability Enforcement

| Capability | Checked At | Enforcer | Test Coverage |
|------------|------------|----------|---------------|
| READ | `VirtualTable.CanPerform()` | Planner | ✅ Red-Flag + Green-Flag |
| TIME_TRAVEL | `Planner.determineRequiredCapabilities()` | Planner | ✅ Red-Flag + Green-Flag |

### Constraint Enforcement

| Constraint | Checked At | Enforcer | Test Coverage |
|------------|------------|----------|---------------|
| READ_ONLY | `VirtualTable.CanPerform()` | Virtual table | ✅ Red-Flag |
| SNAPSHOT_CONSISTENT | `Planner.checkSnapshotConsistency()` | Planner | ✅ Red-Flag + Green-Flag |

### Invariants Actually Enforced Today

1. ✅ READ capability required for SELECT
2. ✅ TIME_TRAVEL capability required for AS OF queries
3. ✅ READ_ONLY constraint blocks all writes
4. ✅ SNAPSHOT_CONSISTENT requires AS OF clause
5. ✅ SNAPSHOT_CONSISTENT + non-SNAPSHOT_CONSISTENT tables cannot be mixed
6. ✅ Write operations (INSERT/UPDATE/DELETE) blocked at parser level

### Invariants Existing Only in Docs

1. ❌ **Metadata from PostgreSQL only** — InMemoryTableRegistry is used
2. ❌ **CLI reflects gateway behavior** — CLI has local logic paths
3. ❌ **Trino as primary read engine** — Only DuckDB is wired

### Unsafe Behavior Currently Possible

1. **Table registration without persistence** — Gateway accepts tables into in-memory registry that disappear on restart
2. **CLI can parse queries without gateway** — `query.go` runs local parsing, could diverge
3. **Empty adapter registry passes readiness** — If no adapters registered, engine selection fails at query time, not startup
4. **No migration runner** — PostgreSQL schema exists but no code applies it

---

## 5. Planner & Router Analysis

### Engine Selection Logic

```go
func (r *Router) SelectEngine(ctx context.Context, required []capabilities.Capability) (string, error) {
    // 1. Iterate all engines
    // 2. Skip unavailable engines
    // 3. Skip engines missing required capabilities
    // 4. Select engine with lowest Priority value
}
```

### Determinism Analysis

| Question | Answer | Evidence |
|----------|--------|----------|
| Is routing deterministic? | **YES** | Priority-based, map iteration order doesn't affect selection |
| Can two identical queries route differently? | **NO** | Same tables → same capabilities → same engine |
| What happens if no engine matches? | `ErrEngineUnavailable` | Returns required capability list |

### DefaultRouter Configuration

```go
DuckDB:  Priority 1, Available: true
Trino:   Priority 2, Available: false
Spark:   Priority 3, Available: false
```

**Consequence**: All queries route to DuckDB regardless of capability requirements.

### Silent Decision Risk

The planner makes one "silent" decision: **authorization requirement determination**.

```go
requiredCap := capabilities.CapabilityRead
if logical.HasTimeTravel {
    requiredCap = capabilities.CapabilityTimeTravel
}
```

This means authorization is checked against READ for normal queries but TIME_TRAVEL for AS OF queries. This is correct but not explicitly logged in the authorization decision field.

---

## 6. Engine Adapters

### DuckDB Adapter

| Aspect | Status |
|--------|--------|
| Responsibilities | Execute SQL via database/sql, return results |
| Logic present | Connection management, row scanning, context cancellation |
| Logic absent | SQL translation (passes through raw SQL), query rewriting |
| Semantic leakage | DuckDB-specific errors propagate to client (e.g., "Catalog Error") |

### Trino Adapter

| Aspect | Status |
|--------|--------|
| Responsibilities | Same as DuckDB |
| Logic present | DSN construction, connection management |
| Logic absent | Catalog/schema rewriting, time-travel translation |
| **Integration status** | **NOT WIRED** — Never instantiated in gateway |

### Spark Adapter

| Aspect | Status |
|--------|--------|
| Responsibilities | Same as DuckDB |
| Logic present | TCP connectivity check, HiveServer2 DSN |
| Logic absent | Spark-specific query translation |
| **Integration status** | **NOT WIRED** — Never instantiated in gateway |

### Adapter Architecture Critique

Adapters are "thin" as designed, but this creates semantic leakage:

1. **Raw SQL passthrough** — DuckDB receives the exact SQL from the user. If the user writes DuckDB-specific syntax that happens to work, canonica doesn't catch it.
2. **Error messages expose engine** — "DuckDB adapter: failed to scan row" reveals implementation details.
3. **Time-travel not translated** — `AS OF` syntax varies by engine; no normalization occurs.

---

## 7. Red-Flag / Green-Flag Test Coverage

### Red-Flag Tests Implemented (120+)

| Category | Count | Critical Tests |
|----------|-------|----------------|
| Authorization | 6 | NoRolesCannotQuery, EnforcedBeforePlanning |
| Authentication | 4 | EmptyToken, InvalidToken, ExpiredToken |
| Bootstrap | 7 | RejectsMissingSections, RejectsUnknownKeys |
| Capability | 6 | MissingReadCapability, TimeTravelWithoutCapability |
| CLI-Gateway | 7 | FailsWithoutGateway, MustNotBypassGateway |
| Engine Adapters | 27 | RejectsNilPlan, RejectsContextCancellation |
| Gateway | 7 | RejectsUnauthenticated, RejectsWriteOperations |
| Parser | 9 | RejectsNestedSelectBypass, ExtractsJoinedTables |
| Planner | 4 | NoAvailableEngine, MissingCapability |
| Readiness | 5 | FailsWhenDatabaseDown, RefusesQueriesWhenNotReady |
| Snapshot Consistent | 4 | RejectsQueryWithoutAsOf, RejectsMixedCapabilities |
| Storage | 8 | RejectsDuplicateTable, RejectsContextCancellation |
| Table Naming | 6 | RejectsUnqualifiedQueryName, ErrorMessageExplainsFormat |
| Unsupported Syntax | 7 | RejectsWindowFunctions, RejectsCTEs |

### Green-Flag Tests Implemented (90+)

| Category | Count | Notable Tests |
|----------|-------|---------------|
| Authorization | 5 | AuthorizedRoleCanQuery, MultiTableAllAuthorized |
| Bootstrap | 7 | ValidConfigurationLoads, ConfigurationRoundTrips |
| Capability | 4 | ReadAllowed, ParseValidCapabilities |
| CLI-Gateway | 8 | ReflectsGatewayMetadata, ErrorsPropagateUnchanged |
| Engine Adapters | 28 | ExecuteSimpleSelect, CloseIsIdempotent |
| Gateway | 11 | AcceptsValidToken, ExecuteValidQuery |
| Observability | 10 | LogValidQuery, TimestampIncluded |
| Parser | 12 | HandlesSimpleSELECT, HandlesSubquery |
| Readiness | 3 | HealthzReturnsOK, ReadyzReportsAllComponents |
| Snapshot Consistent | 3 | AcceptsQueryWithAsOf, AcceptsMultipleSnapshotTables |
| Storage | 10 | CreateTable, TableTimestamps |

### Critical Missing Red-Flag Tests

1. **❌ CLI executes query locally when gateway unavailable** — Test asserts failure, but CLI code contains local execution path
2. **❌ Gateway accepts table registration to in-memory when ProductionMode=true** — Test exists but implementation is incomplete
3. **❌ Time-travel syntax accepted but engine doesn't support it** — Trino/Spark not wired to verify
4. **❌ Concurrent table registration creates race condition** — No concurrency tests for InMemoryTableRegistry
5. **❌ Bootstrap apply writes to PostgreSQL** — Tests use mock repository

### Tests That Appear Mislabeled

| Test | Location | Issue |
|------|----------|-------|
| `TestParser_ExtractsCTETables` | redflag | Marked `t.Skip()` — CTE extraction doesn't work, test should pass when fixed |
| `TestParser_DetectsTimeTravel` | greenflag | Marked `t.Skip()` — Uses unsupported SQL:2011 syntax |

---

## 8. Error Semantics & UX

### Error Structure

All errors extend `CanonicError`:

```go
type CanonicError struct {
    Code       ErrorCode  // Validation=1, Auth=2, Engine=3, Internal=4
    Message    string     // Short description
    Reason     string     // Technical cause
    Suggestion string     // User action
    Cause      error      // Wrapped error
}
```

### Human-Readability Assessment

| Error Type | Message Quality | Reason Quality | Suggestion Quality |
|------------|-----------------|----------------|-------------------|
| ErrCapabilityDenied | ✅ Clear | ✅ Names capability | ✅ Suggests command |
| ErrConstraintViolation | ✅ Clear | ✅ Names constraint | ✅ Suggests command |
| ErrTableNotFound | ✅ Clear | ✅ Explains | ✅ Suggests `table list` |
| ErrEngineUnavailable | ⚠️ Generic | ✅ Lists requirements | ⚠️ Vague |
| ErrAccessDenied | ✅ Clear | ✅ Names table/capability | ⚠️ "contact administrator" |
| ErrUnsupportedSyntax | ✅ Names construct | ✅ Explains | ✅ Suggests alternative |

### Cause Preservation

Errors are wrapped with `fmt.Errorf()` and preserve the original error via `%w`. The `Unwrap()` method allows error inspection.

### Swallowed Errors

1. **Adapter initialization failures** — If `sql.Open()` fails in adapter constructors, the adapter is returned in "closed" state. The error is lost.
2. **Logger write failures** — `LogQuery()` returns an error, but callers (handleQuery) don't check it.

---

## 9. AI / Copilot Usage Signals

### Areas That Appear AI-Generated

1. **Repetitive adapter implementations** — DuckDB, Trino, Spark adapters follow identical structure (appropriate for Copilot)
2. **Test scaffolding** — Table-driven tests with similar patterns (appropriate)
3. **Error type definitions** — Repetitive struct+constructor pairs (appropriate)
4. **CLI command wiring** — Cobra boilerplate (appropriate)

### Potential Copilot Rule Violations

| Location | Concern | Risk |
|----------|---------|------|
| `planner.checkSnapshotConsistency()` | Core constraint enforcement logic | **Should be human-reviewed** |
| `authorization.Authorize()` | Security-critical permission check | **Should be human-reviewed** |
| `router.SelectEngine()` | Routing decision logic | Medium — simple enough to verify |
| `parser.detectUnsupportedSyntax()` | Pattern matching for SQL constructs | Low — defensive code |

### Code Style Signals

The codebase shows consistent style suggesting single-author or Copilot-assisted development:
- Identical comment headers on all packages
- Consistent error wrapping patterns
- Uniform test structure

This is acceptable but raises the question: **Were the core invariant-enforcing functions reviewed by a human?**

---

## 10. Deviations from plan.md

### Features Implemented But Not In plan.md

| Feature | Location | Risk | Recommendation |
|---------|----------|------|----------------|
| Phase 5 Bootstrap System | `internal/bootstrap/` | Low | Keep — addresses adoption |
| EXPLAIN CANONIC | `gateway.ExplainCanonic()` | Low | Keep — improves debuggability |
| Audit Summary | `observability.GetAuditSummary()` | Low | Keep — operational visibility |
| Unsupported Syntax Detection | `parser.detectUnsupportedSyntax()` | Low | Keep — improves UX |
| Vendor Hint Rejection | `parser.detectVendorHints()` | Low | Keep — safety feature |

### Features Promised In plan.md But Missing

| Feature | plan.md Section | Status | Risk | Recommendation |
|---------|-----------------|--------|------|----------------|
| Trino as primary engine | §4.2 | **MISSING** | **HIGH** | Fix — DuckDB-only is not production-ready |
| PostgreSQL metadata authority | §4.3 | **PARTIAL** | **HIGH** | Fix — Repository not wired to planner |
| JDBC interface | §4.4 | Missing | Medium | Defer or remove from docs |
| Multiple physical sources per table | §6.2 | Implemented | Low | Keep |
| Audit logs in PostgreSQL | §4.3 | Missing | Medium | Add — currently in-memory only |

### Scope Creep

1. **Phase specifications** (phase-1-spec.md through phase-5-spec.md) added significantly more detail than original plan.md
2. **Bootstrap/config system** is more elaborate than "adoptable without migration" suggests
3. **Status and audit commands** add operational tooling beyond MVP scope

### Accidental Complexity

1. **Two table registries** — `InMemoryTableRegistry` and `PostgresRepository` exist but aren't unified
2. **Two config systems** — `internal/config/` and `internal/bootstrap/config.go` serve similar purposes
3. **Gateway constructors** — Five different `NewGateway*()` functions with overlapping responsibilities

---

## 11. Technical Debt & Risk Register

| ID | Issue | Location | Impact | Likelihood | Mitigation |
|----|-------|----------|--------|------------|------------|
| D01 | PostgresRepository not wired to gateway | `gateway.go` | **Critical** — Tables not persistent | Certain | Wire repository through planner |
| D02 | CLI commands have local logic | `cli/table.go`, `cli/query.go` | **High** — CLI can diverge from gateway | High | Remove local logic, use GatewayClient |
| D03 | Trino/Spark adapters not integrated | `adapters/trino/`, `adapters/spark/` | **High** — Only DuckDB works | Certain | Wire to AdapterRegistry |
| D04 | Audit logs not persisted | `observability/logger.go` | Medium — Audit lost on restart | Certain | Add PostgreSQL audit table |
| D05 | No migration runner | `migrations/` | Medium — Schema not applied | High | Add migrate tool integration |
| D06 | Adapter init errors swallowed | Adapter constructors | Medium — Silent failures | Medium | Return error from constructors |
| D07 | Time-travel syntax not normalized | `sql/parser.go` | Medium — Engine-specific AS OF | Medium | Add translation layer |
| D08 | Logger write errors ignored | `gateway.handleQuery()` | Low — Logging failures silent | Low | Check error return |
| D09 | JWT not implemented | `auth/auth.go` | Medium — Static tokens in prod | Deferred | Implement T001 |
| D10 | Empty adapter registry accepted | `router/` | Medium — Fails at query time | Low | Check at startup |
| D11 | CTE support missing | `sql/parser.go` | Medium — Common SQL pattern | Medium | Upgrade vitess/sqlparser |
| D12 | InMemoryTableRegistry not thread-safe | `gateway/gateway.go` | Medium — Race conditions | Low | Add mutex or use repository |
| D13 | Config versions not tracked | `bootstrap/config.go` | Low — No rollback | Medium | Add version metadata |
| D14 | No rate limiting | `gateway/gateway.go` | Low — DoS possible | Low | Add middleware |

---

## 12. Questions That Must Be Answered

### Architecture Questions

1. **Why does `NewGatewayWithRepository()` accept a repository but still use `InMemoryTableRegistry` for table lookups?** The repository is only used for health checks.

2. **When will Trino and Spark adapters be wired?** The code exists but is never instantiated. Is this intentional deferral or oversight?

3. **Why are there two configuration systems (`internal/config/` and `internal/bootstrap/`)?** They appear to serve similar purposes.

4. **What is the expected flow for table registration?** Bootstrap config → PostgreSQL → Gateway registry is not connected.

### Implementation Questions

5. **Is the `InMemoryTableRegistry` intended for production use?** It's used even when `ProductionMode=true`.

6. **Why do CLI commands (`table.go`, `query.go`) contain local parsing logic?** The `GatewayClient` exists but isn't used.

7. **How are migration files applied?** No migration runner exists in the codebase.

8. **Why does `DefaultRouter()` mark Trino and Spark as `Available: false`?** If they're not available, why include them?

### Safety Questions

9. **What prevents a query with DuckDB-specific syntax from executing successfully?** Raw SQL is passed through without validation against capabilities.

10. **What prevents time-travel queries on engines that don't support the AS OF syntax?** The parser detects AS OF but doesn't translate it for different engines.

11. **Who reviewed `planner.checkSnapshotConsistency()` and `authorization.Authorize()`?** These are security-critical functions per copilot-instructions.md.

### Operational Questions

12. **How is PostgreSQL schema applied to a new deployment?** Migrations exist but no runner.

13. **How are audit logs retained across gateway restarts?** Currently in-memory only.

14. **What happens when all DuckDB connections are exhausted?** No connection pooling limits visible.

### Testing Questions

15. **Why are `TestParser_ExtractsCTETables` and `TestParser_DetectsTimeTravel` skipped?** They're in the test files but marked as `t.Skip()`.

16. **Are there integration tests with a real PostgreSQL instance?** All tests use mocks.

17. **Are there end-to-end tests from CLI → Gateway → Adapter → Database?** I could not find any.

---

## Summary

canonica-labs has a solid design foundation with well-structured code, comprehensive error handling, and extensive test coverage. However, critical integration work remains incomplete:

1. **PostgreSQL is not the metadata authority** — violates Phase 3 specification
2. **Trino/Spark adapters are not wired** — system is DuckDB-only
3. **CLI has local logic paths** — can diverge from gateway behavior
4. **Audit logs are not persisted** — lost on restart

These issues must be addressed before the system can be considered production-ready. The technical debt items D01, D02, and D03 are blocking issues with **HIGH** or **CRITICAL** impact.

---

*End of Report*
