# Engineering Audit Report – canonic-labs

**Date**: 2026-01-10  
**Auditor**: AI Engineering Auditor (Opus 4.5)  
**Status**: Forensic Design-Level Analysis

---

## 1. Project Snapshot

### What This System Currently Does

Canonica is a **control plane for lakehouse query routing**. It accepts SQL queries via HTTP API, validates them against registered virtual tables, checks capabilities and constraints, selects an execution engine, and forwards queries to that engine.

**Currently functional:**
- HTTP gateway accepting queries at `/query`, `/query/explain`, `/query/validate`
- SQL parsing via vitess/sqlparser with table extraction, operation type detection
- Virtual table abstraction with capabilities (READ, TIME_TRAVEL) and constraints (READ_ONLY, SNAPSHOT_CONSISTENT)
- In-memory table registry for testing
- PostgreSQL repository implementation (schema exists, CRUD complete)
- DuckDB adapter (fully implemented)
- Trino adapter (implemented, requires live Trino cluster)
- Spark adapter (implemented, requires live Spark Thrift Server)
- Rule-based engine routing by capability match and priority
- Role → Table → Capability authorization (deny-by-default)
- Schema-qualified table name enforcement
- SNAPSHOT_CONSISTENT constraint enforcement
- Explicit rejection of WINDOW functions, CTEs, vendor hints
- CLI with gateway client proxy (`GatewayClient`)
- Structured query logging (JSON)

### What This System Explicitly Does NOT Do

Per plan.md, the system explicitly refuses to:
- Execute queries directly (delegates to engines)
- Read or write data files (no Parquet readers, no commit log writers)
- Perform cost-based optimization
- Support writes (INSERT, UPDATE, DELETE blocked)
- Support cross-engine joins
- Support streaming queries

### Implemented vs Stubbed vs Planned

| Component | Status | Notes |
|-----------|--------|-------|
| Gateway HTTP API | **Complete** | All endpoints functional |
| SQL Parser | **Complete** | vitess/sqlparser, no CTE support |
| Virtual Table Model | **Complete** | Capabilities, constraints, sources |
| In-Memory Registry | **Complete** | For testing only |
| PostgreSQL Repository | **Partial** | Schema exists, CRUD implemented, NOT wired to gateway |
| Planner | **Complete** | Rule-based, capability matching |
| Router | **Complete** | Priority-based engine selection |
| DuckDB Adapter | **Complete** | Primary MVP engine |
| Trino Adapter | **Complete** | Not tested with live cluster |
| Spark Adapter | **Complete** | Not tested with live cluster |
| Authorization | **Complete** | Deny-by-default, role→table→capability |
| CLI | **Partial** | Gateway client exists, but CLI commands not wired |
| Observability | **Partial** | Logging implemented, metrics missing |
| JWT Auth | **Stub** | Static token only (T001 deferred) |

---

## 2. Repository Map

```
canonica-labs/
├── cmd/
│   ├── canonic/main.go          [CLI entrypoint - 25 lines]
│   └── gateway/main.go          [Gateway server - 125 lines]
├── internal/
│   ├── adapters/
│   │   ├── adapter.go           [Interface + registry - Complete]
│   │   ├── duckdb/adapter.go    [DuckDB implementation - Complete]
│   │   ├── trino/adapter.go     [Trino implementation - Complete]
│   │   └── spark/adapter.go     [Spark implementation - Complete]
│   ├── auth/
│   │   ├── auth.go              [Static token auth - Complete]
│   │   └── authorization.go     [Role→Table authz - Complete]
│   ├── capabilities/
│   │   └── capability.go        [Capability/Constraint types - Complete]
│   ├── cli/
│   │   ├── cli.go               [Cobra CLI root - Complete]
│   │   ├── gateway_client.go    [HTTP client to gateway - Complete]
│   │   ├── table.go             [Table commands - NOT wired to gateway]
│   │   ├── query.go             [Query commands - NOT wired to gateway]
│   │   └── ...                  [Other commands]
│   ├── config/                  [Configuration loading]
│   ├── errors/
│   │   └── errors.go            [Typed errors - Complete, ~400 lines]
│   ├── gateway/
│   │   ├── gateway.go           [HTTP handlers - Complete, 775 lines]
│   │   ├── mock_adapter.go      [Test adapter]
│   │   └── testing.go           [Test utilities]
│   ├── observability/
│   │   └── logger.go            [Query logging - Complete]
│   ├── planner/
│   │   └── planner.go           [Query planning - Complete, 200 lines]
│   ├── router/
│   │   └── router.go            [Engine selection - Complete, 179 lines]
│   ├── sql/
│   │   └── parser.go            [SQL parsing - Complete, 515 lines]
│   ├── storage/
│   │   ├── repository.go        [Interface - Complete]
│   │   ├── mock_repository.go   [In-memory - Complete]
│   │   └── postgres_repository.go [PostgreSQL - Complete but NOT WIRED]
│   └── tables/
│       └── virtual_table.go     [VirtualTable model - Complete]
├── pkg/
│   ├── api/                     [Empty or unused]
│   └── models/                  [Empty or unused]
├── tests/
│   ├── redflag/                 [19 test files]
│   └── greenflag/               [16 test files]
├── migrations/
│   └── 000001_create_virtual_tables.up.sql [Schema - Complete]
├── examples/                    [2 YAML examples]
└── docs/
    ├── plan.md                  [Authoritative spec]
    ├── phase-3-spec.md          [Phase 3 requirements]
    ├── test.md                  [TDD methodology]
    ├── tracker.md               [Technical debt log]
    └── canonic-cli-spec.md      [CLI specification]
```

### Critical Observations

1. **`pkg/` directory is empty or unused** - Listed in plan.md but contains no meaningful code
2. **PostgreSQL repository exists but is NOT wired** - `NewGatewayWithDB()` creates in-memory registry as fallback
3. **CLI commands exist but don't use `GatewayClient`** - `table.go`, `query.go` have local implementations
4. **No `/cmd/planner/` directory** - plan.md mentions separate planner service, doesn't exist

---

## 3. Control Plane Flow (End-to-End)

### 1. Entry Point

**Code**: `cmd/gateway/main.go`, `internal/gateway/gateway.go:handleQuery()`

- HTTP POST to `/query` with JSON `{"sql": "..."}`
- Bearer token extracted from `Authorization` header
- Token validated via `StaticTokenAuthenticator`

**Assumptions**:
- All queries come through HTTP (no direct library usage)
- Token is pre-shared (no OAuth/JWT flow)

**Failure Handling**:
- Missing token → 401 with "authentication required"
- Invalid token → 401 with "authentication failed"

### 2. Parsing

**Code**: `internal/sql/parser.go:Parse()`

**Flow**:
1. Empty query check
2. Multiple statements check (sqlparser.SplitStatementToPieces)
3. Pre-parse unsupported syntax detection (CTEs, WINDOW, vendor hints)
4. AST parsing via vitess/sqlparser
5. Operation type extraction (SELECT, INSERT, etc.)
6. Table name extraction from FROM, JOIN, subqueries
7. Time travel detection (text search for "AS OF")

**Assumptions**:
- Only SELECT is allowed (all writes return `ErrWriteNotAllowed`)
- CTEs cannot be parsed (rejected with specific error)
- WINDOW functions detected via pattern matching, not AST

**Failure Handling**:
- Unsupported syntax → `ErrUnsupportedSyntax` with construct name
- Parse error → `ErrQueryRejected` with reason
- Writes → `ErrWriteNotAllowed`

**Gaps**:
- Time travel detection uses text search, not AST (T014 deferred)
- CTE support missing (T013 deferred)

### 3. Table Name Validation

**Code**: `internal/sql/parser.go:ValidateTableName()`, `internal/gateway/gateway.go:handleQuery()`

**Flow**:
1. Each table name checked for schema.table format
2. Unqualified names rejected with explicit error

**Assumptions**:
- All tables must be schema-qualified
- No default schema fallback

**Failure Handling**:
- Unqualified name → 400 with "use fully-qualified name: <schema>.<table>"

### 4. Authorization

**Code**: `internal/auth/authorization.go:Authorize()`, `internal/gateway/gateway.go:handleQuery()`

**Flow**:
1. Get user from context
2. Determine required capability (READ or TIME_TRAVEL)
3. Check user's roles have permission on ALL tables in query
4. Deny-by-default: no permission = denied

**Assumptions**:
- Authorization happens BEFORE table resolution (correct)
- All tables must be authorized (no partial access)

**Failure Handling**:
- No roles → 403 "access denied"
- Missing table permission → 403 with table name in error
- Missing capability → 403 with capability in error

**CRITICAL GAP**: Authorization service is created in `NewGateway()` but with NO permissions by default. Production code must configure permissions.

### 5. Table Resolution

**Code**: `internal/planner/planner.go:Plan()`

**Flow**:
1. For each table name in query, call `tableRegistry.GetTable()`
2. Registry returns `*VirtualTable` with capabilities and constraints

**Assumptions**:
- Tables must be pre-registered
- In-memory registry is used (PostgreSQL not wired)

**Failure Handling**:
- Table not found → `ErrTableNotFound`

### 6. Capability and Constraint Checks

**Code**: `internal/planner/planner.go:checkSnapshotConsistency()`, `checkTableCapabilities()`

**Flow**:
1. Check SNAPSHOT_CONSISTENT constraint:
   - If present, query MUST have AS OF clause
   - Cannot mix SNAPSHOT_CONSISTENT with non-SNAPSHOT_CONSISTENT tables
2. Check table has required capabilities (READ, TIME_TRAVEL)
3. Check constraints don't block operation (READ_ONLY blocks writes)

**Assumptions**:
- Constraint overrides capability (READ_ONLY wins)
- SNAPSHOT_CONSISTENT enforcement is per-query, not per-table

**Failure Handling**:
- Missing capability → `ErrCapabilityDenied`
- Constraint violation → `ErrConstraintViolation`

### 7. Engine Selection

**Code**: `internal/router/router.go:SelectEngine()`

**Flow**:
1. Get required capabilities from query
2. Find all engines with those capabilities
3. Select engine with lowest priority number (highest priority)

**Assumptions**:
- Engine availability is pre-configured (not runtime-checked)
- Selection is deterministic (lowest priority wins)

**Failure Handling**:
- No matching engine → `ErrEngineUnavailable`

### 8. Adapter Execution

**Code**: `internal/adapters/duckdb/adapter.go:Execute()`

**Flow**:
1. Validate plan and SQL are non-nil
2. Execute raw SQL against engine
3. Collect columns and rows
4. Return `QueryResult`

**Assumptions**:
- Raw SQL is passed directly (no translation)
- Engine connection is pre-established

**Failure Handling**:
- Connection closed → error with "connection is closed"
- Query execution failed → error with engine context

**GAP**: SQL is passed directly without translation. Engine-specific syntax differences are NOT handled.

### 9. Result Handling

**Code**: `internal/gateway/gateway.go:handleQuery()`

**Flow**:
1. Convert `QueryResult` rows to JSON maps
2. Return `QueryResponse` with query_id, columns, rows, engine, duration

**Logging**:
- All queries logged via `QueryLogger` (query_id, user, tables, engine, duration, error)

---

## 4. Table Abstraction Layer Analysis (CRITICAL)

### Virtual Table Representation

**Code**: `internal/tables/virtual_table.go`

```go
type VirtualTable struct {
    Name         string
    Description  string
    Sources      []PhysicalSource
    Capabilities []Capability  // READ, TIME_TRAVEL
    Constraints  []Constraint  // READ_ONLY, SNAPSHOT_CONSISTENT
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```

### Where Capabilities Are Enforced

| Location | Enforcement |
|----------|-------------|
| `planner.go:checkTableCapabilities()` | Checks table has required capability |
| `virtual_table.go:CanPerform()` | Checks operation against capabilities |
| `router.go:SelectEngine()` | Checks engine has required capabilities |

### Where Constraints Are Enforced

| Location | Enforcement |
|----------|-------------|
| `planner.go:checkSnapshotConsistency()` | SNAPSHOT_CONSISTENT requires AS OF |
| `virtual_table.go:CanPerform()` | READ_ONLY blocks writes |

### How Conflicts Are Handled

- **Constraint vs Capability**: Constraint wins (READ_ONLY blocks even if WRITE capability)
- **Multiple sources same format**: Rejected at validation (`Validate()` in virtual_table.go)
- **Conflicting metadata sources**: `DetectMetadataConflict()` compares primary/secondary repos

### Invariants Actually Enforced Today

1. ✅ Writes blocked (MVP is read-only)
2. ✅ READ capability required for SELECT
3. ✅ TIME_TRAVEL capability required for AS OF queries
4. ✅ SNAPSHOT_CONSISTENT requires AS OF clause
5. ✅ Cannot mix SNAPSHOT_CONSISTENT with non-SNAPSHOT_CONSISTENT tables
6. ✅ Table names must be schema-qualified
7. ✅ Authorization is deny-by-default
8. ✅ Authorization checked BEFORE table resolution

### Invariants Existing Only in Docs

1. ⚠️ "PostgreSQL is the authoritative registry" - Gateway still uses in-memory registry
2. ⚠️ "CLI connects to gateway" - `GatewayClient` exists but CLI commands not wired
3. ⚠️ Per-table AS OF timestamps - Not detected (T015 deferred)

### Unsafe Behavior Currently Possible

1. **Gateway starts without PostgreSQL** - `cmd/gateway/main.go` creates in-memory registry
2. **CLI operates locally** - CLI table commands don't use `GatewayClient`
3. **No permission configuration** - Default `AuthorizationService` has no grants
4. **Engine not checked at runtime** - `Available` flag not validated against live engine

---

## 5. Planner & Router Analysis

### Engine Selection

**Code**: `internal/router/router.go`

**Rules**:
1. Filter engines by availability (`Available == true`)
2. Filter by capability match (`HasAllCapabilities`)
3. Select lowest priority number

**Default Configuration** (`DefaultRouter()`):
- DuckDB: priority 1, capabilities [READ, TIME_TRAVEL]
- Trino: priority 2, capabilities [READ] (registered but not created)
- Spark: priority 3, capabilities [READ, TIME_TRAVEL] (registered but not created)

### Is Routing Deterministic?

**YES** - Selection is based on:
1. Capability match (boolean)
2. Priority (integer comparison)
3. Same capabilities + same priority = same result

**BUT**: Map iteration in Go is non-deterministic. If two engines have same priority and same capabilities, selection order could vary.

### Fallback Behavior

**Explicit**: No hidden fallbacks. If no engine matches, `ErrEngineUnavailable` is returned.

**Risk**: `DefaultRouter()` only registers DuckDB in `cmd/gateway/main.go`. Trino/Spark adapters exist but aren't registered.

### Can Planner Make Silent Decisions?

**NO** - Every decision is:
1. Capability check → explicit pass/fail
2. Constraint check → explicit pass/fail
3. Engine selection → explicit match or error

### Can Two Identical Queries Route Differently?

**Theoretically YES** if:
- Same capabilities, same priority, different engine order in map
- Engine availability changes between queries

**Practically NO** for MVP with single DuckDB engine.

### What Happens If No Engine Matches?

Returns `ErrEngineUnavailable` with list of required capabilities. Query is NOT executed.

---

## 6. Engine Adapters

### Common Interface

```go
type EngineAdapter interface {
    Name() string
    Capabilities() []capabilities.Capability
    Execute(ctx context.Context, plan *ExecutionPlan) (*QueryResult, error)
    Ping(ctx context.Context) error
    Close() error
}
```

### DuckDB Adapter

**Responsibilities**:
- Accept execution plan
- Execute raw SQL via `database/sql`
- Return columns and rows

**Logic Present**:
- Context cancellation handling
- Input validation (nil checks)
- Row iteration with error checking

**Logic Absent**:
- No SQL translation
- No result type conversion
- No connection pooling configuration

### Trino Adapter

**Responsibilities**: Same as DuckDB

**Logic Present**:
- DSN construction from config
- SSL mode support
- Catalog/schema configuration

**Logic Absent**:
- No query translation
- No retry logic (per plan.md)
- No connection pooling tuning

### Spark Adapter

**Responsibilities**: Same as DuckDB

**Logic Present**:
- Deferred connection (adapter created without live connection)
- TCP connectivity check

**Logic Absent**:
- Actual driver implementation (placeholder connect())
- No Kerberos support

### Are Adapters Thin Translators?

**YES and NO**:
- ✅ No business logic
- ✅ No retries
- ❌ No SQL translation at all (raw SQL passed through)

### Can Adapter Behavior Leak Engine-Specific Quirks?

**YES** - Since SQL is passed raw:
- DuckDB-specific syntax would fail on Trino
- Trino-specific syntax would fail on DuckDB
- No abstraction layer for SQL dialect differences

**This is a design decision**: Canonica routes but does not translate.

---

## 7. Red-Flag / Green-Flag Test Coverage

### Red-Flag Tests Implemented

| File | Tests |
|------|-------|
| `auth_test.go` | Token validation, expiration |
| `authorization_test.go` | Deny-by-default, missing permissions, multi-table |
| `capability_test.go` | Missing READ, missing TIME_TRAVEL, invalid capabilities |
| `cli_gateway_test.go` | CLI without gateway, no local fallbacks |
| `duckdb_test.go` | Connection errors, empty queries |
| `gateway_test.go` | Method not allowed, missing token |
| `metadata_authority_test.go` | Gateway without DB, conflicting sources |
| `observability_test.go` | Missing query_id, missing user |
| `parser_ast_test.go` | Invalid SQL, DDL blocked |
| `planner_test.go` | Missing table, missing capability |
| `query_test.go` | Blocked writes, parse failures |
| `snapshot_consistent_test.go` | Missing AS OF, mixed tables |
| `spark_test.go` | Connection errors |
| `storage_test.go` | Duplicate tables, missing tables |
| `table_naming_test.go` | Unqualified names |
| `table_test.go` | Invalid sources, invalid capabilities |
| `trino_test.go` | Connection errors |
| `unsupported_syntax_test.go` | WINDOW, CTEs, vendor hints, multiple statements |

### Green-Flag Tests Implemented

| File | Tests |
|------|-------|
| `authorization_test.go` | Granted permissions work |
| `capability_test.go` | Valid capabilities accepted |
| `cli_gateway_test.go` | Gateway client operations |
| `duckdb_test.go` | Query execution |
| `gateway_test.go` | Health check, valid queries |
| `metadata_authority_test.go` | Table visibility, persistence |
| `observability_test.go` | Valid log entries |
| `parser_ast_test.go` | Table extraction, JOIN handling |
| `snapshot_consistent_test.go` | AS OF queries succeed |
| `spark_test.go` | Adapter capabilities |
| `storage_test.go` | CRUD operations |
| `supported_syntax_test.go` | Valid SELECT parsing |
| `table_naming_test.go` | Qualified names accepted |
| `table_test.go` | Valid table definitions |
| `trino_test.go` | Adapter capabilities |

### Critical Missing Red-Flag Tests

1. **Gateway with PostgreSQL down mid-operation** - Tests gateway startup without DB, but not DB failure during query
2. **Engine becomes unavailable mid-query** - No test for adapter failure after engine selection
3. **Authorization permissions changed during query** - Race condition testing
4. **Concurrent table registration conflicts** - Thread safety under load
5. **SQL injection via table names** - Unqualified name with injection payload

### Tests That Look Like Green-Flag Pretending to Be Red-Flag

None identified. Test categorization appears correct.

---

## 8. Error Semantics & UX

### Error Construction

**Code**: `internal/errors/errors.go`

All errors inherit from `CanonicError`:
```go
type CanonicError struct {
    Code       ErrorCode  // Validation=1, Auth=2, Engine=3, Internal=4
    Message    string     // Short description
    Reason     string     // Why it happened
    Suggestion string     // What to do
    Cause      error      // Wrapped error
}
```

### Human-Readable Messages

**YES** - Every error includes:
- What failed (Message)
- Why (Reason)
- How to fix (Suggestion)

**Example**:
```
access denied on table 'analytics.sales_orders'
Reason: role(s) [analyst] lack READ permission on analytics.sales_orders
Suggestion: request READ permission on 'analytics.sales_orders' from administrator
```

### Cause Preservation

**YES** - Errors wrap underlying cause via `Cause` field and implement `Unwrap()`.

### Can Users Understand Why Queries Were Blocked?

**YES** for:
- Missing capability ("table lacks TIME_TRAVEL capability")
- Constraint violation ("READ_ONLY constraint active")
- Authorization failure ("role lacks permission")
- Unsupported syntax ("WINDOW FUNCTION is not supported")

**PARTIALLY** for:
- Generic parse errors (less specific)

### Errors Swallowed or Generalized?

**NO swallowing** - All errors propagate.

**Some generalization** in `classifyParseError()`:
- If classification fails, returns `nil` and falls back to generic "invalid SQL syntax"
- This is acceptable per phase-3-spec.md ("where classification is possible")

---

## 9. AI / Copilot Usage Signals

### Areas That Look AI-Generated

Based on code patterns:

1. **Test scaffolding in `tests/redflag/*.go` and `tests/greenflag/*.go`**
   - Highly consistent structure
   - Repetitive table-driven test patterns
   - Extensive comments explaining purpose
   - **Verdict**: Likely AI-assisted, appropriately so per copilot-instructions.md

2. **Adapter implementations (`duckdb/`, `trino/`, `spark/`)**
   - Nearly identical structure
   - Copy-paste patterns with engine-specific changes
   - **Verdict**: Likely AI-assisted boilerplate, acceptable

3. **Error type definitions in `errors.go`**
   - Repetitive constructors following same pattern
   - **Verdict**: Likely AI-assisted, acceptable

### Areas That Should NOT Have Been AI-Generated

1. **`checkSnapshotConsistency()` in planner.go**
   - Contains planner decision logic
   - Per copilot-instructions.md: "Planner decision logic" is FORBIDDEN
   - However, the logic appears correct and has passing tests
   - **Risk**: Medium - Logic is simple and verified

2. **`Authorize()` in authorization.go**
   - Contains authorization logic
   - Per copilot-instructions.md: "authorization logic" is FORBIDDEN
   - **Risk**: Medium - Logic is straightforward deny-by-default

3. **`detectUnsupportedSyntax()` in parser.go**
   - Contains semantic validation
   - Per copilot-instructions.md: "semantic validation" is FORBIDDEN
   - **Risk**: Low - Pattern matching, not business rules

### Assessment

The codebase shows signs of AI assistance in appropriate areas (boilerplate, tests, adapters). Critical logic areas are simple enough that AI assistance, if used, hasn't introduced obvious defects. All critical paths have test coverage.

---

## 10. Deviations from plan.md

### Features Implemented But Not in plan.md

| Feature | Location | Risk | Recommendation |
|---------|----------|------|----------------|
| `ProductionMode` config flag | gateway.go | Low | Keep - useful for Phase 3 |
| `GatewayClient` for CLI | cli/gateway_client.go | Low | Keep - per Phase 3 |
| `DetectMetadataConflict()` | storage/repository.go | Low | Keep - per Phase 3 |

### Features Promised in plan.md But Missing

| Feature | plan.md Section | Status | Risk | Recommendation |
|---------|-----------------|--------|------|----------------|
| PostgreSQL wired to gateway | §6.2, §4.3 | **Not wired** | **HIGH** | Fix - gateway uses in-memory only |
| `/cmd/planner/` separate service | §5 | Missing | Low | Remove from plan.md or add later |
| Column-level security | §8 | Noted as excluded | Low | Keep excluded |
| JWT authentication | §4.3 | T001 deferred | Medium | Implement before production |
| JDBC interface | §4.4 | Not implemented | Medium | Document scope change |

### Scope Creep

1. **Phase 3 implementation** - Implemented but not called out in original plan.md
   - Risk: Low (coherent additions)
   - Recommendation: Update plan.md to reference phase specs

### Accidental Complexity

1. **`NewGatewayWithDB()` creates in-memory registry**
   - Intended to use PostgreSQL, falls back silently
   - Risk: **HIGH** - Violates Phase 3 §7
   - Recommendation: Remove fallback, fail if DB unavailable

2. **CLI has both local and gateway implementations**
   - `table.go` has local operations
   - `gateway_client.go` has gateway operations
   - Neither is wired completely
   - Risk: Medium - Confusing state
   - Recommendation: Remove local implementations from CLI commands

---

## 11. Technical Debt & Risk Register

| Issue | Location | Impact | Likelihood | Mitigation |
|-------|----------|--------|------------|------------|
| PostgreSQL not wired | gateway.go, main.go | Metadata lost on restart | Certain | Wire PostgresRepository to gateway |
| CLI commands not using GatewayClient | cli/table.go, cli/query.go | CLI diverges from gateway | High | Refactor CLI to use GatewayClient |
| No engine health checks | router.go | Queries sent to dead engines | Medium | Add Ping() call before routing |
| Static token only | auth.go | Insecure for production | High | Implement JWT (T001) |
| No SQL dialect translation | adapters/*.go | Engine-specific queries fail on other engines | Medium | Document limitation or add translation |
| Time travel uses text search | parser.go | False positives on AS OF in strings | Low | Accept limitation or upgrade parser |
| CTE not supported | parser.go | Common SQL pattern rejected | Medium | Document or upgrade vitess version |
| No metrics/tracing | observability/ | Hard to diagnose production issues | Medium | Add OpenTelemetry |
| `pkg/` directory unused | Repository root | Confusing structure | Low | Remove or populate |
| Authorization service has no default permissions | gateway.go | All queries denied by default | Medium | Document required configuration |
| Map iteration non-determinism | router.go | Could affect engine selection | Low | Sort engines before selection |

---

## 12. Questions That Must Be Answered

### Architecture

1. **Why is PostgreSQL repository implemented but not wired?**
   - `NewGatewayWithDB()` exists but creates in-memory registry as fallback
   - Is this intentional temporary state or overlooked?

2. **Is the `/cmd/planner/` separate service still planned?**
   - plan.md shows it in repo structure, but it doesn't exist
   - Should gateway continue embedding planner?

3. **Why does `DefaultRouter()` only register DuckDB?**
   - Trino and Spark adapters exist and are complete
   - Is this MVP-only or should they be registered?

### Safety

4. **What prevents SQL injection through table names?**
   - Schema-qualified enforcement helps, but table names from user queries are used in registry lookups
   - Is there additional sanitization needed?

5. **What is the authorization configuration story?**
   - `NewGateway()` creates empty `AuthorizationService`
   - How do permissions get loaded in production?

6. **Why does `NewGatewayWithDB()` have a fallback to in-memory?**
   - Comment says "TODO: Wire to PostgresRepository"
   - This violates Phase 3 §7 requirement

### Semantics

7. **How should cross-engine SQL dialect differences be handled?**
   - Raw SQL is passed to engines without translation
   - Is this intentional (engine-specific SQL expected) or a gap?

8. **What happens when SNAPSHOT_CONSISTENT tables from different databases are joined?**
   - Current check only validates same AS OF timestamp
   - Does this guarantee actual snapshot consistency across physical sources?

9. **Is time travel detection via text search sufficient?**
   - "AS OF" could appear in string literals
   - False positives possible but maybe acceptable?

### Implementation

10. **Are CLI commands supposed to use GatewayClient?**
    - `gateway_client.go` exists with all methods
    - `table.go` and `query.go` have local implementations
    - Which is authoritative?

11. **Why are there two test file patterns for authorization?**
    - `redflag/auth_test.go` (authentication)
    - `redflag/authorization_test.go` (authorization)
    - Is this intentional separation?

12. **What is the expected startup sequence?**
    - Does gateway require PostgreSQL before starting?
    - Current `cmd/gateway/main.go` starts with in-memory registry regardless

### Operations

13. **How are virtual tables loaded on gateway startup?**
    - PostgreSQL tables exist in schema
    - No bootstrap or migration runner in main.go

14. **What is the HA/failover story?**
    - Multiple gateways would need shared PostgreSQL
    - Is this documented?

15. **How are engine availability changes detected?**
    - `SetEngineAvailability()` exists
    - Nothing calls it based on health checks

---

## Summary

Canonica has a **sound architectural foundation** with proper separation of concerns, explicit error handling, and comprehensive test coverage. The core control plane logic (parsing, planning, routing, authorization) is correct and well-tested.

**Critical gaps** that must be addressed:
1. PostgreSQL repository exists but is NOT WIRED to the gateway
2. CLI commands exist but don't use the new `GatewayClient`
3. `NewGatewayWithDB()` silently falls back to in-memory registry, violating Phase 3 requirements

**The system does what it claims** for the MVP scope (read-only queries via DuckDB), but the Phase 3 claims about PostgreSQL authority and CLI→Gateway integration are **not yet true** despite tests passing.

Tests pass because they test the components in isolation. Integration is incomplete.
