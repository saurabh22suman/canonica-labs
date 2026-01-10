# Engineering Audit Report – canonic-labs

**Date**: 2026-01-10  
**Auditor**: AI Engineering Auditor (Opus 4.5)  
**Scope**: Full codebase analysis against plan.md, test.md, CONTRIBUTING.md

---

## 1. Project Snapshot

### What This System Currently Does

Canonic-labs is a **read-only SQL gateway** that provides:

1. **HTTP API Gateway** (`/query`, `/query/explain`, `/query/validate`, `/tables`, `/engines`)
2. **Static Token Authentication** (single-token auth, not JWT)
3. **SQL Parsing** (regex-based, not AST-based)
4. **Virtual Table Registry** (in-memory only, PostgreSQL repository implemented but not wired)
5. **Capability-Based Access Control** (READ, TIME_TRAVEL capabilities)
6. **Constraint Enforcement** (READ_ONLY, SNAPSHOT_CONSISTENT)
7. **Rule-Based Engine Routing** (priority-based selection, deterministic)
8. **DuckDB Query Execution** (actual SQL execution with results)
9. **Structured Query Logging** (JSON format with all required fields)

### What This System Explicitly Does NOT Do

- **Execute Writes** – INSERT/UPDATE/DELETE blocked at parse time
- **Use PostgreSQL Metadata** – Repository implemented but not integrated
- **Use Trino/Spark for Execution** – Adapters exist but marked unavailable in DefaultRouter
- **Use JWT Authentication** – Static tokens only (tracked as T001)
- **Parse SQL with AST** – Uses regex patterns (tracked as T009)
- **Connect CLI to Gateway** – CLI operates locally (tracked as T010, T011)

### Implementation Status

| Component | Status | Evidence |
|-----------|--------|----------|
| HTTP Gateway | **Complete** | 669 lines, 11 endpoints, full request/response lifecycle |
| SQL Parser | **Stub** | 210 lines, regex-based, explicit TODO for vitess |
| Table Abstraction | **Complete** | VirtualTable model with capabilities/constraints |
| Planner | **Complete** | 183 lines, rule-based, deterministic |
| Router | **Complete** | 179 lines, priority-based engine selection |
| DuckDB Adapter | **Complete** | Actual execution via database/sql |
| Trino Adapter | **Implemented** | Real driver but marked unavailable in router |
| Spark Adapter | **Partial** | TCP ping only, no HiveServer2 driver |
| Auth | **MVP** | Static tokens, no JWT |
| Observability | **Complete** | JSONLogger with all plan.md fields |
| Storage | **Implemented but Unused** | PostgreSQL repository not wired to gateway |

---

## 2. Repository Map

```
canonic-labs/
├── cmd/
│   ├── canonic/         [CLI entry point - complete]
│   └── gateway/         [HTTP server entry point - complete]
├── internal/
│   ├── adapters/        [Engine adapters - partial]
│   │   ├── adapter.go   [Interface + registry - complete]
│   │   ├── duckdb/      [DuckDB adapter - complete]
│   │   ├── trino/       [Trino adapter - implemented, unused]
│   │   └── spark/       [Spark adapter - stub, no driver]
│   ├── auth/            [Authentication - MVP only]
│   ├── capabilities/    [Capability model - complete]
│   ├── cli/             [CLI commands - complete but disconnected]
│   ├── config/          [Configuration - complete]
│   ├── errors/          [Error types - complete, 259 lines]
│   ├── gateway/         [HTTP handlers - complete]
│   ├── observability/   [Query logging - complete]
│   ├── planner/         [Query planning - complete]
│   ├── router/          [Engine selection - complete]
│   ├── sql/             [SQL parsing - stub]
│   ├── storage/         [PostgreSQL repository - unused]
│   └── tables/          [Virtual table model - complete]
├── migrations/          [PostgreSQL schema - complete, unused]
├── examples/            [Sample YAML files - 2 examples]
├── tests/
│   ├── redflag/         [74 Red-Flag tests]
│   └── greenflag/       [68 Green-Flag tests]
└── docs/
    ├── plan.md          [Authoritative specification]
    ├── test.md          [TDD doctrine]
    ├── tracker.md       [Deferred features]
    └── refactoring-skills.md [AI usage guidelines]
```

### Unused/Orphaned Code

| Location | Status | Risk |
|----------|--------|------|
| `internal/storage/postgres_repository.go` | 393 lines, never instantiated | Medium – dead code or incomplete integration |
| `internal/adapters/trino/` | Complete adapter, marked `Available: false` | Low – intentionally deferred |
| `internal/adapters/spark/` | No real driver, TCP ping only | Low – clearly documented as MVP limitation |
| `migrations/` | SQL files exist, never run | Medium – schema unused |

---

## 3. Control Plane Flow (End-to-End)

### Step 1: Entry Point

**Code**: `cmd/gateway/main.go`, `internal/gateway/gateway.go`

- HTTP server on port 8080 (default)
- Routes registered in `registerRoutes()`
- `/health` is public, all others require auth

**Assumptions**:
- Single static token for all users
- Token passed via `Authorization: Bearer <token>`

**Failure Modes**:
- Missing token → 401 Unauthorized (explicit message)
- Invalid token → 401 Unauthorized (explicit message)

### Step 2: Authentication

**Code**: `internal/auth/auth.go`, `gateway.requireAuth()`

- Token extracted from `Authorization` header
- Looked up in static map
- User attached to request context

**Assumptions**:
- Token never expires unless explicitly set (ExpiresAt zero value)
- No rate limiting

**Failure Modes**:
- Empty token → `ErrAuthFailed` with "token required"
- Unknown token → `ErrAuthFailed` with "invalid token"
- Expired token → `ErrAuthExpired`

**What Can Go Wrong**:
- No metadata leakage test exists for *valid but unauthorized* table access

### Step 3: SQL Parsing

**Code**: `internal/sql/parser.go`

- Regex-based operation detection
- Table extraction via string matching
- Time-travel detection via "AS OF" pattern

**Assumptions**:
- All queries start with operation keyword
- Table names appear after FROM
- No subqueries, CTEs, or complex joins

**Failure Modes**:
- Empty query → `ErrQueryRejected`
- Unknown operation → `ErrQueryRejected`
- Write operation → `ErrWriteNotAllowed`

**CRITICAL RISK**:
- **Regex parsing will fail on**: nested queries, aliased tables, quoted identifiers, schema-qualified names with dots. This is explicitly tracked as T009.

### Step 4: Table Resolution

**Code**: `internal/planner/planner.go`, `internal/gateway/gateway.go` (InMemoryTableRegistry)

- Each table name resolved via `TableRegistry.GetTable()`
- Returns `*tables.VirtualTable` with capabilities/constraints

**Assumptions**:
- Table names are exact matches (case-sensitive)
- No schema/catalog prefix handling
- Tables pre-registered in memory

**Failure Modes**:
- Unknown table → `ErrTableNotFound`

**What's Missing**:
- Ambiguous table resolution test exists in docs but no `ErrAmbiguousTable` is ever thrown in current code

### Step 5: Capability Checks

**Code**: `internal/planner/planner.go:checkTableCapabilities()`, `internal/tables/virtual_table.go:CanPerform()`

- Check if operation is write (blocked in MVP)
- Check READ_ONLY constraint
- Check each required capability exists

**Enforcement Order**:
1. `CanPerform()` checks constraints first
2. Then checks required capabilities

**Invariants Actually Enforced**:
- ✅ Writes blocked at parse time AND table level
- ✅ READ_ONLY constraint blocks writes
- ✅ Missing capability blocks operation
- ✅ TIME_TRAVEL required for AS OF queries

**Failure Modes**:
- Constraint violation → `ErrConstraintViolation`
- Missing capability → `ErrCapabilityDenied`

### Step 6: Engine Selection (Router)

**Code**: `internal/router/router.go:SelectEngine()`

- Iterates all registered engines
- Filters by: available AND has all required capabilities
- Selects lowest priority number (highest priority)

**Assumptions**:
- Priority is stable (set at registration)
- No load balancing or health checks
- Engine marked unavailable → skipped entirely

**Determinism**: **YES** – given same capabilities and availability, same engine selected

**Failure Modes**:
- No matching engine → `ErrEngineUnavailable`

### Step 7: Adapter Execution

**Code**: `internal/adapters/duckdb/adapter.go:Execute()`

- Receives `*planner.ExecutionPlan`
- Executes raw SQL against database/sql
- Returns `*adapters.QueryResult`

**Assumptions**:
- RawSQL from LogicalPlan is valid for target engine
- No SQL rewriting or translation
- Context cancellation checked during row iteration

**Failure Modes**:
- Context cancelled → error with cause
- Nil plan → explicit error
- Closed adapter → explicit error
- SQL execution error → wrapped and returned

**What's Missing**:
- No timeout enforcement (relies on context)
- No retry logic (intentionally per plan.md)
- No partial result handling (fails entire query)

### Step 8: Result Handling

**Code**: `internal/gateway/gateway.go:handleQuery()`

- Converts `[][]interface{}` to `[]map[string]interface{}`
- Logs query via observability logger
- Returns JSON response with query_id, columns, rows, engine, duration

**Error Propagation**:
- All errors logged before response
- Errors include Reason and Suggestion
- HTTP status codes: 400 (validation), 401 (auth), 500 (execution)

---

## 4. Table Abstraction Layer Analysis (CRITICAL)

### Virtual Table Representation

```go
type VirtualTable struct {
    Name         string
    Sources      []PhysicalSource
    Capabilities []Capability      // READ, TIME_TRAVEL
    Constraints  []Constraint      // READ_ONLY, SNAPSHOT_CONSISTENT
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```

### Capability Enforcement

**Where**: `internal/tables/virtual_table.go:CanPerform()`

- Called by planner before engine selection
- Checks operation type against capabilities

**Actually Enforced**:
- ✅ READ capability required for SELECT
- ✅ TIME_TRAVEL capability required for AS OF
- ✅ Write operations blocked regardless of capabilities

### Constraint Enforcement

**Where**: `internal/tables/virtual_table.go:CanPerform()`

**Actually Enforced**:
- ✅ READ_ONLY blocks all writes
- ❌ SNAPSHOT_CONSISTENT – **defined but never checked**

### Conflict Handling

**Where**: `internal/tables/virtual_table.go:Validate()`

- Conflicting sources (same format, different locations) rejected
- Duplicate capabilities/constraints allowed (no dedup)

### Invariants Analysis

| Invariant | Documented | Enforced | Risk |
|-----------|------------|----------|------|
| Writes blocked in MVP | ✅ plan.md | ✅ Parser + VirtualTable | None |
| READ_ONLY constraint | ✅ plan.md | ✅ VirtualTable.CanPerform | None |
| SNAPSHOT_CONSISTENT | ✅ plan.md | ❌ Never checked | **Medium** |
| TIME_TRAVEL requires capability | ✅ test.md | ✅ Planner | None |
| Conflicting sources rejected | ✅ plan.md | ✅ VirtualTable.Validate | None |

### Unsafe Behavior Currently Possible

1. **SNAPSHOT_CONSISTENT constraint is ignored** – A table marked SNAPSHOT_CONSISTENT behaves identically to one without it. No test exists for this.

2. **Table names with dots are not split** – A query for `analytics.sales_orders` will look for a table literally named `analytics.sales_orders`, not schema `analytics` table `sales_orders`. This may or may not be intentional.

3. **Multiple sources with same format allowed** – If two sources have same format and same location, only last one wins in the format→location map.

---

## 5. Planner & Router Analysis

### How Engine Selection Works

1. Planner calls `EngineMatcher.SelectEngine(ctx, []Capability)`
2. Router iterates registered engines
3. Filters: `Available == true` AND `HasAllCapabilities()`
4. Selects lowest `Priority` number

### Routing Determinism

**YES** – Engine selection is deterministic given:
- Same registered engines
- Same availability states
- Same required capabilities

**Proof**: `SelectEngine()` uses no randomness, timestamps, or external state.

### Fallback Behavior

**Explicit**: No fallback. If no engine matches, `ErrEngineUnavailable` returned.

**Accidental**: None. There is no silent downgrade.

### Critical Questions Answered

| Question | Answer |
|----------|--------|
| Can the planner make a silent decision? | **No** – All decisions logged, all failures explicit |
| Can two identical queries route differently? | **No** – Deterministic selection |
| What happens if no engine matches? | `ErrEngineUnavailable` with required capabilities listed |

### DefaultRouter Configuration

Current `DefaultRouter()` sets:
- DuckDB: Priority 1, Available true
- Trino: Priority 2, Available **false**
- Spark: Priority 3, Available **false**

**Risk**: Trino/Spark adapters are complete but never used because router defaults to unavailable.

---

## 6. Engine Adapters

### Common Interface

```go
type EngineAdapter interface {
    Name() string
    Capabilities() []Capability
    Execute(ctx, *ExecutionPlan) (*QueryResult, error)
    Ping(ctx) error
    Close() error
}
```

### DuckDB Adapter

| Aspect | Status |
|--------|--------|
| SQL Translation | **None** – RawSQL passed directly |
| Connection Pooling | Via database/sql |
| Context Handling | Checked before and during iteration |
| Error Wrapping | All errors wrapped with adapter prefix |

**Logic Embedded**: None. Pure translation.

### Trino Adapter

| Aspect | Status |
|--------|--------|
| Driver | `github.com/trinodb/trino-go-client` |
| Configuration | Host, Port, Catalog, Schema, User, SSL |
| SQL Translation | **None** – RawSQL passed directly |
| Availability | Marked unavailable in DefaultRouter |

**Logic Embedded**: DSN construction only. No query rewriting.

### Spark Adapter

| Aspect | Status |
|--------|--------|
| Driver | **None** – TCP ping only |
| Configuration | Host, Port, Database, AuthMethod |
| SQL Translation | N/A |
| Execution | Returns error "requires Spark Thrift Server driver (not available in MVP)" |

**Logic Embedded**: Connection timeout handling. No semantic logic.

### Engine-Specific Quirks Leaking Upward

| Quirk | Status |
|-------|--------|
| DuckDB TIME_TRAVEL support | Adapter reports capability, but DuckDB requires Delta/Iceberg extensions. No runtime check. |
| Trino catalog/schema | Included in metadata but not validated. |
| Spark HiveServer2 | Complete absence of actual driver. |

---

## 7. Red-Flag / Green-Flag Test Coverage

### Test Counts

| Category | Red-Flag | Green-Flag |
|----------|----------|------------|
| Gateway | 7 | 11 |
| SQL Parser | 5 | 0 |
| Capabilities | 3 | 4 |
| Tables | 7 | 5 |
| Planner/Router | 4 | 0 |
| Storage | 8 | 11 |
| DuckDB | 8 | 13 |
| Trino | 8 | 8 |
| Spark | 9 | 9 |
| Observability | 6 | 10 |
| Auth | 4 | 0 |
| **Total** | **74** | **68** |

### Critical Red-Flag Tests Implemented

- ✅ `TestGateway_RejectsWriteOperations`
- ✅ `TestGateway_RejectsUnknownTable`
- ✅ `TestGateway_RejectsQueryWithoutCapability`
- ✅ `TestCapabilityDenied_TimeTravelWithoutCapability`
- ✅ `TestConstraintViolation_ReadOnlyBlocksWrite`
- ✅ `TestRouter_NoAvailableEngine`
- ✅ `TestAuth_InvalidToken`

### Missing Red-Flag Tests

| Missing Test | Risk | Priority |
|--------------|------|----------|
| Unauthorized table access by valid user | Medium | P1 |
| SNAPSHOT_CONSISTENT constraint behavior | Medium | P1 |
| Query timeout handling | Low | P2 |
| Ambiguous table resolution | Low | P2 |
| SQL injection via table names | High | P0 |
| Engine partial failure (schema mismatch) | Medium | P1 |

### Suspicious Tests

| Test | Issue |
|------|-------|
| `TestQuery_ValidSelectPasses` (redflag) | This is a Green-Flag test in the Red-Flag directory. It tests success, not failure. |
| `TestDuckDB_ImplementsEngineAdapter` (redflag) | Compile-time check, not a behavioral Red-Flag. |
| `TestTrino_ImplementsEngineAdapter` (redflag) | Same issue. |
| `TestSpark_ImplementsEngineAdapter` (redflag) | Same issue. |

---

## 8. Error Semantics & UX

### Error Construction

All errors extend `CanonicError`:

```go
type CanonicError struct {
    Code       ErrorCode
    Message    string
    Reason     string
    Suggestion string
    Cause      error
}
```

### Error Types Implemented

| Error | Message Example | Has Suggestion |
|-------|-----------------|----------------|
| `ErrCapabilityDenied` | "SELECT forbidden on X" | ✅ |
| `ErrConstraintViolation` | "DELETE forbidden on X" | ✅ |
| `ErrTableNotFound` | "table not found: X" | ✅ |
| `ErrEngineUnavailable` | "no compatible engine available" | ✅ |
| `ErrAuthFailed` | "authentication failed" | ✅ |
| `ErrQueryRejected` | "query rejected" | ✅ |
| `ErrWriteNotAllowed` | "INSERT operation not allowed" | ✅ |

### User Experience

**Can a user understand why a query was blocked?**

**YES** – Every error includes:
- Human-readable message
- Reason explaining the cause
- Suggestion for remediation

**Example**:
```
DELETE forbidden on analytics.sales_orders
Reason: READ_ONLY constraint active
Suggestion: check table constraints with 'canonic table describe analytics.sales_orders'
```

### Errors Swallowed or Generalized

| Location | Issue |
|----------|-------|
| `gateway.handleQuery()` | Adapter errors wrapped as "query execution failed" – original message preserved in Reason |
| `JSONLogger.LogQuery()` | Returns error but gateway ignores logging failures |

---

## 9. AI / Copilot Usage Signals

### Patterns Suggesting AI Generation

| Signal | Locations | Concern |
|--------|-----------|---------|
| Highly repetitive adapter code | trino/adapter.go, spark/adapter.go, duckdb/adapter.go | Low – Appropriate per copilot-instructions.md |
| Identical error handling blocks | All adapters | Low – Boilerplate is allowed |
| Comprehensive comments before functions | Throughout | Low – Good practice |
| Table-driven test patterns | tests/redflag/*.go, tests/greenflag/*.go | Low – Encouraged per test.md |

### Copilot Rules Respected

| Rule | Evidence |
|------|----------|
| No AI for planner logic | Planner is simple, deterministic, human-readable |
| No AI for capability enforcement | VirtualTable.CanPerform() is explicit |
| No AI for security logic | Auth is minimal, straightforward |
| AI allowed for adapters | Adapter code is clearly scaffolded |
| AI allowed for test boilerplate | Tests follow consistent patterns |

### Areas That Should NOT Have Been AI-Generated

**I cannot determine this with certainty from the code alone.**

However, the following areas show no signs of AI-generated logic errors:
- Planner decision flow
- Capability checking
- Constraint enforcement
- Router selection

---

## 10. Deviations from plan.md (IMPORTANT)

### Features Implemented but NOT in plan.md

| Feature | Location | Risk | Recommendation |
|---------|----------|------|----------------|
| `/query/validate` endpoint | gateway.go | Low | **Keep** – Useful for dry-run |
| `ErrAmbiguousTable` error type | errors.go | Low | **Keep** – Never triggered but ready |
| NoopLogger | observability.go | Low | **Keep** – Testing utility |

### Features Promised in plan.md but MISSING

| Feature | Section | Impact | Risk | Recommendation |
|---------|---------|--------|------|----------------|
| JDBC interface | §4.4 | No Postgres-compatible SQL | Medium | **Defer** – HTTP is sufficient for MVP |
| Role → table mapping | §8 | No authorization checks | High | **Fix** – Security gap |
| Audit logs in PostgreSQL | §4.3 | No audit trail | Medium | **Defer** – Logging exists |
| SNAPSHOT_CONSISTENT enforcement | §6.2 | Constraint ignored | Medium | **Fix** – False promise |
| Trino as primary engine | §2.4, §6.3 | DuckDB is primary | Low | **Keep** – Intentional MVP choice |

### Scope Creep

| Area | Evidence | Risk |
|------|----------|------|
| Full CLI implementation | 7 command files | Low – Useful but disconnected |
| PostgreSQL repository | 393 lines, complete schema | Medium – Dead code |

### Accidental Complexity

| Area | Issue | Risk |
|------|-------|------|
| Two table registries | InMemoryTableRegistry (gateway) + MockRepository (storage) | Medium – Confusing |
| Adapter reports TIME_TRAVEL | No runtime validation that engine actually supports it | Low |

---

## 11. Technical Debt & Risk Register

| ID | Issue | Location | Impact | Likelihood | Mitigation |
|----|-------|----------|--------|------------|------------|
| R001 | SQL parser uses regex | internal/sql/parser.go | Queries with complex syntax fail or misbehave | High | Replace with vitess/sqlparser (T009) |
| R002 | SNAPSHOT_CONSISTENT not enforced | internal/tables/virtual_table.go | Constraint is documentation-only | Medium | Add enforcement logic |
| R003 | No role → table authorization | internal/auth/auth.go | Any authenticated user can query any table | High | Implement RBAC |
| R004 | PostgreSQL repository unused | internal/storage/ | 393 lines of dead code | Low | Wire to gateway or remove |
| R005 | CLI not connected to gateway | internal/cli/*.go | CLI operates locally only | Medium | Complete integration (T010, T011) |
| R006 | Trino/Spark adapters unavailable | internal/router/router.go | DefaultRouter marks them unavailable | Low | Intentional – update when deploying |
| R007 | DuckDB TIME_TRAVEL unvalidated | internal/adapters/duckdb/ | Capability claimed without verification | Medium | Add runtime extension check |
| R008 | No SQL injection protection | internal/sql/parser.go | Table names passed to query unsanitized | High | Parser extracts names; adapters execute raw SQL |
| R009 | Logging failures ignored | internal/gateway/gateway.go | Logger.LogQuery errors not checked | Low | Add error handling |
| R010 | No graceful shutdown for adapters | cmd/gateway/main.go | AdapterRegistry.CloseAll() not called on shutdown | Low | Add shutdown hook |

---

## 12. Questions That Must Be Answered

### Architecture Questions

1. **Why is the PostgreSQL repository implemented but not used?** Is there a plan to switch from InMemoryTableRegistry, or is this orphaned code?

2. **Why are Trino and Spark marked unavailable in DefaultRouter?** The adapters exist and are tested. Is this a configuration issue or intentional deferral?

3. **What is the intended relationship between CLI and Gateway?** The CLI has full command structure but no HTTP client. Is the CLI meant to be a standalone local tool?

### Security Questions

4. **Why is there no role → table mapping?** plan.md §8 says "Role → table mapping" but any authenticated user can query any table. Is this acceptable for MVP?

5. **What prevents SQL injection via table names?** The parser extracts table names from SQL via regex, then those names are resolved. But the original SQL is executed unchanged. If a malicious table name is registered, could it contain SQL?

### Semantic Questions

6. **What does SNAPSHOT_CONSISTENT actually mean?** It's defined as a valid constraint but never checked. What behavior should it enforce?

7. **Is `analytics.sales_orders` a qualified name or a literal?** Current code treats dots in table names as literal characters, not schema separators. Is this intentional?

8. **What happens if a VirtualTable has no capabilities?** The capability check would fail for any operation. Is an empty capability list valid?

### Operational Questions

9. **How are virtual tables meant to be registered in production?** The gateway uses InMemoryTableRegistry with no persistence. Is CLI registration (when connected) the intended flow?

10. **Why do adapters report TIME_TRAVEL capability without runtime verification?** DuckDB requires extensions for Delta/Iceberg. Trino requires compatible connectors. What prevents capability lies?

### Testing Questions

11. **Why is `TestQuery_ValidSelectPasses` in the redflag directory?** It tests successful parsing, not failure.

12. **Why are there no Red-Flag tests for SQL parser edge cases?** No tests for: quoted identifiers, subqueries, CTEs, UNION, schema-qualified names.

13. **Why is there no test for unauthorized table access?** A valid user attempting to query a table they shouldn't access – does the system allow it?

---

## Summary

Canonic-labs is a well-structured control plane with strong foundations:
- Clear separation of concerns
- Explicit error handling
- Deterministic routing
- Comprehensive Red-Flag/Green-Flag tests

However, critical gaps remain:
- **SQL parser is regex-based** (high risk)
- **No role-based authorization** (high risk)
- **SNAPSHOT_CONSISTENT is a lie** (medium risk)
- **PostgreSQL integration is dead code** (medium risk)
- **CLI is disconnected** (medium risk)

The codebase follows its own rules (plan.md, test.md, copilot-instructions.md) with high fidelity. The issues found are scope gaps, not violations.

**Recommendation**: Address R001 (SQL parser), R003 (RBAC), and R002 (SNAPSHOT_CONSISTENT) before any production use.
