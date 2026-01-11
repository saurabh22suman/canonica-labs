# Canonic-Labs: User Flow & System Architecture

## System Overview

Canonic-labs is a **federated query gateway** that routes SQL queries to the appropriate engine while enforcing access control, capability checks, and consistency guarantees.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER INTERFACES                                 │
├─────────────┬─────────────┬─────────────────────────────────────────────────┤
│   CLI       │   HTTP API  │   SQL Clients (future)                          │
│  `canonic`  │  :8080      │                                                 │
└──────┬──────┴──────┬──────┴─────────────────────────────────────────────────┘
       │             │
       ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GATEWAY                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  1. Parse SQL (vitess/sqlparser)                                      │   │
│  │  2. Extract tables                                                    │   │
│  │  3. Authorize (Role → Table → Capability)                             │   │
│  │  4. Plan (match capabilities to engines)                              │   │
│  │  5. Route to engine                                                   │   │
│  │  6. Return results or rejection reason                                │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
       │                    │                           │
       ▼                    ▼                           ▼
┌─────────────┐    ┌─────────────────┐    ┌───────────────────────────────────┐
│ PostgreSQL  │    │ Adapter Registry│    │         Query Engines             │
│ (Metadata)  │    │                 │    │  ┌─────────┬─────────┬─────────┐  │
│             │    │  - DuckDB       │    │  │ DuckDB  │  Trino  │  Spark  │  │
│ - tables    │    │  - Trino        │    │  │ (OLAP)  │ (dist)  │ (batch) │  │
│ - roles     │    │  - Spark        │    │  └─────────┴─────────┴─────────┘  │
│ - grants    │    │                 │    │                                   │
└─────────────┘    └─────────────────┘    └───────────────────────────────────┘
```

---

## Lifecycle of a Query

### Step 1: Entry Point

**CLI Path:**
```bash
canonic query run "SELECT * FROM analytics.sales WHERE region = 'US'"
```

The CLI (`internal/cli/query.go`) does **no local processing**. It sends the query to the gateway via HTTP:

```go
// internal/cli/gateway_client.go
func (c *GatewayClient) ExecuteQuery(ctx context.Context, sql string) (*QueryResult, error) {
    req := QueryRequest{SQL: sql}
    resp, err := c.httpClient.Post(c.baseURL+"/query", "application/json", body)
    // Returns results or error - no local fallback
}
```

**HTTP API Path:**
```bash
POST /query
Content-Type: application/json
Authorization: Bearer <token>

{"sql": "SELECT * FROM analytics.sales WHERE region = 'US'"}
```

---

### Step 2: Parsing & Table Extraction

The gateway parses SQL using vitess/sqlparser:

```go
// internal/sql/parser.go
func (p *Parser) ExtractTables(sql string) ([]string, error) {
    stmt, err := sqlparser.Parse(sql)
    // Walks AST to find all TableName nodes
    // Returns: ["analytics.sales"]
}
```

**Enforced Invariants:**
- Tables MUST be schema-qualified (`analytics.sales`, not `sales`)
- Unsupported syntax (CTEs, WINDOW functions) → explicit rejection
- Vendor hints (USE INDEX, FORCE INDEX) → rejected

---

### Step 3: Authorization (Deny-by-Default)

Before checking if tables exist, the gateway checks if the user **is allowed to know**:

```go
// internal/auth/authorization.go
func (s *AuthorizationService) Authorize(role, table string, caps []Capability) error {
    grant, exists := s.grants[role][table]
    if !exists {
        return ErrUnauthorized  // Deny by default
    }
    for _, cap := range caps {
        if !grant.HasCapability(cap) {
            return ErrCapabilityDenied
        }
    }
    return nil
}
```

**Security Properties:**
- No implicit access
- Unauthorized tables don't leak existence
- Multi-table queries require permission on ALL tables

---

### Step 4: Table Resolution

The gateway fetches table metadata from PostgreSQL:

```go
// internal/storage/repository.go
func (r *PostgresRepository) GetTable(ctx context.Context, name string) (*Table, error) {
    row := r.db.QueryRowContext(ctx, 
        "SELECT name, engine, capabilities, constraints FROM tables WHERE name = $1", 
        name)
    // Returns table with capabilities and constraints
}
```

**Table Definition Example:**
```yaml
tables:
  analytics.sales:
    engine: trino
    capabilities: [READ, AGGREGATE]
    constraints: [SNAPSHOT_CONSISTENT]
```

---

### Step 5: Capability & Constraint Checking

The planner validates query requirements against table capabilities:

```go
// internal/planner/planner.go
func (p *Planner) Plan(query ParsedQuery, tables []Table) (*Plan, error) {
    // 1. Check all required capabilities are satisfied
    for _, table := range tables {
        if query.RequiresAggregate && !table.HasCapability(AGGREGATE) {
            return nil, ErrCapabilityMissing
        }
    }
    
    // 2. Enforce constraints
    if table.HasConstraint(SNAPSHOT_CONSISTENT) {
        if !query.HasAsOfClause {
            return nil, ErrSnapshotRequired
        }
    }
    
    // 3. Select engine
    engine := p.selectEngine(tables, query)
    return &Plan{Engine: engine, Query: query}, nil
}
```

**SNAPSHOT_CONSISTENT Constraint:**
- Queries MUST include `AS OF <timestamp>`
- Cannot mix SNAPSHOT_CONSISTENT with non-snapshot tables

---

### Step 6: Engine Routing

The router selects an engine adapter based on table configuration:

```go
// internal/router/router.go
func (r *Router) Route(plan *Plan) (EngineAdapter, error) {
    adapter, exists := r.adapters[plan.Engine]
    if !exists {
        return nil, ErrNoEngineAvailable
    }
    return adapter, nil
}
```

**Available Engines:**

| Engine | Capabilities | Use Case |
|--------|--------------|----------|
| DuckDB | READ, AGGREGATE, FILTER | Local OLAP, development |
| Trino | READ, AGGREGATE, FILTER, TIME_TRAVEL | Distributed analytics |
| Spark | READ, AGGREGATE, TIME_TRAVEL | Large-scale batch |

---

### Step 7: Query Execution

The adapter translates and executes the query:

```go
// internal/adapters/trino/adapter.go
func (a *TrinoAdapter) Execute(ctx context.Context, sql string) (*Result, error) {
    // Translate SQL if needed (e.g., time-travel syntax)
    translated := a.translateTimeTravel(sql)
    
    // Execute against Trino
    rows, err := a.client.Query(translated)
    
    // Return standardized results
    return &Result{Columns: cols, Rows: rows}, nil
}
```

---

### Step 8: Response & Logging

Every query is logged with full context:

```go
// internal/observability/logger.go
type QueryLogEntry struct {
    Timestamp             time.Time
    QueryHash             string
    Tables                []string
    Role                  string
    AuthorizationDecision string  // "allowed" | "denied"
    PlannerDecision       string  // "trino" | "duckdb" | "rejected"
    Outcome               string  // "success" | "error"
    InvariantViolated     string  // e.g., "SNAPSHOT_CONSISTENT"
    Duration              time.Duration
}
```

---

## EXPLAIN CANONIC: Inspecting Decisions

Users can inspect routing decisions before execution:

```sql
EXPLAIN CANONIC
SELECT * FROM analytics.sales WHERE region = 'US'
```

**Response:**
```json
{
  "tables": ["analytics.sales"],
  "route": "trino",
  "reason": "table configured for trino engine",
  "will_refuse": false
}
```

If the query would be rejected:
```json
{
  "tables": ["analytics.sales"],
  "route": "",
  "reason": "SNAPSHOT_CONSISTENT table requires AS OF clause",
  "will_refuse": true
}
```

---

## Bootstrap & Configuration

### Initial Setup

```bash
# Generate example configuration
canonic bootstrap init --output canonic.yaml

# Validate configuration
canonic bootstrap validate --config canonic.yaml

# Apply to database (idempotent)
canonic bootstrap apply --config canonic.yaml
```

**Configuration File (canonic.yaml):**
```yaml
gateway:
  listen: ":8080"

database:
  host: "localhost"
  port: 5432
  name: "canonic"

engines:
  trino:
    host: "trino.cluster.local:8080"
  duckdb:
    enabled: true

roles:
  analyst:
    tables:
      - analytics.sales
      - analytics.customers
    capabilities: [READ, AGGREGATE]

tables:
  analytics.sales:
    engine: trino
    capabilities: [READ, AGGREGATE, FILTER]
    constraints: [SNAPSHOT_CONSISTENT]
  
  analytics.customers:
    engine: duckdb
    capabilities: [READ, FILTER]
```

---

## Operator Commands

### Health Check
```bash
canonic status
```
```
Gateway: healthy
Repository: connected (PostgreSQL 15.2)
Engines:
  - duckdb: ready
  - trino: ready
Config Version: 2026-01-16T10:30:00Z
```

### Audit Summary
```bash
canonic audit summary
```
```
Queries (last 24h):
  Accepted: 1,247
  Rejected: 23

Top Rejection Reasons:
  1. SNAPSHOT_CONSISTENT violation: 12
  2. Unauthorized table access: 8
  3. Unsupported syntax: 3

Top Queried Tables:
  1. analytics.sales: 523
  2. analytics.customers: 412
```

---

## Startup Sequence

```
┌─────────────────────────────────────────────────────────────────┐
│                      GATEWAY STARTUP                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Load configuration                                          │
│     └── Validate schema (strict mode)                           │
│                                                                 │
│  2. Connect to PostgreSQL                                       │
│     └── FAIL if unreachable (no fallback)                       │
│                                                                 │
│  3. Run migrations                                              │
│     └── FAIL if migration fails                                 │
│                                                                 │
│  4. Initialize engine adapters                                  │
│     └── FAIL if adapter registry empty                          │
│     └── FAIL if configured adapter unreachable                  │
│                                                                 │
│  5. Start HTTP server                                           │
│     └── /healthz returns 200                                    │
│     └── /readyz returns 200 (all checks pass)                   │
│                                                                 │
│  6. Accept queries                                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Non-Negotiable Invariants:**
- Gateway cannot start without PostgreSQL
- Gateway cannot start with zero adapters
- Gateway cannot route to unconfigured engines
- Gateway cannot accept queries until ready

---

## Error Handling Philosophy

Every error tells the user:
1. **What failed**
2. **Why it failed**
3. **How to fix it**

```go
// Good error
ErrSnapshotRequired = errors.New(
    "query rejected: table 'analytics.sales' has SNAPSHOT_CONSISTENT constraint; " +
    "add AS OF <timestamp> clause to query"
)

// Bad error (forbidden)
ErrGeneric = errors.New("query failed")
```

---

## Query Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           QUERY FLOW                                      │
└──────────────────────────────────────────────────────────────────────────┘

User Request
     │
     ▼
┌─────────────┐
│  CLI/HTTP   │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│   Parser    │────▶│ Extract tables, detect time-travel, check syntax    │
└──────┬──────┘     └─────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│    Auth     │────▶│ Role → Table → Capability (deny-by-default)         │
└──────┬──────┘     └─────────────────────────────────────────────────────┘
       │
       │ ┌──────────────────────┐
       │ │ Unauthorized?        │──────▶ Return 403 + reason
       │ └──────────────────────┘
       │
       ▼
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│  Resolver   │────▶│ Fetch table metadata from PostgreSQL                │
└──────┬──────┘     └─────────────────────────────────────────────────────┘
       │
       │ ┌──────────────────────┐
       │ │ Table not found?     │──────▶ Return 404 + reason
       │ └──────────────────────┘
       │
       ▼
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│   Planner   │────▶│ Check capabilities, enforce constraints             │
└──────┬──────┘     └─────────────────────────────────────────────────────┘
       │
       │ ┌──────────────────────┐
       │ │ Constraint violated? │──────▶ Return 400 + reason
       │ └──────────────────────┘
       │
       ▼
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│   Router    │────▶│ Select engine based on table config                 │
└──────┬──────┘     └─────────────────────────────────────────────────────┘
       │
       │ ┌──────────────────────┐
       │ │ No engine available? │──────▶ Return 503 + reason
       │ └──────────────────────┘
       │
       ▼
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│   Adapter   │────▶│ Translate SQL, execute on engine                    │
└──────┬──────┘     └─────────────────────────────────────────────────────┘
       │
       │ ┌──────────────────────┐
       │ │ Execution error?     │──────▶ Return 500 + reason
       │ └──────────────────────┘
       │
       ▼
┌─────────────┐
│   Logger    │────▶ Log query, decision, outcome
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Response   │────▶ Return results to user
└─────────────┘
```

---

## Current Status

| Component | Status |
|-----------|--------|
| SQL Parser (vitess) | ✅ Complete |
| Authorization | ✅ Complete |
| PostgreSQL Metadata | ✅ Wired |
| CLI → Gateway | ✅ Complete |
| DuckDB Adapter | ✅ Complete |
| Trino Adapter | ✅ Wired |
| Spark Adapter | ⚠️ Partial (code exists, not wired) |
| Bootstrap CLI | ✅ Complete |
| EXPLAIN CANONIC | ✅ Complete |
| Migrations | ✅ Complete |

**System is production-ready for read-only workloads with DuckDB and Trino.**
