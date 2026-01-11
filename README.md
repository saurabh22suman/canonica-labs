# Canonic-Labs

**Federated SQL Query Gateway** — Route queries to the right engine with access control, capability enforcement, and consistency guarantees.

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## What is Canonic?

Canonic is a **query gateway** that sits between your applications and multiple SQL engines. Instead of connecting directly to databases, applications send queries to Canonic, which:

1. **Parses** the SQL to understand what's being requested
2. **Authorizes** the request against role-based access control
3. **Routes** the query to the appropriate engine (Trino, DuckDB, Spark)
4. **Enforces** constraints like snapshot consistency
5. **Returns** results or clear rejection reasons

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐
│ Application │────▶│   Canonic   │────▶│  Trino / DuckDB / Spark │
└─────────────┘     │   Gateway   │     └─────────────────────────┘
                    └─────────────┘
```

---

## Quick Start

### Prerequisites

- Go 1.22+
- PostgreSQL 14+ (metadata store)
- Docker (optional, for engines)

### Installation

```bash
# Clone the repository
git clone https://github.com/canonic-labs/canonic-labs.git
cd canonic-labs

# Build
go build -o canonic ./cmd/canonic
go build -o canonic-gateway ./cmd/gateway

# Verify installation
./canonic version
```

### Start the Gateway

```bash
# Development mode (in-memory, DuckDB only)
./canonic-gateway -dev

# Production mode
./canonic-gateway \
  -postgres-url "postgres://user:pass@localhost:5432/canonic" \
  -trino-host "trino.cluster.local:8080"
```

### Run Your First Query

```bash
# Using the CLI
./canonic query run "SELECT 1 + 1 AS result"

# Using HTTP API
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 + 1 AS result"}'
```

---

## Configuration

### Bootstrap Configuration

Create a `canonic.yaml` file:

```yaml
gateway:
  listen: ":8080"

database:
  host: "localhost"
  port: 5432
  name: "canonic"
  user: "canonic"
  password: "${CANONIC_DB_PASSWORD}"

engines:
  duckdb:
    enabled: true
  trino:
    host: "trino.cluster.local:8080"

roles:
  analyst:
    tables:
      - analytics.sales
      - analytics.customers
    capabilities: [READ, AGGREGATE]
  
  data_engineer:
    tables:
      - analytics.*
    capabilities: [READ, AGGREGATE, FILTER]

tables:
  analytics.sales:
    engine: trino
    capabilities: [READ, AGGREGATE, FILTER]
    constraints: [SNAPSHOT_CONSISTENT]
  
  analytics.customers:
    engine: duckdb
    capabilities: [READ, FILTER]
```

Apply the configuration:

```bash
# Validate first
./canonic bootstrap validate --config canonic.yaml

# Apply to database
./canonic bootstrap apply --config canonic.yaml
```

---

## Usage

### CLI Commands

```bash
# Query execution
canonic query run "SELECT * FROM analytics.sales LIMIT 10"

# Explain routing decision (without executing)
canonic query explain "SELECT * FROM analytics.sales"

# Table management
canonic table list
canonic table describe analytics.sales

# Health check
canonic status

# Audit logs
canonic audit summary
canonic audit query --query-id abc123
```

### HTTP API

```bash
# Execute query
POST /query
Content-Type: application/json
Authorization: Bearer <token>

{"sql": "SELECT * FROM analytics.sales WHERE region = 'US'"}

# Explain routing
POST /explain
Content-Type: application/json

{"sql": "SELECT * FROM analytics.sales WHERE region = 'US'"}

# Health endpoints
GET /healthz   # Liveness probe
GET /readyz    # Readiness probe
```

### EXPLAIN CANONIC

Inspect routing decisions before execution:

```sql
EXPLAIN CANONIC
SELECT SUM(amount) FROM analytics.sales WHERE region = 'US'
```

Response:
```json
{
  "tables": ["analytics.sales"],
  "route": "trino",
  "reason": "table configured for trino engine",
  "capabilities_required": ["READ", "AGGREGATE", "FILTER"],
  "will_refuse": false
}
```

---

## Use Cases

### 1. Multi-Engine Analytics Platform

**Problem:** Your organization uses Trino for distributed queries, DuckDB for fast local analytics, and Spark for batch jobs. Each team connects to different engines directly, leading to inconsistent access control and no visibility into query patterns.

**Solution:** Canonic provides a single entry point with unified access control:

```yaml
tables:
  # Large tables → Trino
  analytics.events:
    engine: trino
    
  # Small lookup tables → DuckDB  
  analytics.regions:
    engine: duckdb
    
  # Historical batch data → Spark
  analytics.historical_sales:
    engine: spark
```

### 2. Enforcing Time-Travel Consistency

**Problem:** Your analytics pipeline requires reproducible queries. Analysts must always query data at a specific point in time, but they forget to add `AS OF` clauses.

**Solution:** Canonic enforces `SNAPSHOT_CONSISTENT` constraint:

```yaml
tables:
  analytics.sales:
    engine: trino
    constraints: [SNAPSHOT_CONSISTENT]
```

Now queries **must** include a timestamp:
```sql
-- This is rejected
SELECT * FROM analytics.sales

-- This works
SELECT * FROM analytics.sales FOR SYSTEM_TIME AS OF '2026-01-01'
```

### 3. Role-Based Query Governance

**Problem:** Different teams need different access levels. Finance can see revenue data, marketing cannot. Enforcing this at the database level is complex.

**Solution:** Canonic's deny-by-default authorization:

```yaml
roles:
  finance:
    tables: [analytics.revenue, analytics.sales]
    capabilities: [READ, AGGREGATE]
  
  marketing:
    tables: [analytics.campaigns, analytics.customers]
    capabilities: [READ]
```

### 4. Query Auditing & Compliance

**Problem:** Regulations require logging all access to sensitive data with full context.

**Solution:** Every query is logged with:
- User/role identity
- Tables accessed
- Authorization decision
- Routing decision
- Execution outcome

```bash
canonic audit summary --last 7d
```

### 5. Gradual Engine Migration

**Problem:** You're migrating from one engine to another but can't do it all at once.

**Solution:** Route tables to new engine incrementally:

```yaml
tables:
  # Already migrated
  analytics.sales:
    engine: trino
    
  # Still on old engine
  analytics.inventory:
    engine: duckdb
```

---

## Limitations

### Current Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **Read-only** | No INSERT, UPDATE, DELETE support | Use direct engine connections for writes |
| **Single-engine queries** | Cannot join tables from different engines | Pre-aggregate data or use same engine |
| **No query rewriting** | Queries pass through as-is (except time-travel) | Write engine-compatible SQL |
| **PostgreSQL required** | Metadata store must be PostgreSQL | No workaround; PostgreSQL is mandatory |

### SQL Syntax Restrictions

The following SQL features are **explicitly rejected**:

```sql
-- CTEs (Common Table Expressions)
WITH sales AS (SELECT ...) SELECT * FROM sales  ❌

-- Window functions
SELECT ROW_NUMBER() OVER (PARTITION BY region) ❌

-- Vendor hints
SELECT /*+ USE_HASH_JOIN */ * FROM sales       ❌

-- Non-qualified table names
SELECT * FROM sales                             ❌ (must be schema.table)
```

### Authorization Model Limitations

- **Deny-by-default only** — No explicit deny rules
- **Table-level granularity** — No column or row-level access control
- **Static roles** — Roles defined in config, not runtime

### Engine-Specific Limitations

| Engine | Limitation |
|--------|------------|
| DuckDB | Single-node only, no distributed queries |
| Trino | Requires external Trino cluster |
| Spark | Not fully wired (code exists, integration pending) |

### Operational Limitations

- **No query caching** — Every query hits the engine
- **No connection pooling** — New connection per query
- **No query timeout** — Relies on engine timeouts
- **Single gateway instance** — No built-in HA (use load balancer)

---

## Architecture

See [docs/user-flow.md](docs/user-flow.md) for detailed system architecture and query flow diagrams.

### Component Overview

| Component | Purpose |
|-----------|---------|
| `cmd/canonic` | CLI client |
| `cmd/gateway` | HTTP gateway server |
| `internal/sql` | SQL parsing (vitess/sqlparser) |
| `internal/auth` | Authorization service |
| `internal/planner` | Capability/constraint planning |
| `internal/router` | Engine routing |
| `internal/adapters` | Engine adapters (DuckDB, Trino, Spark) |
| `internal/storage` | PostgreSQL metadata repository |

---

## Development

### Running Tests

```bash
# All tests
go test ./...

# Red-flag tests (failure cases)
go test ./tests/redflag/...

# Green-flag tests (success cases)
go test ./tests/greenflag/...
```

### Docker Compose

```bash
# Start all services (PostgreSQL, Trino, Gateway)
docker-compose up -d

# Check logs
docker-compose logs -f gateway
```

### Project Structure

```
canonic-labs/
├── cmd/
│   ├── canonic/      # CLI
│   └── gateway/      # Gateway server
├── internal/
│   ├── adapters/     # Engine adapters
│   ├── auth/         # Authorization
│   ├── cli/          # CLI implementation
│   ├── config/       # Configuration
│   ├── gateway/      # Gateway logic
│   ├── planner/      # Query planning
│   ├── router/       # Engine routing
│   ├── sql/          # SQL parsing
│   └── storage/      # Metadata storage
├── migrations/       # Database migrations
├── tests/
│   ├── redflag/      # Failure tests
│   └── greenflag/    # Success tests
└── docs/             # Documentation
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Key principles:**
- All code follows Red-Flag / Green-Flag TDD (see [docs/test.md](docs/test.md))
- Copilot may assist, never decide (see [.github/copilot-instructions.md](.github/copilot-instructions.md))
- Human engineers own correctness, semantics, safety, and trust

---

## Roadmap

- [ ] Column-level access control
- [ ] Query caching layer
- [ ] Connection pooling
- [ ] Write support (INSERT, UPDATE)
- [ ] Spark adapter full integration
- [ ] Multi-gateway clustering
- [ ] Query cost estimation

---

## License

MIT License — See [LICENSE](LICENSE) for details.
