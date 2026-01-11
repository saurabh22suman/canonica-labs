# Phase 6 Specification – Real Engine Connectivity

## Status
**Authoritative**
This document is binding for Phase 6 work.

Phase 6 exists to make canonic-labs **connect to real query engines** (Trino, Spark) and execute queries against actual data in lakehouse formats (Iceberg, Delta, Hudi).

This phase optimizes for:
- real-world connectivity
- production reliability
- operational visibility

It does **not** optimize for cross-engine federation (Phase 9).

---

## Phase 6 Goals

Phase 6 addresses the following problems:

1. Trino adapter exists but is not tested with real clusters
2. Spark adapter exists but is not wired to gateway startup
3. No connection health monitoring or retry logic
4. No query timeout enforcement at gateway level
5. No integration test with real Iceberg/Delta data

After Phase 6:
> Canonic can route queries to a real Trino cluster and return results from Iceberg/Delta/Hudi tables.

---

## Prerequisites

Before starting Phase 6, ensure:
- Phases 1-5 invariants are passing
- PostgreSQL metadata repository is working
- Gateway startup sequence is correct
- `go test ./...` passes

---

## In-Scope Work (MANDATORY)

### 1. Trino Cluster Connectivity

#### Objective
Wire the existing Trino adapter to connect to a real Trino cluster and execute queries.

---

#### 1.1 Connection Establishment

The Trino adapter MUST:
- Connect to Trino coordinator via HTTP/HTTPS
- Support authentication (none, basic, JWT)
- Validate connection on startup
- Report connection status to health endpoints

**Configuration (config.yaml):**
```yaml
engines:
  trino:
    host: trino.cluster.local
    port: 8080
    catalog: iceberg        # default catalog
    schema: analytics       # default schema
    user: canonic           # query user
    ssl: false              # or: require
    auth:
      type: none            # none | basic | jwt
      # For basic:
      # username: canonic
      # password: ${TRINO_PASSWORD}
      # For jwt:
      # token: ${TRINO_JWT_TOKEN}
```

**Go Implementation:**
```go
// internal/adapters/trino/adapter.go

type AuthConfig struct {
    Type     string // "none", "basic", "jwt"
    Username string
    Password string
    Token    string
}

type AdapterConfig struct {
    Host     string
    Port     int
    Catalog  string
    Schema   string
    User     string
    SSL      bool
    Auth     AuthConfig
    
    // Connection settings
    ConnectTimeout time.Duration // Default: 10s
    QueryTimeout   time.Duration // Default: 5m
    MaxRetries     int           // Default: 3
}
```

---

#### 1.2 Connection Validation

On adapter initialization:
1. Attempt connection to Trino coordinator
2. Execute `SELECT 1` to verify connectivity
3. Return error if connection fails

**Startup Behavior:**
```
Gateway startup:
  1. Load config
  2. Connect to PostgreSQL ✓
  3. Initialize Trino adapter
     └── Connect to Trino coordinator
     └── Execute: SELECT 1
     └── FAIL startup if connection fails
  4. Register adapter
  5. Start HTTP server
```

**Required Method:**
```go
// CheckConnectivity verifies the Trino connection is alive.
// Returns nil if healthy, error with details if not.
func (a *Adapter) CheckConnectivity(ctx context.Context) error
```

---

#### 1.3 Red-Flag Tests (Required)

| Test | Scenario | Expected |
|------|----------|----------|
| `TestTrinoUnreachable` | Host does not exist | Startup fails with clear error |
| `TestTrinoAuthFailed` | Wrong credentials | Startup fails with auth error |
| `TestTrinoQueryTimeout` | Query exceeds timeout | Returns timeout error, cancels query |
| `TestTrinoConnectionLost` | Connection drops mid-query | Returns connection error |
| `TestTrinoInvalidCatalog` | Catalog does not exist | Returns catalog error |

---

#### 1.4 Green-Flag Tests (Required)

| Test | Scenario | Expected |
|------|----------|----------|
| `TestTrinoSimpleQuery` | SELECT 1 | Returns result |
| `TestTrinoTableQuery` | SELECT from real table | Returns rows |
| `TestTrinoWithTimeout` | Query within timeout | Completes normally |
| `TestTrinoReconnect` | Connection restored | Automatic recovery |

---

### 2. Connection Pooling & Health Checks

#### Objective
Implement robust connection management for production use.

---

#### 2.1 Connection Pool

The adapter MUST use connection pooling:

```go
// internal/adapters/trino/adapter.go

func NewAdapter(config AdapterConfig) (*Adapter, error) {
    db, err := sql.Open("trino", dsn)
    if err != nil {
        return nil, fmt.Errorf("trino: failed to open connection: %w", err)
    }
    
    // Pool configuration
    db.SetMaxOpenConns(10)           // Max concurrent connections
    db.SetMaxIdleConns(5)            // Keep 5 warm
    db.SetConnMaxLifetime(5 * time.Minute)
    db.SetConnMaxIdleTime(1 * time.Minute)
    
    return &Adapter{db: db, config: config}, nil
}
```

---

#### 2.2 Health Check Integration

Add Trino health to `/readyz` endpoint:

```json
GET /readyz

{
  "status": "ready",
  "checks": {
    "repository": "healthy",
    "adapters": {
      "duckdb": "healthy",
      "trino": "healthy"
    }
  }
}
```

If Trino is unhealthy:
```json
GET /readyz

{
  "status": "not_ready",
  "checks": {
    "repository": "healthy",
    "adapters": {
      "duckdb": "healthy",
      "trino": "unhealthy: connection refused"
    }
  }
}
```

**HTTP Status Codes:**
- 200: All adapters healthy
- 503: Any adapter unhealthy

---

#### 2.3 Health Check Method

```go
// internal/adapters/adapter.go

// EngineAdapter is the interface all adapters must implement.
type EngineAdapter interface {
    // Execute runs a query and returns results.
    Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error)
    
    // Name returns the adapter name (e.g., "trino", "duckdb").
    Name() string
    
    // Capabilities returns what this adapter can do.
    Capabilities() []capabilities.Capability
    
    // CheckHealth returns nil if adapter is healthy, error otherwise.
    CheckHealth(ctx context.Context) error
    
    // Close releases resources.
    Close() error
}
```

---

### 3. Spark Thrift Server Connectivity

#### Objective
Wire the Spark adapter to connect to Spark Thrift Server (HiveServer2 protocol).

---

#### 3.1 Spark Configuration

```yaml
engines:
  spark:
    host: spark-thrift.cluster.local
    port: 10000
    # Spark uses HiveServer2 protocol
    auth:
      type: none          # none | kerberos
    transport: binary     # binary | http
    
    # Connection settings
    connect_timeout: 30s
    query_timeout: 30m    # Spark queries are often long
```

---

#### 3.2 Spark Adapter Implementation

```go
// internal/adapters/spark/adapter.go

type AdapterConfig struct {
    Host           string
    Port           int
    AuthType       string        // "none", "kerberos"
    Transport      string        // "binary", "http"
    ConnectTimeout time.Duration
    QueryTimeout   time.Duration
}

func NewAdapter(config AdapterConfig) (*Adapter, error) {
    // Spark uses HiveServer2 protocol via go-hive driver
    dsn := fmt.Sprintf("hive://%s:%d/default", config.Host, config.Port)
    
    db, err := sql.Open("hive", dsn)
    if err != nil {
        return nil, fmt.Errorf("spark: failed to connect: %w", err)
    }
    
    return &Adapter{db: db, config: config}, nil
}
```

---

#### 3.3 Gateway Wiring

Add Spark adapter initialization to gateway startup:

```go
// cmd/gateway/main.go

func main() {
    // ... existing code ...
    
    // Initialize adapters
    adapters := make(map[string]adapters.EngineAdapter)
    
    // DuckDB (always available)
    adapters["duckdb"] = duckdb.NewAdapter(duckdbConfig)
    
    // Trino (if configured)
    if cfg.Engines.Trino.Host != "" {
        trinoAdapter, err := trino.NewAdapter(cfg.Engines.Trino)
        if err != nil {
            log.Fatalf("failed to initialize Trino: %v", err)
        }
        adapters["trino"] = trinoAdapter
    }
    
    // Spark (if configured)
    if cfg.Engines.Spark.Host != "" {
        sparkAdapter, err := spark.NewAdapter(cfg.Engines.Spark)
        if err != nil {
            log.Fatalf("failed to initialize Spark: %v", err)
        }
        adapters["spark"] = sparkAdapter
    }
}
```

---

### 4. Retry Logic with Exponential Backoff

#### Objective
Handle transient failures gracefully without silent fallbacks.

---

#### 4.1 Retry Policy

```go
// internal/adapters/retry.go

type RetryConfig struct {
    MaxRetries     int           // Default: 3
    InitialBackoff time.Duration // Default: 100ms
    MaxBackoff     time.Duration // Default: 5s
    Multiplier     float64       // Default: 2.0
    
    // RetryableErrors defines which errors trigger retry
    RetryableErrors []string     // e.g., "connection refused", "timeout"
}

func (c *RetryConfig) ShouldRetry(err error, attempt int) bool {
    if attempt >= c.MaxRetries {
        return false
    }
    
    errStr := err.Error()
    for _, retryable := range c.RetryableErrors {
        if strings.Contains(errStr, retryable) {
            return true
        }
    }
    return false
}

func (c *RetryConfig) Backoff(attempt int) time.Duration {
    backoff := float64(c.InitialBackoff) * math.Pow(c.Multiplier, float64(attempt))
    if backoff > float64(c.MaxBackoff) {
        backoff = float64(c.MaxBackoff)
    }
    return time.Duration(backoff)
}
```

---

#### 4.2 Retry Wrapper

```go
// internal/adapters/retry.go

func ExecuteWithRetry(
    ctx context.Context,
    adapter EngineAdapter,
    plan *planner.ExecutionPlan,
    config RetryConfig,
) (*QueryResult, error) {
    var lastErr error
    
    for attempt := 0; attempt <= config.MaxRetries; attempt++ {
        if attempt > 0 {
            // Log retry attempt
            log.Printf("retrying query (attempt %d/%d): %v", 
                attempt+1, config.MaxRetries+1, lastErr)
            
            // Wait with backoff
            select {
            case <-time.After(config.Backoff(attempt - 1)):
            case <-ctx.Done():
                return nil, fmt.Errorf("retry cancelled: %w", ctx.Err())
            }
        }
        
        result, err := adapter.Execute(ctx, plan)
        if err == nil {
            return result, nil
        }
        
        lastErr = err
        
        if !config.ShouldRetry(err, attempt) {
            return nil, err
        }
    }
    
    return nil, fmt.Errorf("query failed after %d retries: %w", 
        config.MaxRetries+1, lastErr)
}
```

---

#### 4.3 Retryable vs Non-Retryable Errors

| Retryable | Non-Retryable |
|-----------|---------------|
| Connection refused | Authentication failed |
| Connection reset | Invalid SQL syntax |
| Timeout (connect) | Table not found |
| Network unreachable | Permission denied |
| Service unavailable | Query cancelled by user |

**Invariant:** Authorization failures MUST NOT be retried.

---

### 5. Query Timeout & Cancellation

#### Objective
Enforce query timeouts at the gateway level and support cancellation.

---

#### 5.1 Timeout Configuration

```yaml
gateway:
  query_timeout: 5m        # Default query timeout
  connect_timeout: 10s     # Adapter connection timeout

engines:
  trino:
    query_timeout: 10m     # Override for Trino
  spark:
    query_timeout: 30m     # Spark queries can be long
```

---

#### 5.2 Timeout Enforcement

```go
// internal/gateway/gateway.go

func (g *Gateway) handleQuery(w http.ResponseWriter, r *http.Request) {
    // Get timeout (per-engine or default)
    timeout := g.config.QueryTimeout
    if engineTimeout := g.getEngineTimeout(plan.Engine); engineTimeout > 0 {
        timeout = engineTimeout
    }
    
    // Create timeout context
    ctx, cancel := context.WithTimeout(r.Context(), timeout)
    defer cancel()
    
    // Execute with timeout
    result, err := adapter.Execute(ctx, plan)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            // Log timeout
            g.logger.Warn("query timeout",
                "engine", plan.Engine,
                "timeout", timeout,
                "sql_hash", hashSQL(plan.LogicalPlan.RawSQL))
            
            writeError(w, http.StatusGatewayTimeout,
                fmt.Sprintf("query exceeded timeout of %v", timeout))
            return
        }
        // Handle other errors...
    }
}
```

---

#### 5.3 Cancellation Propagation

When a client disconnects, the query MUST be cancelled:

```go
// internal/adapters/trino/adapter.go

func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error) {
    // Start query
    rows, err := a.db.QueryContext(ctx, plan.LogicalPlan.RawSQL)
    if err != nil {
        return nil, fmt.Errorf("trino: query failed: %w", err)
    }
    defer rows.Close()
    
    // Check context during row iteration
    for rows.Next() {
        if err := ctx.Err(); err != nil {
            // Context cancelled - query will be cancelled on Trino
            return nil, fmt.Errorf("trino: query cancelled: %w", err)
        }
        // ... read row ...
    }
}
```

---

### 6. Integration Testing with Real Data

#### Objective
Validate end-to-end query execution with real Iceberg/Delta tables.

---

#### 6.1 Test Environment

**docker-compose.test.yaml:**
```yaml
version: '3.8'

services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"

  trino:
    image: trinodb/trino:latest
    ports:
      - "8080:8080"
    volumes:
      - ./trino-config:/etc/trino
    depends_on:
      - minio

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: canonic
      POSTGRES_PASSWORD: canonic
      POSTGRES_DB: canonic

  canonic-gateway:
    build: .
    ports:
      - "8081:8081"
    environment:
      CANONIC_POSTGRES_URL: postgres://canonic:canonic@postgres:5432/canonic
      CANONIC_TRINO_HOST: trino
      CANONIC_TRINO_PORT: 8080
    depends_on:
      - postgres
      - trino
```

---

#### 6.2 Trino Configuration for Iceberg

**trino-config/catalog/iceberg.properties:**
```properties
connector.name=iceberg
hive.metastore=file
hive.metastore.catalog.dir=s3a://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
```

---

#### 6.3 Integration Test

```go
// tests/integration/trino_iceberg_test.go

//go:build integration

package integration

import (
    "context"
    "testing"
    "time"
)

func TestTrinoIcebergEndToEnd(t *testing.T) {
    // Skip if not running integration tests
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    
    // Setup: Create Iceberg table via Trino
    _, err := trinoClient.Exec(ctx, `
        CREATE TABLE iceberg.analytics.test_orders (
            order_id BIGINT,
            customer_id BIGINT,
            amount DECIMAL(10,2),
            order_date DATE
        ) WITH (
            format = 'PARQUET'
        )
    `)
    require.NoError(t, err)
    
    // Insert test data
    _, err = trinoClient.Exec(ctx, `
        INSERT INTO iceberg.analytics.test_orders VALUES
        (1, 100, 99.99, DATE '2026-01-01'),
        (2, 101, 149.99, DATE '2026-01-02')
    `)
    require.NoError(t, err)
    
    // Register table in Canonic
    _, err = canonicClient.RegisterTable(ctx, RegisterTableRequest{
        Name:   "analytics.test_orders",
        Engine: "trino",
        Format: "iceberg",
    })
    require.NoError(t, err)
    
    // Query via Canonic gateway
    result, err := canonicClient.Query(ctx, "SELECT * FROM analytics.test_orders")
    require.NoError(t, err)
    
    // Verify results
    assert.Equal(t, 2, len(result.Rows))
    assert.Equal(t, int64(1), result.Rows[0]["order_id"])
}
```

---

#### 6.4 Required Test Cases

| Test | Description |
|------|-------------|
| `TestTrinoIcebergEndToEnd` | Create, insert, query Iceberg table |
| `TestTrinoDeltaEndToEnd` | Query Delta table via Trino Delta connector |
| `TestTrinoTimeTravel` | Query Iceberg table with AS OF |
| `TestTrinoParquetDirect` | Query Parquet files in S3 |
| `TestGatewayTrinoRouting` | Verify query routes to Trino |
| `TestGatewayTrinoHealthCheck` | Verify /readyz reports Trino status |
| `TestGatewayTrinoTimeout` | Verify timeout kills long query |
| `TestGatewayTrinoRetry` | Verify retry on transient failure |

---

## Out-of-Scope (Deferred to Later Phases)

| Item | Deferred To | Reason |
|------|-------------|--------|
| Catalog integration (Hive/Glue) | Phase 7 | Requires catalog client |
| Auto-discovery of tables | Phase 7 | Requires catalog sync |
| Cross-engine JOINs | Phase 9 | Requires query decomposition |
| Query result caching | Phase 9 | Performance optimization |
| Snowflake/BigQuery adapters | Phase 8 | Different protocol |

---

## Exit Criteria

Phase 6 is complete when:

1. **Trino Connectivity**
   - [ ] Trino adapter connects to real Trino cluster
   - [ ] Connection failure on startup prevents gateway from starting
   - [ ] Health check reports Trino status accurately

2. **Spark Connectivity**
   - [ ] Spark adapter connects to Spark Thrift Server
   - [ ] Spark adapter wired to gateway startup
   - [ ] Health check reports Spark status accurately

3. **Reliability**
   - [ ] Connection pooling configured correctly
   - [ ] Retry logic handles transient failures
   - [ ] Query timeout enforced at gateway level
   - [ ] Client disconnect cancels in-flight query

4. **Integration Tests**
   - [ ] End-to-end test with Trino + Iceberg passes
   - [ ] docker-compose test environment documented
   - [ ] All red-flag and green-flag tests pass

5. **Documentation**
   - [ ] Engine configuration documented
   - [ ] Troubleshooting guide for connection issues
   - [ ] Performance tuning recommendations

---

## Appendix A: Error Messages

All error messages MUST be actionable:

```go
// Good
"trino: connection failed: dial tcp trino.cluster.local:8080: connection refused; verify trino.host and trino.port configuration"

// Bad
"connection error"
```

---

## Appendix B: Metrics (Future)

Metrics to add in future phases:

| Metric | Type | Description |
|--------|------|-------------|
| `canonic_adapter_requests_total` | Counter | Total queries per adapter |
| `canonic_adapter_errors_total` | Counter | Errors per adapter |
| `canonic_adapter_latency_seconds` | Histogram | Query latency per adapter |
| `canonic_adapter_connection_pool_size` | Gauge | Current pool size |
| `canonic_adapter_health_status` | Gauge | 1=healthy, 0=unhealthy |

---

## Appendix C: Go Driver Dependencies

```go
// go.mod additions
require (
    github.com/trinodb/trino-go-client v0.313.0  // Trino
    github.com/apache/thrift v0.19.0             // For Spark/Hive
    github.com/beltran/gohive v1.6.0             // Spark Thrift
)
```

---

## Appendix D: Configuration Reference

```yaml
# Complete Phase 6 configuration example

gateway:
  listen: ":8080"
  query_timeout: 5m
  connect_timeout: 10s

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true
    
  trino:
    host: trino.cluster.local
    port: 8080
    catalog: iceberg
    schema: analytics
    user: canonic
    ssl: false
    auth:
      type: none
    query_timeout: 10m
    connect_timeout: 10s
    max_retries: 3
    pool:
      max_open: 10
      max_idle: 5
      conn_max_lifetime: 5m
      
  spark:
    host: spark-thrift.cluster.local
    port: 10000
    auth:
      type: none
    transport: binary
    query_timeout: 30m
    connect_timeout: 30s
    max_retries: 3
```
