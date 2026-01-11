# Phase 8 Specification – Multi-Format Query Support

## Status
**Authoritative**
This document is binding for Phase 8 work.

Phase 8 exists to make canonic-labs **transparently query different table formats** (Iceberg, Delta, Hudi) and **connect to cloud data warehouses** (Snowflake, BigQuery, Redshift) through a unified SQL interface.

This phase optimizes for:
- format transparency
- SQL normalization
- warehouse connectivity

It does **not** optimize for cross-engine federation (Phase 9).

---

## Phase 8 Goals

Phase 8 addresses the following problems:

1. Time-travel syntax differs between formats (Iceberg vs Delta vs Hudi)
2. Users must know which format a table uses to write correct SQL
3. DuckDB cannot query remote files (S3/GCS) without configuration
4. No connectivity to cloud warehouses (Snowflake, BigQuery, Redshift)
5. Capability mapping doesn't account for format-specific features

After Phase 8:
> Users write standard SQL and Canonic translates it to the correct format-specific syntax.

---

## Prerequisites

Before starting Phase 8, ensure:
- Phase 6 complete (Trino/Spark connectivity working)
- Phase 7 complete (catalog sync discovers table formats)
- Table metadata includes format information

---

## In-Scope Work (MANDATORY)

### 1. Unified Time-Travel Syntax

#### Objective
Define a single time-travel syntax that works across all formats.

---

#### 1.1 Canonic Time-Travel Syntax

Users write:
```sql
SELECT * FROM analytics.sales 
FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'
```

Or using snapshot ID:
```sql
SELECT * FROM analytics.sales 
FOR VERSION AS OF 12345678901234567890
```

---

#### 1.2 Format-Specific Translation

| Canonic Syntax | Iceberg (Trino) | Delta (Trino/Spark) | Hudi (Trino/Spark) |
|----------------|-----------------|---------------------|---------------------|
| `FOR SYSTEM_TIME AS OF 'ts'` | `FOR TIMESTAMP AS OF TIMESTAMP 'ts'` | `TIMESTAMP AS OF 'ts'` | `TIMESTAMP AS OF 'ts'` |
| `FOR VERSION AS OF id` | `FOR VERSION AS OF id` | `VERSION AS OF id` | N/A (use timestamp) |

---

#### 1.3 SQL Rewriter

```go
// internal/sql/rewriter.go

type TimeTravelRewriter struct {
    format TableFormat
    engine string
}

func (r *TimeTravelRewriter) Rewrite(sql string) (string, error) {
    // Parse SQL
    stmt, err := sqlparser.Parse(sql)
    if err != nil {
        return "", err
    }
    
    // Find time-travel clauses
    modified := sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
        switch node := cursor.Node().(type) {
        case *sqlparser.TableExpr:
            // Check for FOR SYSTEM_TIME AS OF
            if node.AsOf != nil {
                newExpr := r.translateTimeTravel(node, r.format, r.engine)
                cursor.Replace(newExpr)
            }
        }
        return true
    }, nil)
    
    return sqlparser.String(modified), nil
}

func (r *TimeTravelRewriter) translateTimeTravel(
    expr *sqlparser.TableExpr,
    format TableFormat,
    engine string,
) *sqlparser.TableExpr {
    switch format {
    case FormatIceberg:
        return r.translateIcebergTimeTravel(expr, engine)
    case FormatDelta:
        return r.translateDeltaTimeTravel(expr, engine)
    case FormatHudi:
        return r.translateHudiTimeTravel(expr, engine)
    default:
        return expr // No translation for unknown formats
    }
}
```

---

#### 1.4 Iceberg Time-Travel Translation

```go
// internal/sql/rewriter_iceberg.go

func (r *TimeTravelRewriter) translateIcebergTimeTravel(
    expr *sqlparser.TableExpr,
    engine string,
) *sqlparser.TableExpr {
    switch engine {
    case "trino":
        // Trino Iceberg: FOR TIMESTAMP AS OF TIMESTAMP '...'
        // Input:  SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01'
        // Output: SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP '2026-01-01'
        return rewriteAsOfClause(expr, "FOR TIMESTAMP AS OF TIMESTAMP")
        
    case "spark":
        // Spark Iceberg: TIMESTAMP AS OF '...'
        // Input:  SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01'
        // Output: SELECT * FROM t TIMESTAMP AS OF '2026-01-01'
        return rewriteAsOfClause(expr, "TIMESTAMP AS OF")
        
    default:
        return expr
    }
}

// For snapshot/version queries
func (r *TimeTravelRewriter) translateIcebergVersionTravel(
    expr *sqlparser.TableExpr,
    engine string,
) *sqlparser.TableExpr {
    switch engine {
    case "trino":
        // Trino: FOR VERSION AS OF <snapshot_id>
        return rewriteVersionClause(expr, "FOR VERSION AS OF")
        
    case "spark":
        // Spark: VERSION AS OF <snapshot_id>
        return rewriteVersionClause(expr, "VERSION AS OF")
        
    default:
        return expr
    }
}
```

---

#### 1.5 Delta Time-Travel Translation

```go
// internal/sql/rewriter_delta.go

func (r *TimeTravelRewriter) translateDeltaTimeTravel(
    expr *sqlparser.TableExpr,
    engine string,
) *sqlparser.TableExpr {
    switch engine {
    case "trino":
        // Trino Delta connector uses special syntax
        // Input:  SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01'
        // Output: SELECT * FROM t$history WHERE timestamp = '2026-01-01'
        // OR use the delta.history function
        return rewriteDeltaTimestamp(expr)
        
    case "spark":
        // Spark Delta: TIMESTAMP AS OF '...'
        // Input:  SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01'
        // Output: SELECT * FROM t TIMESTAMP AS OF '2026-01-01'
        return rewriteAsOfClause(expr, "TIMESTAMP AS OF")
        
    default:
        return expr
    }
}

func (r *TimeTravelRewriter) translateDeltaVersionTravel(
    expr *sqlparser.TableExpr,
    engine string,
) *sqlparser.TableExpr {
    switch engine {
    case "spark":
        // Spark Delta: VERSION AS OF <version>
        return rewriteVersionClause(expr, "VERSION AS OF")
        
    case "trino":
        // Trino Delta: different approach needed
        return rewriteDeltaVersion(expr)
        
    default:
        return expr
    }
}
```

---

#### 1.6 Hudi Time-Travel Translation

```go
// internal/sql/rewriter_hudi.go

func (r *TimeTravelRewriter) translateHudiTimeTravel(
    expr *sqlparser.TableExpr,
    engine string,
) *sqlparser.TableExpr {
    // Hudi uses timestamp-based queries with special suffix
    switch engine {
    case "trino":
        // Trino Hudi: use table suffix
        // Input:  SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01'
        // Output: SELECT * FROM "t$timeline" WHERE timestamp <= '2026-01-01'
        return rewriteHudiTimestamp(expr)
        
    case "spark":
        // Spark Hudi: use read options
        // This requires query restructuring or config
        return rewriteHudiSparkTimestamp(expr)
        
    default:
        return expr
    }
}
```

---

#### 1.7 Red-Flag Tests (Required)

| Test | Scenario | Expected |
|------|----------|----------|
| `TestTimeTravelInvalidFormat` | Unknown timestamp format | Clear parse error |
| `TestTimeTravelFutureDate` | Timestamp in the future | Rejection with reason |
| `TestTimeTravelUnsupportedFormat` | Hudi + VERSION AS OF | Rejection: not supported |
| `TestTimeTravelMixedFormats` | Join tables with different formats | Rejection: cannot mix |

---

#### 1.8 Green-Flag Tests (Required)

| Test | Scenario | Expected |
|------|----------|----------|
| `TestIcebergTimeTravelTrino` | SYSTEM_TIME on Iceberg/Trino | Correct translation |
| `TestIcebergTimeTravelSpark` | SYSTEM_TIME on Iceberg/Spark | Correct translation |
| `TestDeltaTimeTravelSpark` | SYSTEM_TIME on Delta/Spark | Correct translation |
| `TestVersionTravelIceberg` | VERSION AS OF on Iceberg | Correct translation |

---

### 2. Format-Aware Capability Mapping

#### Objective
Map capabilities based on table format, not just engine.

---

#### 2.1 Format Capabilities

```go
// internal/capabilities/format.go

// FormatCapabilities defines what each format supports.
var FormatCapabilities = map[TableFormat][]Capability{
    FormatIceberg: {
        CapabilityRead,
        CapabilityAggregate,
        CapabilityFilter,
        CapabilityTimeTravel,
        CapabilitySnapshotQuery,      // Query specific snapshot
        CapabilitySchemaEvolution,    // Read old schemas
        CapabilityPartitionPruning,
    },
    FormatDelta: {
        CapabilityRead,
        CapabilityAggregate,
        CapabilityFilter,
        CapabilityTimeTravel,
        CapabilityVersionQuery,       // Query specific version
        CapabilitySchemaEvolution,
        CapabilityPartitionPruning,
    },
    FormatHudi: {
        CapabilityRead,
        CapabilityAggregate,
        CapabilityFilter,
        CapabilityTimeTravel,         // Limited
        CapabilityIncrementalQuery,   // Read changes since timestamp
        CapabilityPartitionPruning,
    },
    FormatParquet: {
        CapabilityRead,
        CapabilityAggregate,
        CapabilityFilter,
        // No time-travel for raw Parquet
    },
}
```

---

#### 2.2 Capability Validation

```go
// internal/planner/planner.go

func (p *Planner) validateFormatCapabilities(
    query *ParsedQuery,
    table *TableMetadata,
) error {
    format := table.Format
    formatCaps := FormatCapabilities[format]
    
    // Check time-travel capability
    if query.HasTimeTravel() {
        if !hasCapability(formatCaps, CapabilityTimeTravel) {
            return fmt.Errorf(
                "table %q (format: %s) does not support time-travel queries",
                table.Name, format)
        }
    }
    
    // Check version query capability
    if query.HasVersionQuery() {
        if !hasCapability(formatCaps, CapabilityVersionQuery) {
            return fmt.Errorf(
                "table %q (format: %s) does not support VERSION AS OF queries; "+
                    "use SYSTEM_TIME AS OF instead",
                table.Name, format)
        }
    }
    
    return nil
}
```

---

### 3. DuckDB Remote File Access

#### Objective
Enable DuckDB to query Parquet/CSV files directly from S3/GCS/Azure.

---

#### 3.1 DuckDB Extensions

```go
// internal/adapters/duckdb/adapter.go

func (a *Adapter) initializeExtensions(ctx context.Context) error {
    // Install and load required extensions
    extensions := []string{
        "httpfs",     // HTTP/S3/GCS file access
        "parquet",    // Parquet support
        "json",       // JSON support
        "aws",        // AWS credentials
    }
    
    for _, ext := range extensions {
        if _, err := a.db.ExecContext(ctx, 
            fmt.Sprintf("INSTALL %s; LOAD %s;", ext, ext)); err != nil {
            return fmt.Errorf("duckdb: failed to load extension %s: %w", ext, err)
        }
    }
    
    return nil
}
```

---

#### 3.2 S3 Configuration

```go
// internal/adapters/duckdb/adapter.go

func (a *Adapter) configureS3(ctx context.Context, config S3Config) error {
    // Configure S3 access
    queries := []string{
        fmt.Sprintf("SET s3_region='%s';", config.Region),
        fmt.Sprintf("SET s3_access_key_id='%s';", config.AccessKeyID),
        fmt.Sprintf("SET s3_secret_access_key='%s';", config.SecretAccessKey),
    }
    
    if config.Endpoint != "" {
        // For MinIO or other S3-compatible stores
        queries = append(queries,
            fmt.Sprintf("SET s3_endpoint='%s';", config.Endpoint),
            "SET s3_url_style='path';",
        )
    }
    
    if config.SessionToken != "" {
        queries = append(queries,
            fmt.Sprintf("SET s3_session_token='%s';", config.SessionToken),
        )
    }
    
    for _, q := range queries {
        if _, err := a.db.ExecContext(ctx, q); err != nil {
            return fmt.Errorf("duckdb: failed to configure S3: %w", err)
        }
    }
    
    return nil
}
```

---

#### 3.3 GCS Configuration

```go
// internal/adapters/duckdb/adapter.go

func (a *Adapter) configureGCS(ctx context.Context, config GCSConfig) error {
    // DuckDB uses the gcs:// scheme
    queries := []string{
        "INSTALL gcs; LOAD gcs;",
    }
    
    if config.ServiceAccountJSON != "" {
        // Set credentials from service account
        queries = append(queries,
            fmt.Sprintf("SET gcs_service_account_json='%s';", 
                escapeSQL(config.ServiceAccountJSON)),
        )
    }
    
    for _, q := range queries {
        if _, err := a.db.ExecContext(ctx, q); err != nil {
            return fmt.Errorf("duckdb: failed to configure GCS: %w", err)
        }
    }
    
    return nil
}
```

---

#### 3.4 Direct File Query

```go
// internal/adapters/duckdb/adapter.go

func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error) {
    sql := plan.LogicalPlan.RawSQL
    
    // Check if query references a file-based table
    for _, table := range plan.Tables {
        if table.Location != "" && isRemoteLocation(table.Location) {
            // Rewrite table reference to direct file path
            sql = a.rewriteTableToLocation(sql, table.Name, table.Location)
        }
    }
    
    return a.executeSQL(ctx, sql)
}

func (a *Adapter) rewriteTableToLocation(sql, tableName, location string) string {
    // Replace table name with read_parquet() or appropriate function
    // Input:  SELECT * FROM analytics.events
    // Output: SELECT * FROM read_parquet('s3://bucket/events/*.parquet')
    
    pattern := regexp.MustCompile(fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(tableName)))
    
    // Determine file format from location
    reader := "read_parquet"
    if strings.HasSuffix(location, ".csv") {
        reader = "read_csv_auto"
    } else if strings.HasSuffix(location, ".json") {
        reader = "read_json_auto"
    }
    
    // Handle glob patterns
    filePattern := location
    if !strings.Contains(location, "*") {
        filePattern = filepath.Join(location, "*.parquet")
    }
    
    replacement := fmt.Sprintf("%s('%s')", reader, filePattern)
    return pattern.ReplaceAllString(sql, replacement)
}
```

---

#### 3.5 Configuration

```yaml
engines:
  duckdb:
    enabled: true
    
    # Cloud storage access
    s3:
      region: us-east-1
      access_key_id: ${AWS_ACCESS_KEY_ID}
      secret_access_key: ${AWS_SECRET_ACCESS_KEY}
      # For S3-compatible (MinIO, etc.)
      # endpoint: http://minio:9000
      
    gcs:
      service_account_json: ${GCS_SERVICE_ACCOUNT_JSON}
      
    azure:
      storage_account_name: ${AZURE_STORAGE_ACCOUNT}
      storage_account_key: ${AZURE_STORAGE_KEY}
```

---

### 4. Snowflake Adapter

#### Objective
Connect to Snowflake data warehouse for query execution.

---

#### 4.1 Snowflake Client

```go
// internal/adapters/snowflake/adapter.go

type Config struct {
    // Account is the Snowflake account identifier.
    // Format: <account>.<region>.snowflakecomputing.com
    Account string
    
    // User is the Snowflake username.
    User string
    
    // Password for basic auth (or use key-pair).
    Password string
    
    // PrivateKey for key-pair authentication (PEM format).
    PrivateKey string
    
    // Database is the default database.
    Database string
    
    // Schema is the default schema.
    Schema string
    
    // Warehouse is the compute warehouse.
    Warehouse string
    
    // Role is the Snowflake role.
    Role string
    
    // Connection settings
    ConnectTimeout time.Duration
    QueryTimeout   time.Duration
}

type Adapter struct {
    config Config
    db     *sql.DB
}

func NewAdapter(config Config) (*Adapter, error) {
    // Build DSN
    dsn := fmt.Sprintf(
        "%s:%s@%s/%s/%s?warehouse=%s&role=%s",
        config.User,
        config.Password,
        config.Account,
        config.Database,
        config.Schema,
        config.Warehouse,
        config.Role,
    )
    
    db, err := sql.Open("snowflake", dsn)
    if err != nil {
        return nil, fmt.Errorf("snowflake: failed to connect: %w", err)
    }
    
    // Test connection
    ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
    defer cancel()
    
    if err := db.PingContext(ctx); err != nil {
        return nil, fmt.Errorf("snowflake: connection test failed: %w", err)
    }
    
    return &Adapter{
        config: config,
        db:     db,
    }, nil
}
```

---

#### 4.2 Snowflake Query Execution

```go
// internal/adapters/snowflake/adapter.go

func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error) {
    if plan == nil || plan.LogicalPlan == nil {
        return nil, fmt.Errorf("snowflake: execution plan is nil")
    }
    
    sql := plan.LogicalPlan.RawSQL
    
    // Rewrite time-travel if needed
    if plan.HasTimeTravel {
        sql = a.rewriteTimeTravel(sql, plan)
    }
    
    // Execute with timeout
    rows, err := a.db.QueryContext(ctx, sql)
    if err != nil {
        return nil, fmt.Errorf("snowflake: query failed: %w", err)
    }
    defer rows.Close()
    
    return a.collectResults(rows)
}

func (a *Adapter) rewriteTimeTravel(sql string, plan *planner.ExecutionPlan) string {
    // Snowflake time-travel: AT(TIMESTAMP => 'ts') or AT(OFFSET => -3600)
    // Input:  SELECT * FROM t FOR SYSTEM_TIME AS OF '2026-01-01'
    // Output: SELECT * FROM t AT(TIMESTAMP => '2026-01-01'::TIMESTAMP)
    
    for _, tt := range plan.TimeTravelClauses {
        oldPattern := fmt.Sprintf("FOR SYSTEM_TIME AS OF '%s'", tt.Timestamp)
        newPattern := fmt.Sprintf("AT(TIMESTAMP => '%s'::TIMESTAMP)", tt.Timestamp)
        sql = strings.Replace(sql, oldPattern, newPattern, 1)
    }
    
    return sql
}
```

---

#### 4.3 Snowflake Capabilities

```go
// internal/adapters/snowflake/adapter.go

func (a *Adapter) Capabilities() []capabilities.Capability {
    return []capabilities.Capability{
        capabilities.CapabilityRead,
        capabilities.CapabilityAggregate,
        capabilities.CapabilityFilter,
        capabilities.CapabilityTimeTravel,  // Snowflake supports up to 90 days
        capabilities.CapabilityWindow,      // Window functions supported
        capabilities.CapabilityCTE,         // CTEs supported
    }
}
```

---

#### 4.4 Configuration

```yaml
engines:
  snowflake:
    account: xy12345.us-east-1
    user: canonic_user
    password: ${SNOWFLAKE_PASSWORD}
    database: ANALYTICS
    schema: PUBLIC
    warehouse: COMPUTE_WH
    role: CANONIC_ROLE
    
    connect_timeout: 30s
    query_timeout: 5m
```

---

### 5. BigQuery Adapter

#### Objective
Connect to Google BigQuery for query execution.

---

#### 5.1 BigQuery Client

```go
// internal/adapters/bigquery/adapter.go

type Config struct {
    // ProjectID is the GCP project ID.
    ProjectID string
    
    // CredentialsJSON is the service account key (optional if using ADC).
    CredentialsJSON string
    
    // Location is the BigQuery region (e.g., "US", "EU").
    Location string
    
    // DefaultDataset is the default dataset for unqualified tables.
    DefaultDataset string
    
    QueryTimeout time.Duration
}

type Adapter struct {
    config Config
    client *bigquery.Client
}

func NewAdapter(ctx context.Context, config Config) (*Adapter, error) {
    var opts []option.ClientOption
    
    if config.CredentialsJSON != "" {
        opts = append(opts, option.WithCredentialsJSON([]byte(config.CredentialsJSON)))
    }
    
    client, err := bigquery.NewClient(ctx, config.ProjectID, opts...)
    if err != nil {
        return nil, fmt.Errorf("bigquery: failed to create client: %w", err)
    }
    
    return &Adapter{
        config: config,
        client: client,
    }, nil
}
```

---

#### 5.2 BigQuery Query Execution

```go
// internal/adapters/bigquery/adapter.go

func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error) {
    sql := plan.LogicalPlan.RawSQL
    
    // Rewrite time-travel if needed
    if plan.HasTimeTravel {
        sql = a.rewriteTimeTravel(sql, plan)
    }
    
    // Create query
    query := a.client.Query(sql)
    query.DefaultDatasetID = a.config.DefaultDataset
    query.Location = a.config.Location
    
    // Run query
    job, err := query.Run(ctx)
    if err != nil {
        return nil, fmt.Errorf("bigquery: failed to start query: %w", err)
    }
    
    // Wait for completion
    status, err := job.Wait(ctx)
    if err != nil {
        return nil, fmt.Errorf("bigquery: query failed: %w", err)
    }
    if status.Err() != nil {
        return nil, fmt.Errorf("bigquery: query error: %w", status.Err())
    }
    
    // Read results
    iter, err := job.Read(ctx)
    if err != nil {
        return nil, fmt.Errorf("bigquery: failed to read results: %w", err)
    }
    
    return a.collectResults(iter)
}

func (a *Adapter) rewriteTimeTravel(sql string, plan *planner.ExecutionPlan) string {
    // BigQuery time-travel: FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    // OR: FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01 00:00:00'
    // 
    // Our syntax already matches BigQuery's native syntax!
    // Just ensure timestamp format is correct
    
    for _, tt := range plan.TimeTravelClauses {
        // BigQuery expects: FOR SYSTEM_TIME AS OF TIMESTAMP '...'
        oldPattern := fmt.Sprintf("FOR SYSTEM_TIME AS OF '%s'", tt.Timestamp)
        newPattern := fmt.Sprintf("FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", tt.Timestamp)
        sql = strings.Replace(sql, oldPattern, newPattern, 1)
    }
    
    return sql
}
```

---

#### 5.3 Configuration

```yaml
engines:
  bigquery:
    project_id: my-gcp-project
    credentials_json: ${GOOGLE_APPLICATION_CREDENTIALS_JSON}
    location: US
    default_dataset: analytics
    
    query_timeout: 5m
```

---

### 6. Redshift Adapter

#### Objective
Connect to Amazon Redshift for query execution.

---

#### 6.1 Redshift Client

```go
// internal/adapters/redshift/adapter.go

type Config struct {
    // Host is the Redshift cluster endpoint.
    Host string
    
    // Port is the Redshift port (default 5439).
    Port int
    
    // Database is the Redshift database name.
    Database string
    
    // User is the database user.
    User string
    
    // Password is the database password.
    Password string
    
    // SSLMode controls SSL: disable, require, verify-ca, verify-full
    SSLMode string
    
    // IAM Auth (alternative to password)
    UseIAMAuth    bool
    AWSRegion     string
    ClusterID     string
    
    ConnectTimeout time.Duration
    QueryTimeout   time.Duration
}

type Adapter struct {
    config Config
    db     *sql.DB
}

func NewAdapter(ctx context.Context, config Config) (*Adapter, error) {
    var dsn string
    
    if config.UseIAMAuth {
        // Use IAM-based authentication
        token, err := getIAMToken(ctx, config)
        if err != nil {
            return nil, fmt.Errorf("redshift: IAM auth failed: %w", err)
        }
        dsn = fmt.Sprintf(
            "host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
            config.Host, config.Port, config.Database,
            config.User, token, config.SSLMode,
        )
    } else {
        dsn = fmt.Sprintf(
            "host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
            config.Host, config.Port, config.Database,
            config.User, config.Password, config.SSLMode,
        )
    }
    
    db, err := sql.Open("postgres", dsn) // Redshift uses postgres driver
    if err != nil {
        return nil, fmt.Errorf("redshift: failed to connect: %w", err)
    }
    
    // Test connection
    if err := db.PingContext(ctx); err != nil {
        return nil, fmt.Errorf("redshift: connection test failed: %w", err)
    }
    
    return &Adapter{
        config: config,
        db:     db,
    }, nil
}
```

---

#### 6.2 Redshift Limitations

Redshift does NOT support time-travel. Handle this gracefully:

```go
// internal/adapters/redshift/adapter.go

func (a *Adapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*QueryResult, error) {
    // Redshift does not support time-travel
    if plan.HasTimeTravel {
        return nil, fmt.Errorf(
            "redshift: time-travel queries are not supported; " +
            "Redshift does not have built-in time-travel capability")
    }
    
    return a.executeSQL(ctx, plan.LogicalPlan.RawSQL)
}

func (a *Adapter) Capabilities() []capabilities.Capability {
    return []capabilities.Capability{
        capabilities.CapabilityRead,
        capabilities.CapabilityAggregate,
        capabilities.CapabilityFilter,
        capabilities.CapabilityWindow,
        // NO CapabilityTimeTravel
    }
}
```

---

#### 6.3 Configuration

```yaml
engines:
  redshift:
    host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
    port: 5439
    database: analytics
    user: canonic_user
    password: ${REDSHIFT_PASSWORD}
    ssl_mode: require
    
    # OR use IAM auth
    # use_iam_auth: true
    # aws_region: us-east-1
    # cluster_id: my-cluster
    
    connect_timeout: 30s
    query_timeout: 5m
```

---

### 7. Engine Selection Logic

#### Objective
Intelligently route queries to the best available engine.

---

#### 7.1 Engine Selection Rules

```go
// internal/router/selector.go

type EngineSelector struct {
    adapters map[string]adapters.EngineAdapter
    tables   storage.TableRepository
}

func (s *EngineSelector) SelectEngine(ctx context.Context, plan *planner.ExecutionPlan) (string, error) {
    // Rule 1: If table has explicit engine assignment, use it
    for _, table := range plan.Tables {
        if table.Engine != "" {
            return table.Engine, nil
        }
    }
    
    // Rule 2: Select based on format capabilities
    format := plan.Tables[0].Format
    requiredCaps := plan.RequiredCapabilities()
    
    candidates := s.findCapableEngines(format, requiredCaps)
    if len(candidates) == 0 {
        return "", fmt.Errorf(
            "no engine available for format %s with capabilities %v",
            format, requiredCaps)
    }
    
    // Rule 3: Prefer engine by format
    preferred := s.preferredEngineForFormat(format)
    if contains(candidates, preferred) {
        return preferred, nil
    }
    
    // Rule 4: Use first available
    return candidates[0], nil
}

func (s *EngineSelector) preferredEngineForFormat(format TableFormat) string {
    switch format {
    case FormatIceberg:
        return "trino"  // Trino has best Iceberg support
    case FormatDelta:
        return "spark"  // Spark has native Delta support
    case FormatHudi:
        return "spark"  // Hudi is Spark-native
    case FormatParquet:
        return "duckdb" // DuckDB is fast for raw Parquet
    default:
        return "duckdb"
    }
}
```

---

#### 7.2 Multi-Table Engine Resolution

```go
// internal/router/selector.go

func (s *EngineSelector) SelectEngineForMultiTable(
    ctx context.Context,
    plan *planner.ExecutionPlan,
) (string, error) {
    // All tables must use the same engine (Phase 8 limitation)
    // Cross-engine queries deferred to Phase 9
    
    engines := make(map[string]bool)
    for _, table := range plan.Tables {
        engine, err := s.SelectEngine(ctx, &planner.ExecutionPlan{
            Tables: []*TableMetadata{table},
        })
        if err != nil {
            return "", err
        }
        engines[engine] = true
    }
    
    if len(engines) > 1 {
        var engineList []string
        for e := range engines {
            engineList = append(engineList, e)
        }
        return "", fmt.Errorf(
            "query spans multiple engines (%s); "+
                "cross-engine queries not yet supported",
            strings.Join(engineList, ", "))
    }
    
    // Return the single engine
    for e := range engines {
        return e, nil
    }
    
    return "", fmt.Errorf("no engine available for query")
}
```

---

## Out-of-Scope (Deferred to Later Phases)

| Item | Deferred To | Reason |
|------|-------------|--------|
| Cross-engine JOINs | Phase 9 | Requires query decomposition |
| Query result caching | Phase 9 | Performance optimization |
| Write operations (INSERT/UPDATE) | Future | Read-only for now |
| Custom SQL dialects | Future | Standard SQL first |

---

## Exit Criteria

Phase 8 is complete when:

1. **Time-Travel Normalization**
   - [ ] `FOR SYSTEM_TIME AS OF` works for Iceberg on Trino/Spark
   - [ ] `FOR SYSTEM_TIME AS OF` works for Delta on Spark
   - [ ] `FOR VERSION AS OF` works for Iceberg on Trino
   - [ ] Unsupported time-travel combinations rejected with clear error

2. **Format Capabilities**
   - [ ] Format-specific capabilities enforced
   - [ ] Hudi VERSION AS OF rejected with helpful message
   - [ ] Raw Parquet time-travel rejected

3. **DuckDB Remote Access**
   - [ ] DuckDB can query S3 Parquet files
   - [ ] DuckDB can query GCS Parquet files
   - [ ] S3/GCS credentials configured securely

4. **Warehouse Adapters**
   - [ ] Snowflake adapter connects and executes queries
   - [ ] BigQuery adapter connects and executes queries
   - [ ] Redshift adapter connects and executes queries
   - [ ] All adapters report health correctly

5. **Engine Selection**
   - [ ] Queries routed to correct engine by format
   - [ ] Multi-table queries require same engine
   - [ ] Clear error when no engine can handle query

6. **Tests**
   - [ ] All red-flag and green-flag tests pass
   - [ ] Integration tests with real warehouses (optional)

---

## Appendix A: Time-Travel Syntax Reference

| Format | Engine | Timestamp Syntax | Version Syntax |
|--------|--------|------------------|----------------|
| Iceberg | Trino | `FOR TIMESTAMP AS OF TIMESTAMP 'ts'` | `FOR VERSION AS OF id` |
| Iceberg | Spark | `TIMESTAMP AS OF 'ts'` | `VERSION AS OF id` |
| Delta | Spark | `TIMESTAMP AS OF 'ts'` | `VERSION AS OF v` |
| Delta | Trino | Connector-specific | Connector-specific |
| Hudi | Spark | Read options | N/A |
| Snowflake | - | `AT(TIMESTAMP => 'ts')` | N/A |
| BigQuery | - | `FOR SYSTEM_TIME AS OF TIMESTAMP 'ts'` | N/A |
| Redshift | - | Not supported | Not supported |

---

## Appendix B: Go Dependencies

```go
// go.mod additions
require (
    github.com/snowflakedb/gosnowflake v1.7.0    // Snowflake
    cloud.google.com/go/bigquery v1.57.0         // BigQuery
    github.com/lib/pq v1.10.9                    // Redshift (postgres)
    github.com/aws/aws-sdk-go-v2 v1.24.0         // AWS SDK
)
```

---

## Appendix C: Configuration Reference

```yaml
# Complete Phase 8 configuration example

engines:
  duckdb:
    enabled: true
    s3:
      region: us-east-1
      access_key_id: ${AWS_ACCESS_KEY_ID}
      secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    gcs:
      service_account_json: ${GCS_SERVICE_ACCOUNT_JSON}
      
  trino:
    host: trino.cluster.local
    port: 8080
    catalog: iceberg
    
  spark:
    host: spark-thrift.cluster.local
    port: 10000
    
  snowflake:
    account: xy12345.us-east-1
    user: canonic_user
    password: ${SNOWFLAKE_PASSWORD}
    database: ANALYTICS
    warehouse: COMPUTE_WH
    
  bigquery:
    project_id: my-gcp-project
    location: US
    default_dataset: analytics
    
  redshift:
    host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
    port: 5439
    database: analytics
    user: canonic_user
    password: ${REDSHIFT_PASSWORD}
```
