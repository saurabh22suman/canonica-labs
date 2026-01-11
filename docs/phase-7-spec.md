# Phase 7 Specification – Catalog Integration & Table Discovery

## Status
**Authoritative**
This document is binding for Phase 7 work.

Phase 7 exists to make canonic-labs **automatically discover tables** from external catalogs (Hive Metastore, AWS Glue, Databricks Unity Catalog) instead of requiring manual registration.

This phase optimizes for:
- operational simplicity
- catalog synchronization
- format detection

It does **not** optimize for cross-engine query execution (Phase 9).

---

## Phase 7 Goals

Phase 7 addresses the following problems:

1. Tables must be manually registered in Canonic
2. No integration with existing metadata catalogs
3. Table format (Iceberg/Delta/Hudi) must be specified manually
4. Schema changes in source tables are not reflected
5. Large organizations have thousands of tables to manage

After Phase 7:
> Canonic automatically discovers and syncs tables from Hive Metastore, AWS Glue, or Unity Catalog.

---

## Prerequisites

Before starting Phase 7, ensure:
- Phase 6 complete (real engine connectivity working)
- Trino adapter can query Iceberg/Delta tables
- Gateway health checks include adapter status

---

## In-Scope Work (MANDATORY)

### 1. Catalog Client Interface

#### Objective
Define a unified interface for interacting with different metadata catalogs.

---

#### 1.1 Catalog Interface Definition

```go
// internal/catalog/catalog.go

// Catalog represents an external metadata catalog.
type Catalog interface {
    // Name returns the catalog identifier (e.g., "hive", "glue", "unity").
    Name() string
    
    // ListDatabases returns all databases/schemas in the catalog.
    ListDatabases(ctx context.Context) ([]string, error)
    
    // ListTables returns all tables in a database.
    ListTables(ctx context.Context, database string) ([]TableInfo, error)
    
    // GetTable returns detailed metadata for a specific table.
    GetTable(ctx context.Context, database, table string) (*TableMetadata, error)
    
    // CheckConnectivity verifies the catalog is reachable.
    CheckConnectivity(ctx context.Context) error
    
    // Close releases resources.
    Close() error
}

// TableInfo is a lightweight table reference.
type TableInfo struct {
    Database string
    Name     string
    Format   TableFormat  // ICEBERG, DELTA, HUDI, PARQUET, etc.
}

// TableMetadata is detailed table information.
type TableMetadata struct {
    Database    string
    Name        string
    Format      TableFormat
    Location    string           // s3://bucket/path or hdfs://path
    Columns     []ColumnMetadata
    Partitions  []string
    Properties  map[string]string
    CreatedAt   time.Time
    UpdatedAt   time.Time
}

// ColumnMetadata describes a table column.
type ColumnMetadata struct {
    Name     string
    Type     string   // Trino/Spark type string
    Nullable bool
    Comment  string
}

// TableFormat identifies the table format.
type TableFormat string

const (
    FormatIceberg TableFormat = "iceberg"
    FormatDelta   TableFormat = "delta"
    FormatHudi    TableFormat = "hudi"
    FormatParquet TableFormat = "parquet"
    FormatORC     TableFormat = "orc"
    FormatCSV     TableFormat = "csv"
    FormatUnknown TableFormat = "unknown"
)
```

---

### 2. Hive Metastore Integration

#### Objective
Connect to Apache Hive Metastore to discover tables.

---

#### 2.1 Hive Metastore Client

```go
// internal/catalog/hive/client.go

type Config struct {
    // ThriftURI is the Hive Metastore Thrift endpoint.
    // Format: thrift://host:port
    ThriftURI string
    
    // ConnectTimeout for initial connection.
    ConnectTimeout time.Duration
    
    // RequestTimeout for each request.
    RequestTimeout time.Duration
}

type Client struct {
    config Config
    conn   *thrift.TTransport
    client *hive_metastore.ThriftHiveMetastoreClient
}

func NewClient(config Config) (*Client, error) {
    // Parse URI
    host, port, err := parseThriftURI(config.ThriftURI)
    if err != nil {
        return nil, fmt.Errorf("hive: invalid URI %q: %w", config.ThriftURI, err)
    }
    
    // Create Thrift transport
    transport, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))
    if err != nil {
        return nil, fmt.Errorf("hive: failed to create socket: %w", err)
    }
    
    // Open connection with timeout
    ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
    defer cancel()
    
    if err := transport.Open(); err != nil {
        return nil, fmt.Errorf("hive: connection failed: %w", err)
    }
    
    // Create client
    protocol := thrift.NewTBinaryProtocol(transport, true, true)
    client := hive_metastore.NewThriftHiveMetastoreClientProtocol(transport, protocol, protocol)
    
    return &Client{
        config: config,
        conn:   transport,
        client: client,
    }, nil
}
```

---

#### 2.2 Table Format Detection

Detect Iceberg/Delta/Hudi from Hive table properties:

```go
// internal/catalog/hive/format.go

func DetectFormat(tableParams map[string]string, serdeInfo *hive_metastore.SerDeInfo) TableFormat {
    // Check for Iceberg
    if tableParams["table_type"] == "ICEBERG" {
        return FormatIceberg
    }
    if _, ok := tableParams["metadata_location"]; ok {
        return FormatIceberg
    }
    
    // Check for Delta
    if tableParams["spark.sql.sources.provider"] == "delta" {
        return FormatDelta
    }
    if strings.Contains(tableParams["location"], "_delta_log") {
        return FormatDelta
    }
    
    // Check for Hudi
    if tableParams["spark.sql.sources.provider"] == "hudi" {
        return FormatHudi
    }
    if _, ok := tableParams["hoodie.table.name"]; ok {
        return FormatHudi
    }
    
    // Check SerDe for format hints
    if serdeInfo != nil {
        switch {
        case strings.Contains(serdeInfo.SerializationLib, "parquet"):
            return FormatParquet
        case strings.Contains(serdeInfo.SerializationLib, "orc"):
            return FormatORC
        case strings.Contains(serdeInfo.SerializationLib, "csv"):
            return FormatCSV
        }
    }
    
    return FormatUnknown
}
```

---

#### 2.3 Configuration

```yaml
catalogs:
  hive:
    type: hive
    thrift_uri: thrift://hive-metastore.cluster.local:9083
    connect_timeout: 10s
    request_timeout: 30s
    
    # Optional: filter which databases to sync
    include_databases:
      - analytics
      - warehouse
    exclude_databases:
      - tmp
      - staging
```

---

#### 2.4 Red-Flag Tests (Required)

| Test | Scenario | Expected |
|------|----------|----------|
| `TestHiveUnreachable` | Metastore not running | Clear connection error |
| `TestHiveInvalidURI` | Malformed thrift URI | Validation error on startup |
| `TestHiveTimeout` | Slow metastore | Timeout error |
| `TestHiveAuthFailed` | Kerberos auth fails | Auth error with details |
| `TestHiveUnknownDatabase` | Database doesn't exist | Empty result, no error |

---

#### 2.5 Green-Flag Tests (Required)

| Test | Scenario | Expected |
|------|----------|----------|
| `TestHiveListDatabases` | List all databases | Returns database names |
| `TestHiveListTables` | List tables in database | Returns table info |
| `TestHiveGetTableIceberg` | Get Iceberg table | Correct format detected |
| `TestHiveGetTableDelta` | Get Delta table | Correct format detected |
| `TestHiveGetTableHudi` | Get Hudi table | Correct format detected |

---

### 3. AWS Glue Integration

#### Objective
Connect to AWS Glue Data Catalog to discover tables.

---

#### 3.1 Glue Client

```go
// internal/catalog/glue/client.go

type Config struct {
    // Region is the AWS region.
    Region string
    
    // CatalogID is the AWS account ID (optional, uses caller's account if empty).
    CatalogID string
    
    // Credentials (if not using IAM role/instance profile)
    AccessKeyID     string
    SecretAccessKey string
    SessionToken    string
    
    // RequestTimeout for API calls.
    RequestTimeout time.Duration
}

type Client struct {
    config Config
    glue   *glue.Client
}

func NewClient(ctx context.Context, config Config) (*Client, error) {
    // Load AWS config
    cfg, err := awsconfig.LoadDefaultConfig(ctx,
        awsconfig.WithRegion(config.Region),
    )
    if err != nil {
        return nil, fmt.Errorf("glue: failed to load AWS config: %w", err)
    }
    
    // Override credentials if provided
    if config.AccessKeyID != "" {
        cfg.Credentials = credentials.NewStaticCredentialsProvider(
            config.AccessKeyID,
            config.SecretAccessKey,
            config.SessionToken,
        )
    }
    
    client := glue.NewFromConfig(cfg)
    
    return &Client{
        config: config,
        glue:   client,
    }, nil
}
```

---

#### 3.2 Glue Table Discovery

```go
// internal/catalog/glue/client.go

func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
    var databases []string
    paginator := glue.NewGetDatabasesPaginator(c.glue, &glue.GetDatabasesInput{
        CatalogId: &c.config.CatalogID,
    })
    
    for paginator.HasMorePages() {
        page, err := paginator.NextPage(ctx)
        if err != nil {
            return nil, fmt.Errorf("glue: failed to list databases: %w", err)
        }
        for _, db := range page.DatabaseList {
            databases = append(databases, *db.Name)
        }
    }
    
    return databases, nil
}

func (c *Client) ListTables(ctx context.Context, database string) ([]TableInfo, error) {
    var tables []TableInfo
    paginator := glue.NewGetTablesPaginator(c.glue, &glue.GetTablesInput{
        CatalogId:    &c.config.CatalogID,
        DatabaseName: &database,
    })
    
    for paginator.HasMorePages() {
        page, err := paginator.NextPage(ctx)
        if err != nil {
            return nil, fmt.Errorf("glue: failed to list tables: %w", err)
        }
        for _, t := range page.TableList {
            tables = append(tables, TableInfo{
                Database: database,
                Name:     *t.Name,
                Format:   detectGlueFormat(t),
            })
        }
    }
    
    return tables, nil
}

func detectGlueFormat(t types.Table) TableFormat {
    // Check table parameters
    if t.Parameters != nil {
        if t.Parameters["table_type"] == "ICEBERG" {
            return FormatIceberg
        }
        if t.Parameters["spark.sql.sources.provider"] == "delta" {
            return FormatDelta
        }
    }
    
    // Check storage descriptor
    if t.StorageDescriptor != nil {
        loc := aws.ToString(t.StorageDescriptor.Location)
        if strings.Contains(loc, "_delta_log") {
            return FormatDelta
        }
    }
    
    return FormatUnknown
}
```

---

#### 3.3 Configuration

```yaml
catalogs:
  glue:
    type: glue
    region: us-east-1
    catalog_id: "123456789012"   # Optional
    request_timeout: 30s
    
    # Optional: Use explicit credentials (prefer IAM role)
    # access_key_id: ${AWS_ACCESS_KEY_ID}
    # secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    
    include_databases:
      - analytics
      - data_warehouse
```

---

### 4. Catalog Sync Command

#### Objective
Provide a CLI command to synchronize tables from external catalogs to Canonic.

---

#### 4.1 CLI Command

```bash
# Sync all tables from all configured catalogs
canonic catalog sync

# Sync from specific catalog
canonic catalog sync --source hive

# Sync specific database
canonic catalog sync --source glue --database analytics

# Dry-run (show what would be synced)
canonic catalog sync --dry-run

# Force refresh (update existing tables)
canonic catalog sync --force
```

---

#### 4.2 Sync Logic

```go
// internal/cli/catalog.go

func (c *CLI) CatalogSync(ctx context.Context, opts CatalogSyncOptions) error {
    // Get catalog client
    catalog, err := c.getCatalog(opts.Source)
    if err != nil {
        return err
    }
    
    // List databases to sync
    databases, err := c.getDatabasesToSync(ctx, catalog, opts)
    if err != nil {
        return err
    }
    
    var synced, skipped, failed int
    
    for _, db := range databases {
        tables, err := catalog.ListTables(ctx, db)
        if err != nil {
            c.logger.Error("failed to list tables", "database", db, "error", err)
            continue
        }
        
        for _, table := range tables {
            // Get full metadata
            meta, err := catalog.GetTable(ctx, db, table.Name)
            if err != nil {
                c.logger.Error("failed to get table", "table", table.Name, "error", err)
                failed++
                continue
            }
            
            // Check if table already registered
            existing, _ := c.gateway.GetTable(ctx, fmt.Sprintf("%s.%s", db, table.Name))
            if existing != nil && !opts.Force {
                c.logger.Debug("table already registered, skipping", "table", table.Name)
                skipped++
                continue
            }
            
            // Dry-run: just print
            if opts.DryRun {
                fmt.Printf("Would sync: %s.%s (format: %s)\n", db, table.Name, meta.Format)
                continue
            }
            
            // Register in Canonic
            err = c.gateway.RegisterTable(ctx, RegisterTableRequest{
                Name:     fmt.Sprintf("%s.%s", db, table.Name),
                Engine:   c.selectEngine(meta.Format),
                Format:   string(meta.Format),
                Location: meta.Location,
                Columns:  meta.Columns,
            })
            if err != nil {
                c.logger.Error("failed to register table", "table", table.Name, "error", err)
                failed++
                continue
            }
            
            synced++
        }
    }
    
    fmt.Printf("Sync complete: %d synced, %d skipped, %d failed\n", synced, skipped, failed)
    return nil
}
```

---

#### 4.3 Engine Selection

```go
// internal/cli/catalog.go

// selectEngine chooses the query engine based on table format.
func (c *CLI) selectEngine(format TableFormat) string {
    switch format {
    case FormatIceberg:
        return "trino"  // Trino has best Iceberg support
    case FormatDelta:
        return "trino"  // Trino Delta connector
    case FormatHudi:
        return "trino"  // Trino Hudi connector
    case FormatParquet:
        return "duckdb" // DuckDB is fast for Parquet
    default:
        return "duckdb" // Default fallback
    }
}
```

---

#### 4.4 Sync Output

```
$ canonic catalog sync --source hive

Connecting to Hive Metastore: thrift://hive-metastore:9083
Discovering databases... found 3

Syncing database: analytics
  ✓ analytics.sales_orders (iceberg → trino)
  ✓ analytics.customers (delta → trino)
  ✓ analytics.events (parquet → duckdb)
  - analytics.tmp_staging (skipped: already registered)

Syncing database: warehouse
  ✓ warehouse.inventory (iceberg → trino)
  ✗ warehouse.broken_table (failed: invalid location)

Sync complete: 4 synced, 1 skipped, 1 failed
```

---

### 5. Auto-Format Detection

#### Objective
Automatically detect table format from storage location and metadata.

---

#### 5.1 Location-Based Detection

```go
// internal/catalog/format.go

func DetectFormatFromLocation(location string) (TableFormat, error) {
    // Check for Iceberg metadata
    if hasIcebergMetadata(location) {
        return FormatIceberg, nil
    }
    
    // Check for Delta log
    if hasDeltaLog(location) {
        return FormatDelta, nil
    }
    
    // Check for Hudi metadata
    if hasHudiMetadata(location) {
        return FormatHudi, nil
    }
    
    // Check file extensions
    if hasParquetFiles(location) {
        return FormatParquet, nil
    }
    
    return FormatUnknown, nil
}

func hasIcebergMetadata(location string) bool {
    // Iceberg: look for metadata/ directory with .metadata.json files
    metadataPath := filepath.Join(location, "metadata")
    // Check if metadata directory exists and has .metadata.json files
    return pathExists(metadataPath) && hasFiles(metadataPath, "*.metadata.json")
}

func hasDeltaLog(location string) bool {
    // Delta: look for _delta_log/ directory
    deltaLogPath := filepath.Join(location, "_delta_log")
    return pathExists(deltaLogPath)
}

func hasHudiMetadata(location string) bool {
    // Hudi: look for .hoodie/ directory
    hoodiePath := filepath.Join(location, ".hoodie")
    return pathExists(hoodiePath)
}
```

---

#### 5.2 S3/Cloud Storage Support

```go
// internal/catalog/storage/s3.go

type S3Client struct {
    client *s3.Client
}

func (c *S3Client) PathExists(ctx context.Context, uri string) (bool, error) {
    bucket, key, err := parseS3URI(uri)
    if err != nil {
        return false, err
    }
    
    _, err = c.client.HeadObject(ctx, &s3.HeadObjectInput{
        Bucket: &bucket,
        Key:    &key,
    })
    if err != nil {
        var notFound *types.NotFound
        if errors.As(err, &notFound) {
            return false, nil
        }
        return false, err
    }
    return true, nil
}

func (c *S3Client) ListPrefix(ctx context.Context, uri string) ([]string, error) {
    bucket, prefix, err := parseS3URI(uri)
    if err != nil {
        return nil, err
    }
    
    var objects []string
    paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
        Bucket: &bucket,
        Prefix: &prefix,
    })
    
    for paginator.HasMorePages() {
        page, err := paginator.NextPage(ctx)
        if err != nil {
            return nil, err
        }
        for _, obj := range page.Contents {
            objects = append(objects, *obj.Key)
        }
    }
    
    return objects, nil
}
```

---

### 6. Periodic Catalog Refresh

#### Objective
Automatically sync tables on a schedule without manual intervention.

---

#### 6.1 Background Sync Worker

```go
// internal/catalog/sync/worker.go

type SyncWorker struct {
    catalogs   []Catalog
    repository storage.TableRepository
    interval   time.Duration
    logger     *slog.Logger
    
    stopCh     chan struct{}
    doneCh     chan struct{}
}

func NewSyncWorker(config SyncConfig) *SyncWorker {
    return &SyncWorker{
        catalogs:   config.Catalogs,
        repository: config.Repository,
        interval:   config.Interval,
        logger:     config.Logger,
        stopCh:     make(chan struct{}),
        doneCh:     make(chan struct{}),
    }
}

func (w *SyncWorker) Start(ctx context.Context) {
    go w.run(ctx)
}

func (w *SyncWorker) run(ctx context.Context) {
    defer close(w.doneCh)
    
    // Initial sync
    w.syncAll(ctx)
    
    ticker := time.NewTicker(w.interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            w.syncAll(ctx)
        case <-w.stopCh:
            return
        case <-ctx.Done():
            return
        }
    }
}

func (w *SyncWorker) syncAll(ctx context.Context) {
    w.logger.Info("starting catalog sync")
    
    for _, catalog := range w.catalogs {
        if err := w.syncCatalog(ctx, catalog); err != nil {
            w.logger.Error("catalog sync failed",
                "catalog", catalog.Name(),
                "error", err)
        }
    }
    
    w.logger.Info("catalog sync complete")
}

func (w *SyncWorker) Stop() {
    close(w.stopCh)
    <-w.doneCh
}
```

---

#### 6.2 Configuration

```yaml
catalog_sync:
  enabled: true
  interval: 5m          # Sync every 5 minutes
  on_startup: true      # Sync immediately on gateway start
  
  # What to do with removed tables
  orphan_policy: mark   # mark | delete | ignore
  
  catalogs:
    - hive
    - glue
```

---

#### 6.3 Orphan Handling

When a table exists in Canonic but not in the source catalog:

```go
// internal/catalog/sync/worker.go

func (w *SyncWorker) handleOrphans(ctx context.Context, catalogTables, canonicTables map[string]bool) {
    for table := range canonicTables {
        if !catalogTables[table] {
            switch w.config.OrphanPolicy {
            case "mark":
                // Mark as orphaned, don't delete
                w.repository.MarkOrphaned(ctx, table)
                w.logger.Warn("table marked as orphaned", "table", table)
                
            case "delete":
                // Delete from Canonic
                w.repository.DeleteTable(ctx, table)
                w.logger.Warn("orphaned table deleted", "table", table)
                
            case "ignore":
                // Do nothing
            }
        }
    }
}
```

---

### 7. Unity Catalog Integration (Databricks)

#### Objective
Connect to Databricks Unity Catalog for table discovery.

---

#### 7.1 Unity Catalog Client

```go
// internal/catalog/unity/client.go

type Config struct {
    // Host is the Databricks workspace URL.
    // Format: https://adb-1234567890.12.azuredatabricks.net
    Host string
    
    // Token is the personal access token or service principal token.
    Token string
    
    // Catalog is the Unity Catalog name (optional, lists all if empty).
    Catalog string
    
    RequestTimeout time.Duration
}

type Client struct {
    config     Config
    httpClient *http.Client
}

func NewClient(config Config) (*Client, error) {
    if config.Host == "" {
        return nil, fmt.Errorf("unity: host is required")
    }
    if config.Token == "" {
        return nil, fmt.Errorf("unity: token is required")
    }
    
    return &Client{
        config: config,
        httpClient: &http.Client{
            Timeout: config.RequestTimeout,
        },
    }, nil
}
```

---

#### 7.2 Unity Catalog API

```go
// internal/catalog/unity/client.go

func (c *Client) ListCatalogs(ctx context.Context) ([]string, error) {
    resp, err := c.request(ctx, "GET", "/api/2.1/unity-catalog/catalogs", nil)
    if err != nil {
        return nil, err
    }
    
    var result struct {
        Catalogs []struct {
            Name string `json:"name"`
        } `json:"catalogs"`
    }
    if err := json.Unmarshal(resp, &result); err != nil {
        return nil, err
    }
    
    var catalogs []string
    for _, c := range result.Catalogs {
        catalogs = append(catalogs, c.Name)
    }
    return catalogs, nil
}

func (c *Client) ListSchemas(ctx context.Context, catalog string) ([]string, error) {
    path := fmt.Sprintf("/api/2.1/unity-catalog/schemas?catalog_name=%s", catalog)
    resp, err := c.request(ctx, "GET", path, nil)
    // ... parse response
}

func (c *Client) ListTables(ctx context.Context, catalog, schema string) ([]TableInfo, error) {
    path := fmt.Sprintf("/api/2.1/unity-catalog/tables?catalog_name=%s&schema_name=%s", 
        catalog, schema)
    resp, err := c.request(ctx, "GET", path, nil)
    // ... parse response with format detection
}
```

---

#### 7.3 Configuration

```yaml
catalogs:
  unity:
    type: unity
    host: https://adb-1234567890.12.azuredatabricks.net
    token: ${DATABRICKS_TOKEN}
    catalog: main          # Optional: specific catalog
    request_timeout: 30s
    
    include_schemas:
      - analytics
      - data_warehouse
```

---

## Out-of-Scope (Deferred to Later Phases)

| Item | Deferred To | Reason |
|------|-------------|--------|
| Cross-engine JOINs | Phase 9 | Requires query decomposition |
| Write-back to catalogs | Future | Read-only for now |
| Real-time CDC sync | Future | Requires Kafka/Debezium |
| Custom catalog plugins | Future | Current set covers most use cases |
| Column-level lineage | Future | Requires deep metadata parsing |

---

## Exit Criteria

Phase 7 is complete when:

1. **Catalog Clients**
   - [ ] Hive Metastore client implemented and tested
   - [ ] AWS Glue client implemented and tested
   - [ ] Unity Catalog client implemented and tested
   - [ ] All clients implement `Catalog` interface

2. **Format Detection**
   - [ ] Iceberg tables correctly identified
   - [ ] Delta Lake tables correctly identified
   - [ ] Hudi tables correctly identified
   - [ ] Parquet/ORC fallback works

3. **Sync Command**
   - [ ] `canonic catalog sync` works end-to-end
   - [ ] Dry-run mode shows changes without applying
   - [ ] Force mode updates existing tables
   - [ ] Progress output is clear and useful

4. **Background Sync**
   - [ ] Periodic sync runs on configured interval
   - [ ] Orphan handling respects policy
   - [ ] Sync errors logged but don't crash gateway

5. **Integration Tests**
   - [ ] End-to-end test with real Hive Metastore
   - [ ] End-to-end test with LocalStack Glue
   - [ ] All red-flag and green-flag tests pass

6. **Documentation**
   - [ ] Catalog configuration documented
   - [ ] Troubleshooting guide for each catalog
   - [ ] Format detection rules documented

---

## Appendix A: Error Messages

```go
// Good
"hive: connection failed to thrift://hive-metastore:9083: dial tcp: connection refused; " +
"verify the Hive Metastore is running and accessible"

// Bad
"catalog error"
```

---

## Appendix B: Go Dependencies

```go
// go.mod additions
require (
    github.com/apache/thrift v0.19.0           // Hive Metastore Thrift
    github.com/aws/aws-sdk-go-v2 v1.24.0       // AWS SDK
    github.com/aws/aws-sdk-go-v2/service/glue  // Glue client
    github.com/aws/aws-sdk-go-v2/service/s3    // S3 client
)
```

---

## Appendix C: Test Infrastructure

**docker-compose.catalog-test.yaml:**
```yaml
version: '3.8'

services:
  hive-metastore:
    image: apache/hive:3.1.3
    ports:
      - "9083:9083"
    environment:
      SERVICE_NAME: metastore
    
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      SERVICES: glue,s3
      
  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
```

---

## Appendix D: Configuration Reference

```yaml
# Complete Phase 7 configuration example

catalogs:
  hive:
    type: hive
    thrift_uri: thrift://hive-metastore:9083
    connect_timeout: 10s
    request_timeout: 30s
    include_databases:
      - analytics
      - warehouse
      
  glue:
    type: glue
    region: us-east-1
    catalog_id: "123456789012"
    request_timeout: 30s
    include_databases:
      - analytics
      
  unity:
    type: unity
    host: https://adb-1234567890.12.azuredatabricks.net
    token: ${DATABRICKS_TOKEN}
    request_timeout: 30s
    include_schemas:
      - analytics

catalog_sync:
  enabled: true
  interval: 5m
  on_startup: true
  orphan_policy: mark
  catalogs:
    - hive
    - glue
```
