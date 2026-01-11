# Phase 9 Specification – Cross-Engine Federation

## Status
**Authoritative**
This document is binding for Phase 9 work.

Phase 9 exists to make canonic-labs **execute queries that span multiple engines**, enabling JOINs between Iceberg tables on Trino, Delta tables on Spark, and warehouse tables on Snowflake through a single SQL query.

This phase optimizes for:
- query unification
- transparent federation
- optimal execution

It does **not** optimize for write operations or real-time streaming.

---

## Phase 9 Goals

Phase 9 addresses the following problems:

1. Users cannot JOIN tables from different engines
2. No mechanism to decompose queries across engines
3. No intermediate result handling for multi-engine queries
4. No cost-based engine selection
5. No pushdown optimization for federated queries

After Phase 9:
> Users write `SELECT * FROM iceberg.sales JOIN snowflake.customers` and Canonic handles the rest.

---

## Prerequisites

Before starting Phase 9, ensure:
- Phase 6 complete (Trino/Spark connectivity)
- Phase 7 complete (catalog integration)
- Phase 8 complete (multi-format support, warehouse adapters)
- All engines healthy and queryable independently

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FEDERATED QUERY FLOW                               │
└─────────────────────────────────────────────────────────────────────────────┘

User Query:
  SELECT s.*, c.name 
  FROM iceberg.sales s 
  JOIN snowflake.customers c ON s.customer_id = c.id
  WHERE s.region = 'US' AND c.tier = 'premium'

                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              QUERY ANALYZER                                  │
│  1. Parse SQL                                                                │
│  2. Identify tables and their engines                                        │
│  3. Detect cross-engine query                                                │
│  4. Extract pushable predicates per engine                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            QUERY DECOMPOSER                                  │
│  Split into sub-queries:                                                     │
│                                                                              │
│  SubQuery 1 (Trino/Iceberg):                                                │
│    SELECT s.* FROM iceberg.sales s WHERE s.region = 'US'                    │
│                                                                              │
│  SubQuery 2 (Snowflake):                                                    │
│    SELECT c.id, c.name FROM snowflake.customers c WHERE c.tier = 'premium'  │
│                                                                              │
│  Join Strategy: HASH JOIN on customer_id = id                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
┌───────────────────────────┐       ┌───────────────────────────┐
│       TRINO ENGINE        │       │     SNOWFLAKE ENGINE      │
│                           │       │                           │
│  Execute SubQuery 1       │       │  Execute SubQuery 2       │
│  Stream results           │       │  Stream results           │
└─────────────┬─────────────┘       └─────────────┬─────────────┘
              │                                   │
              └───────────────┬───────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            JOIN EXECUTOR                                     │
│                                                                              │
│  Strategy: Hash Join (smaller table → build, larger → probe)                │
│  1. Build hash table from SubQuery 2 results                                │
│  2. Stream SubQuery 1 results, probe hash table                             │
│  3. Emit joined rows                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RESULT STREAM                                   │
│  Return unified results to client                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## In-Scope Work (MANDATORY)

### 1. Query Decomposition

#### Objective
Split a multi-engine query into sub-queries that can be executed independently.

---

#### 1.1 Query Analyzer

```go
// internal/federation/analyzer.go

type QueryAnalysis struct {
    // Original query
    OriginalSQL string
    
    // Tables involved, grouped by engine
    TablesByEngine map[string][]*TableRef
    
    // Is this a cross-engine query?
    IsCrossEngine bool
    
    // Join conditions
    Joins []*JoinCondition
    
    // Predicates that can be pushed to each engine
    PushablePredicates map[string][]*Predicate
    
    // Columns needed from each table
    RequiredColumns map[string][]string
    
    // Aggregations (must be done post-join)
    Aggregations []*Aggregation
    
    // Order by (must be done post-join if cross-engine)
    OrderBy []*OrderByClause
    
    // Limit (applied after join)
    Limit *int
}

type TableRef struct {
    Schema    string
    Name      string
    Alias     string
    Engine    string
    Format    TableFormat
}

type JoinCondition struct {
    Type      JoinType  // INNER, LEFT, RIGHT, FULL
    LeftTable string
    LeftCol   string
    RightTable string
    RightCol  string
    Operator  string    // =, <, >, etc.
}

type Predicate struct {
    Table    string
    Column   string
    Operator string
    Value    interface{}
    Raw      string  // Original SQL fragment
}
```

---

#### 1.2 Analyzer Implementation

```go
// internal/federation/analyzer.go

type Analyzer struct {
    parser   *sql.Parser
    metadata storage.TableRepository
}

func (a *Analyzer) Analyze(ctx context.Context, sql string) (*QueryAnalysis, error) {
    // Parse SQL
    stmt, err := a.parser.Parse(sql)
    if err != nil {
        return nil, fmt.Errorf("federation: parse error: %w", err)
    }
    
    analysis := &QueryAnalysis{
        OriginalSQL:        sql,
        TablesByEngine:     make(map[string][]*TableRef),
        PushablePredicates: make(map[string][]*Predicate),
        RequiredColumns:    make(map[string][]string),
    }
    
    // Extract tables
    tables, err := a.extractTables(ctx, stmt)
    if err != nil {
        return nil, err
    }
    
    // Group by engine
    for _, table := range tables {
        meta, err := a.metadata.GetTable(ctx, table.FullName())
        if err != nil {
            return nil, fmt.Errorf("table %s not found: %w", table.FullName(), err)
        }
        table.Engine = meta.Engine
        table.Format = meta.Format
        
        analysis.TablesByEngine[meta.Engine] = append(
            analysis.TablesByEngine[meta.Engine], table)
    }
    
    // Check if cross-engine
    analysis.IsCrossEngine = len(analysis.TablesByEngine) > 1
    
    if !analysis.IsCrossEngine {
        // Single engine - no decomposition needed
        return analysis, nil
    }
    
    // Extract join conditions
    analysis.Joins = a.extractJoins(stmt)
    
    // Extract and classify predicates
    analysis.PushablePredicates = a.extractPushablePredicates(stmt, tables)
    
    // Determine required columns per table
    analysis.RequiredColumns = a.extractRequiredColumns(stmt, tables)
    
    // Extract aggregations
    analysis.Aggregations = a.extractAggregations(stmt)
    
    // Extract order by
    analysis.OrderBy = a.extractOrderBy(stmt)
    
    // Extract limit
    analysis.Limit = a.extractLimit(stmt)
    
    return analysis, nil
}
```

---

#### 1.3 Predicate Pushdown Analysis

```go
// internal/federation/pushdown.go

func (a *Analyzer) extractPushablePredicates(
    stmt sqlparser.Statement,
    tables []*TableRef,
) map[string][]*Predicate {
    predicates := make(map[string][]*Predicate)
    
    // Walk WHERE clause
    sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
        switch expr := node.(type) {
        case *sqlparser.ComparisonExpr:
            // Check if this predicate references only one table
            table := a.getSingleTableReference(expr, tables)
            if table != "" {
                pred := &Predicate{
                    Table:    table,
                    Column:   a.extractColumn(expr.Left),
                    Operator: expr.Operator,
                    Value:    a.extractValue(expr.Right),
                    Raw:      sqlparser.String(expr),
                }
                predicates[table] = append(predicates[table], pred)
            }
        }
        return true, nil
    }, stmt)
    
    return predicates
}

// getSingleTableReference returns the table name if the expression
// references only one table, empty string otherwise.
func (a *Analyzer) getSingleTableReference(
    expr *sqlparser.ComparisonExpr,
    tables []*TableRef,
) string {
    // Extract all column references
    var refs []string
    sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
        if col, ok := node.(*sqlparser.ColName); ok {
            refs = append(refs, col.Qualifier.Name.String())
        }
        return true, nil
    }, expr)
    
    // Check if all refs are from the same table
    if len(refs) == 0 {
        return ""
    }
    
    firstTable := refs[0]
    for _, ref := range refs[1:] {
        if ref != firstTable {
            return "" // References multiple tables
        }
    }
    
    return firstTable
}
```

---

#### 1.4 Query Decomposer

```go
// internal/federation/decomposer.go

type SubQuery struct {
    Engine      string
    SQL         string
    Tables      []*TableRef
    Predicates  []*Predicate
    Columns     []string
    EstimatedRows int64  // For join ordering
}

type DecomposedQuery struct {
    SubQueries   []*SubQuery
    JoinPlan     *JoinPlan
    PostJoinOps  *PostJoinOperations
}

type JoinPlan struct {
    Steps []JoinStep
}

type JoinStep struct {
    Type        JoinType
    LeftInput   string  // SubQuery ID or previous step
    RightInput  string
    LeftKey     string
    RightKey    string
    Strategy    JoinStrategy  // HASH, MERGE, NESTED_LOOP
}

type JoinStrategy string

const (
    JoinStrategyHash       JoinStrategy = "hash"
    JoinStrategyMerge      JoinStrategy = "merge"
    JoinStrategyNestedLoop JoinStrategy = "nested_loop"
)

type PostJoinOperations struct {
    Aggregations []*Aggregation
    OrderBy      []*OrderByClause
    Limit        *int
}
```

---

#### 1.5 Decomposer Implementation

```go
// internal/federation/decomposer.go

type Decomposer struct {
    analyzer *Analyzer
}

func (d *Decomposer) Decompose(analysis *QueryAnalysis) (*DecomposedQuery, error) {
    if !analysis.IsCrossEngine {
        return nil, fmt.Errorf("not a cross-engine query")
    }
    
    result := &DecomposedQuery{
        SubQueries: make([]*SubQuery, 0),
    }
    
    // Generate sub-query for each engine
    for engine, tables := range analysis.TablesByEngine {
        subQuery, err := d.generateSubQuery(engine, tables, analysis)
        if err != nil {
            return nil, fmt.Errorf("failed to generate sub-query for %s: %w", engine, err)
        }
        result.SubQueries = append(result.SubQueries, subQuery)
    }
    
    // Generate join plan
    joinPlan, err := d.generateJoinPlan(analysis, result.SubQueries)
    if err != nil {
        return nil, fmt.Errorf("failed to generate join plan: %w", err)
    }
    result.JoinPlan = joinPlan
    
    // Set post-join operations
    result.PostJoinOps = &PostJoinOperations{
        Aggregations: analysis.Aggregations,
        OrderBy:      analysis.OrderBy,
        Limit:        analysis.Limit,
    }
    
    return result, nil
}

func (d *Decomposer) generateSubQuery(
    engine string,
    tables []*TableRef,
    analysis *QueryAnalysis,
) (*SubQuery, error) {
    // Build SELECT clause with required columns
    var columns []string
    for _, table := range tables {
        tableCols := analysis.RequiredColumns[table.FullName()]
        for _, col := range tableCols {
            columns = append(columns, fmt.Sprintf("%s.%s", table.Alias, col))
        }
        
        // Always include join keys
        for _, join := range analysis.Joins {
            if join.LeftTable == table.Alias {
                columns = append(columns, fmt.Sprintf("%s.%s", table.Alias, join.LeftCol))
            }
            if join.RightTable == table.Alias {
                columns = append(columns, fmt.Sprintf("%s.%s", table.Alias, join.RightCol))
            }
        }
    }
    columns = deduplicate(columns)
    
    // Build FROM clause
    var fromParts []string
    for _, table := range tables {
        fromParts = append(fromParts, fmt.Sprintf("%s AS %s", table.FullName(), table.Alias))
    }
    
    // Build WHERE clause with pushable predicates
    var whereParts []string
    for _, table := range tables {
        preds := analysis.PushablePredicates[table.FullName()]
        for _, pred := range preds {
            whereParts = append(whereParts, pred.Raw)
        }
    }
    
    // Construct SQL
    sql := fmt.Sprintf("SELECT %s FROM %s",
        strings.Join(columns, ", "),
        strings.Join(fromParts, ", "))
    
    if len(whereParts) > 0 {
        sql += " WHERE " + strings.Join(whereParts, " AND ")
    }
    
    return &SubQuery{
        Engine:     engine,
        SQL:        sql,
        Tables:     tables,
        Predicates: flattenPredicates(analysis.PushablePredicates, tables),
        Columns:    columns,
    }, nil
}
```

---

### 2. Intermediate Result Handling

#### Objective
Efficiently transfer and store results between sub-queries for join execution.

---

#### 2.1 Result Streaming

```go
// internal/federation/stream.go

// ResultStream represents a stream of rows from a sub-query.
type ResultStream interface {
    // Schema returns column names and types.
    Schema() *ResultSchema
    
    // Next returns the next row, or nil if exhausted.
    Next(ctx context.Context) (Row, error)
    
    // Close releases resources.
    Close() error
    
    // EstimatedRows returns estimated row count (-1 if unknown).
    EstimatedRows() int64
}

type ResultSchema struct {
    Columns []ColumnDef
}

type ColumnDef struct {
    Name string
    Type string
}

type Row map[string]interface{}
```

---

#### 2.2 Memory-Based Intermediate Storage

For small results:

```go
// internal/federation/storage/memory.go

type MemoryResultStore struct {
    rows   []Row
    schema *ResultSchema
    mu     sync.RWMutex
}

func NewMemoryResultStore(schema *ResultSchema) *MemoryResultStore {
    return &MemoryResultStore{
        rows:   make([]Row, 0),
        schema: schema,
    }
}

func (s *MemoryResultStore) Append(row Row) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.rows = append(s.rows, row)
    return nil
}

func (s *MemoryResultStore) Stream() ResultStream {
    return &memoryStream{
        rows:   s.rows,
        schema: s.schema,
        idx:    0,
    }
}

func (s *MemoryResultStore) Size() int {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return len(s.rows)
}
```

---

#### 2.3 Disk-Spill Storage

For large results:

```go
// internal/federation/storage/spill.go

type SpillResultStore struct {
    schema    *ResultSchema
    threshold int           // Rows before spilling to disk
    memory    []Row         // In-memory buffer
    spillFile *os.File      // Spill file
    encoder   *gob.Encoder
    decoder   *gob.Decoder
    spilled   bool
    mu        sync.Mutex
}

func NewSpillResultStore(schema *ResultSchema, threshold int) *SpillResultStore {
    return &SpillResultStore{
        schema:    schema,
        threshold: threshold,
        memory:    make([]Row, 0, threshold),
    }
}

func (s *SpillResultStore) Append(row Row) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    if !s.spilled && len(s.memory) < s.threshold {
        s.memory = append(s.memory, row)
        return nil
    }
    
    // Spill to disk
    if !s.spilled {
        if err := s.initSpillFile(); err != nil {
            return err
        }
        // Write buffered rows
        for _, r := range s.memory {
            if err := s.encoder.Encode(r); err != nil {
                return err
            }
        }
        s.memory = nil // Free memory
        s.spilled = true
    }
    
    return s.encoder.Encode(row)
}

func (s *SpillResultStore) initSpillFile() error {
    f, err := os.CreateTemp("", "canonic-spill-*.gob")
    if err != nil {
        return fmt.Errorf("failed to create spill file: %w", err)
    }
    s.spillFile = f
    s.encoder = gob.NewEncoder(f)
    return nil
}
```

---

#### 2.4 Configuration

```yaml
federation:
  # Intermediate result settings
  intermediate_storage:
    # Memory threshold before spilling to disk
    memory_threshold_rows: 100000
    memory_threshold_bytes: 100MB
    
    # Spill directory
    spill_dir: /tmp/canonic-spill
    
    # Clean up spill files after query
    auto_cleanup: true
```

---

### 3. Join Execution

#### Objective
Execute joins between results from different engines.

---

#### 3.1 Hash Join Implementation

```go
// internal/federation/join/hash.go

type HashJoinExecutor struct {
    config HashJoinConfig
}

type HashJoinConfig struct {
    // BuildSide is the smaller input (used to build hash table)
    BuildSide ResultStream
    
    // ProbeSide is the larger input (streamed through)
    ProbeSide ResultStream
    
    // JoinKeys
    BuildKey string
    ProbeKey string
    
    // JoinType
    Type JoinType
}

func (e *HashJoinExecutor) Execute(ctx context.Context) (ResultStream, error) {
    // Phase 1: Build hash table from build side
    hashTable := make(map[interface{}][]Row)
    
    for {
        row, err := e.config.BuildSide.Next(ctx)
        if err != nil {
            return nil, fmt.Errorf("hash join build phase failed: %w", err)
        }
        if row == nil {
            break
        }
        
        key := row[e.config.BuildKey]
        hashTable[key] = append(hashTable[key], row)
    }
    
    // Phase 2: Create probe stream
    return &hashJoinStream{
        hashTable: hashTable,
        probeSide: e.config.ProbeSide,
        probeKey:  e.config.ProbeKey,
        joinType:  e.config.Type,
        buildSchema: e.config.BuildSide.Schema(),
        probeSchema: e.config.ProbeSide.Schema(),
    }, nil
}

type hashJoinStream struct {
    hashTable   map[interface{}][]Row
    probeSide   ResultStream
    probeKey    string
    joinType    JoinType
    buildSchema *ResultSchema
    probeSchema *ResultSchema
    
    // Current state
    currentProbeRow Row
    matchIdx        int
    matches         []Row
}

func (s *hashJoinStream) Next(ctx context.Context) (Row, error) {
    for {
        // If we have pending matches, emit them
        if s.matchIdx < len(s.matches) {
            result := s.mergeRows(s.currentProbeRow, s.matches[s.matchIdx])
            s.matchIdx++
            return result, nil
        }
        
        // Get next probe row
        probeRow, err := s.probeSide.Next(ctx)
        if err != nil {
            return nil, err
        }
        if probeRow == nil {
            return nil, nil // Done
        }
        
        // Look up in hash table
        key := probeRow[s.probeKey]
        matches := s.hashTable[key]
        
        if len(matches) == 0 {
            if s.joinType == JoinTypeLeft || s.joinType == JoinTypeFull {
                // Emit probe row with nulls for build side
                return s.mergeRowsWithNulls(probeRow, nil), nil
            }
            continue // INNER JOIN: skip non-matching rows
        }
        
        s.currentProbeRow = probeRow
        s.matches = matches
        s.matchIdx = 0
    }
}

func (s *hashJoinStream) mergeRows(left, right Row) Row {
    result := make(Row)
    for k, v := range left {
        result[k] = v
    }
    for k, v := range right {
        result[k] = v
    }
    return result
}
```

---

#### 3.2 Join Strategy Selection

```go
// internal/federation/join/selector.go

type JoinStrategySelector struct {
    memoryLimit int64
}

func (s *JoinStrategySelector) SelectStrategy(
    leftStream ResultStream,
    rightStream ResultStream,
    join *JoinCondition,
) (JoinStrategy, *JoinConfig) {
    leftRows := leftStream.EstimatedRows()
    rightRows := rightStream.EstimatedRows()
    
    // Rule 1: If one side is small, use hash join with small side as build
    if leftRows >= 0 && leftRows < 100000 {
        return JoinStrategyHash, &JoinConfig{
            BuildSide: leftStream,
            ProbeSide: rightStream,
            BuildKey:  join.LeftCol,
            ProbeKey:  join.RightCol,
        }
    }
    if rightRows >= 0 && rightRows < 100000 {
        return JoinStrategyHash, &JoinConfig{
            BuildSide: rightStream,
            ProbeSide: leftStream,
            BuildKey:  join.RightCol,
            ProbeKey:  join.LeftCol,
        }
    }
    
    // Rule 2: If both sorted on join key, use merge join
    if s.isSorted(leftStream, join.LeftCol) && s.isSorted(rightStream, join.RightCol) {
        return JoinStrategyMerge, &JoinConfig{
            LeftStream:  leftStream,
            RightStream: rightStream,
            LeftKey:     join.LeftCol,
            RightKey:    join.RightCol,
        }
    }
    
    // Rule 3: Default to hash join with spill
    // Pick smaller estimated side as build
    if leftRows < rightRows || rightRows < 0 {
        return JoinStrategyHash, &JoinConfig{
            BuildSide: leftStream,
            ProbeSide: rightStream,
            BuildKey:  join.LeftCol,
            ProbeKey:  join.RightCol,
            AllowSpill: true,
        }
    }
    
    return JoinStrategyHash, &JoinConfig{
        BuildSide: rightStream,
        ProbeSide: leftStream,
        BuildKey:  join.RightCol,
        ProbeKey:  join.LeftCol,
        AllowSpill: true,
    }
}
```

---

#### 3.3 Multi-Way Join Executor

```go
// internal/federation/join/executor.go

type FederatedJoinExecutor struct {
    decomposed *DecomposedQuery
    adapters   map[string]adapters.EngineAdapter
    selector   *JoinStrategySelector
}

func (e *FederatedJoinExecutor) Execute(ctx context.Context) (ResultStream, error) {
    // Step 1: Execute all sub-queries in parallel
    subResults := make(map[string]ResultStream)
    var mu sync.Mutex
    var wg sync.WaitGroup
    var firstErr error
    
    for _, subQuery := range e.decomposed.SubQueries {
        wg.Add(1)
        go func(sq *SubQuery) {
            defer wg.Done()
            
            adapter := e.adapters[sq.Engine]
            plan := &planner.ExecutionPlan{
                LogicalPlan: &planner.LogicalPlan{RawSQL: sq.SQL},
            }
            
            result, err := adapter.Execute(ctx, plan)
            
            mu.Lock()
            defer mu.Unlock()
            
            if err != nil && firstErr == nil {
                firstErr = fmt.Errorf("sub-query on %s failed: %w", sq.Engine, err)
                return
            }
            
            subResults[sq.Engine] = result.AsStream()
        }(subQuery)
    }
    
    wg.Wait()
    
    if firstErr != nil {
        return nil, firstErr
    }
    
    // Step 2: Execute join plan
    var currentResult ResultStream
    
    for i, step := range e.decomposed.JoinPlan.Steps {
        var leftStream, rightStream ResultStream
        
        if i == 0 {
            leftStream = subResults[step.LeftInput]
            rightStream = subResults[step.RightInput]
        } else {
            leftStream = currentResult
            rightStream = subResults[step.RightInput]
        }
        
        // Select join strategy
        strategy, config := e.selector.SelectStrategy(
            leftStream, rightStream,
            &JoinCondition{
                LeftCol:  step.LeftKey,
                RightCol: step.RightKey,
                Type:     step.Type,
            },
        )
        
        // Execute join
        var err error
        currentResult, err = e.executeJoin(ctx, strategy, config)
        if err != nil {
            return nil, fmt.Errorf("join step %d failed: %w", i, err)
        }
    }
    
    // Step 3: Apply post-join operations
    if e.decomposed.PostJoinOps != nil {
        currentResult = e.applyPostJoinOps(currentResult, e.decomposed.PostJoinOps)
    }
    
    return currentResult, nil
}
```

---

### 4. Cost-Based Engine Selection

#### Objective
Choose the optimal engine for each sub-query based on cost estimates.

---

#### 4.1 Cost Model

```go
// internal/federation/cost/model.go

type CostModel struct {
    // Per-engine cost factors
    engineCosts map[string]*EngineCostFactors
}

type EngineCostFactors struct {
    // Fixed overhead per query (startup cost)
    QueryOverhead time.Duration
    
    // Cost per row scanned
    ScanCostPerRow float64
    
    // Cost per row transferred to gateway
    TransferCostPerRow float64
    
    // Cost per filter evaluation
    FilterCostPerRow float64
    
    // Cost per aggregation
    AggCostPerRow float64
    
    // Network latency (one-way)
    NetworkLatency time.Duration
}

// Default cost factors
var DefaultCostFactors = map[string]*EngineCostFactors{
    "duckdb": {
        QueryOverhead:      1 * time.Millisecond,
        ScanCostPerRow:     0.00001,  // Very fast local scans
        TransferCostPerRow: 0.0,      // No transfer (local)
        FilterCostPerRow:   0.00001,
        AggCostPerRow:      0.00002,
        NetworkLatency:     0,
    },
    "trino": {
        QueryOverhead:      100 * time.Millisecond,
        ScanCostPerRow:     0.0001,
        TransferCostPerRow: 0.001,
        FilterCostPerRow:   0.0001,
        AggCostPerRow:      0.0002,
        NetworkLatency:     5 * time.Millisecond,
    },
    "snowflake": {
        QueryOverhead:      500 * time.Millisecond,  // Cold start
        ScanCostPerRow:     0.0005,
        TransferCostPerRow: 0.005,
        FilterCostPerRow:   0.0001,
        AggCostPerRow:      0.0001,   // Very efficient aggregation
        NetworkLatency:     20 * time.Millisecond,
    },
}
```

---

#### 4.2 Cost Estimator

```go
// internal/federation/cost/estimator.go

type CostEstimator struct {
    model    *CostModel
    metadata storage.TableRepository
}

type QueryCost struct {
    Engine        string
    EstimatedTime time.Duration
    EstimatedRows int64
    Breakdown     *CostBreakdown
}

type CostBreakdown struct {
    ScanCost     time.Duration
    FilterCost   time.Duration
    AggCost      time.Duration
    TransferCost time.Duration
    Overhead     time.Duration
}

func (e *CostEstimator) EstimateCost(
    ctx context.Context,
    subQuery *SubQuery,
    engine string,
) (*QueryCost, error) {
    factors := e.model.engineCosts[engine]
    if factors == nil {
        factors = DefaultCostFactors["duckdb"] // Fallback
    }
    
    // Get table statistics
    var totalRows int64
    var selectivity float64 = 1.0
    
    for _, table := range subQuery.Tables {
        stats, err := e.metadata.GetTableStats(ctx, table.FullName())
        if err != nil {
            // Use defaults if stats unavailable
            totalRows += 1000000
            continue
        }
        totalRows += stats.RowCount
        
        // Estimate selectivity from predicates
        for _, pred := range subQuery.Predicates {
            if pred.Table == table.FullName() {
                selectivity *= e.estimatePredicateSelectivity(pred, stats)
            }
        }
    }
    
    // Calculate costs
    breakdown := &CostBreakdown{
        Overhead:     factors.QueryOverhead + factors.NetworkLatency,
        ScanCost:     time.Duration(float64(totalRows) * factors.ScanCostPerRow * float64(time.Microsecond)),
        FilterCost:   time.Duration(float64(totalRows) * factors.FilterCostPerRow * float64(time.Microsecond)),
        TransferCost: time.Duration(float64(totalRows) * selectivity * factors.TransferCostPerRow * float64(time.Microsecond)),
    }
    
    // Add aggregation cost if applicable
    if subQuery.HasAggregation {
        breakdown.AggCost = time.Duration(float64(totalRows) * selectivity * factors.AggCostPerRow * float64(time.Microsecond))
    }
    
    total := breakdown.Overhead + breakdown.ScanCost + breakdown.FilterCost + 
             breakdown.TransferCost + breakdown.AggCost
    
    return &QueryCost{
        Engine:        engine,
        EstimatedTime: total,
        EstimatedRows: int64(float64(totalRows) * selectivity),
        Breakdown:     breakdown,
    }, nil
}

func (e *CostEstimator) estimatePredicateSelectivity(
    pred *Predicate,
    stats *TableStats,
) float64 {
    // Simple heuristics
    switch pred.Operator {
    case "=":
        if stats.DistinctValues[pred.Column] > 0 {
            return 1.0 / float64(stats.DistinctValues[pred.Column])
        }
        return 0.1
    case "<", ">", "<=", ">=":
        return 0.33
    case "LIKE":
        if strings.HasPrefix(pred.Value.(string), "%") {
            return 0.5 // Leading wildcard: bad selectivity
        }
        return 0.1
    case "IN":
        values := pred.Value.([]interface{})
        if stats.DistinctValues[pred.Column] > 0 {
            return float64(len(values)) / float64(stats.DistinctValues[pred.Column])
        }
        return float64(len(values)) * 0.1
    default:
        return 0.5
    }
}
```

---

#### 4.3 Optimal Engine Selection

```go
// internal/federation/cost/optimizer.go

type QueryOptimizer struct {
    estimator *CostEstimator
    adapters  map[string]adapters.EngineAdapter
}

func (o *QueryOptimizer) SelectOptimalEngine(
    ctx context.Context,
    table *TableRef,
    predicates []*Predicate,
) (string, *QueryCost, error) {
    // Get available engines for this table format
    candidates := o.getCandidateEngines(table.Format)
    
    if len(candidates) == 0 {
        return "", nil, fmt.Errorf("no engine available for format %s", table.Format)
    }
    
    if len(candidates) == 1 {
        return candidates[0], nil, nil
    }
    
    // Estimate cost for each candidate
    var bestEngine string
    var bestCost *QueryCost
    
    for _, engine := range candidates {
        cost, err := o.estimator.EstimateCost(ctx, &SubQuery{
            Tables:     []*TableRef{table},
            Predicates: predicates,
        }, engine)
        if err != nil {
            continue
        }
        
        if bestCost == nil || cost.EstimatedTime < bestCost.EstimatedTime {
            bestEngine = engine
            bestCost = cost
        }
    }
    
    return bestEngine, bestCost, nil
}
```

---

### 5. Pushdown Optimization

#### Objective
Push as much work as possible to source engines.

---

#### 5.1 Pushdown Rules

```go
// internal/federation/pushdown/rules.go

type PushdownRule interface {
    // CanPush returns true if this operation can be pushed to the engine
    CanPush(op Operation, engine string) bool
    
    // Rewrite transforms the sub-query to include the pushed operation
    Rewrite(subQuery *SubQuery, op Operation) *SubQuery
}

// FilterPushdown pushes WHERE predicates to source engines
type FilterPushdown struct{}

func (f *FilterPushdown) CanPush(op Operation, engine string) bool {
    pred, ok := op.(*PredicateOp)
    if !ok {
        return false
    }
    
    // Can always push simple predicates
    if pred.IsSimple() {
        return true
    }
    
    // Check engine-specific support
    switch engine {
    case "duckdb":
        return true // DuckDB supports most predicates
    case "trino":
        return !pred.HasSubquery() // Trino: no correlated subqueries in pushdown
    case "snowflake":
        return true
    default:
        return false
    }
}

// ProjectionPushdown pushes column selection to source engines
type ProjectionPushdown struct{}

func (p *ProjectionPushdown) CanPush(op Operation, engine string) bool {
    _, ok := op.(*ProjectionOp)
    return ok // Always push projections
}

// AggregationPushdown pushes GROUP BY to source engines
type AggregationPushdown struct{}

func (a *AggregationPushdown) CanPush(op Operation, engine string) bool {
    agg, ok := op.(*AggregationOp)
    if !ok {
        return false
    }
    
    // Can push if:
    // 1. Single table (no joins needed first)
    // 2. All group-by columns from same table
    // 3. All aggregation inputs from same table
    return agg.IsSingleTable()
}

// LimitPushdown pushes LIMIT to source engines
type LimitPushdown struct{}

func (l *LimitPushdown) CanPush(op Operation, engine string) bool {
    limit, ok := op.(*LimitOp)
    if !ok {
        return false
    }
    
    // Can only push limit if:
    // 1. No join (limit applies to single source)
    // 2. Or limit applies to outer query after join
    return limit.IsFinal()
}
```

---

#### 5.2 Pushdown Optimizer

```go
// internal/federation/pushdown/optimizer.go

type PushdownOptimizer struct {
    rules []PushdownRule
}

func NewPushdownOptimizer() *PushdownOptimizer {
    return &PushdownOptimizer{
        rules: []PushdownRule{
            &FilterPushdown{},
            &ProjectionPushdown{},
            &AggregationPushdown{},
            &LimitPushdown{},
        },
    }
}

func (o *PushdownOptimizer) Optimize(
    decomposed *DecomposedQuery,
    analysis *QueryAnalysis,
) (*DecomposedQuery, error) {
    optimized := decomposed.Clone()
    
    // For each sub-query, try to push down operations
    for _, subQuery := range optimized.SubQueries {
        // Try each rule
        for _, rule := range o.rules {
            for _, op := range analysis.Operations {
                if rule.CanPush(op, subQuery.Engine) {
                    subQuery = rule.Rewrite(subQuery, op)
                }
            }
        }
    }
    
    return optimized, nil
}
```

---

### 6. Result Streaming

#### Objective
Stream results to clients without buffering entire result sets.

---

#### 6.1 Streaming Response

```go
// internal/gateway/streaming.go

func (g *Gateway) handleQueryStreaming(w http.ResponseWriter, r *http.Request) {
    // Set streaming headers
    w.Header().Set("Content-Type", "application/x-ndjson")
    w.Header().Set("Transfer-Encoding", "chunked")
    w.Header().Set("X-Content-Type-Options", "nosniff")
    
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming not supported", http.StatusInternalServerError)
        return
    }
    
    // Parse request
    var req QueryRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    
    // Execute query
    stream, err := g.executor.ExecuteStreaming(r.Context(), req.SQL)
    if err != nil {
        writeJSONError(w, err)
        return
    }
    defer stream.Close()
    
    // Write schema first
    schema := stream.Schema()
    schemaJSON, _ := json.Marshal(map[string]interface{}{
        "type":    "schema",
        "columns": schema.Columns,
    })
    w.Write(schemaJSON)
    w.Write([]byte("\n"))
    flusher.Flush()
    
    // Stream rows
    encoder := json.NewEncoder(w)
    for {
        row, err := stream.Next(r.Context())
        if err != nil {
            errJSON, _ := json.Marshal(map[string]interface{}{
                "type":  "error",
                "error": err.Error(),
            })
            w.Write(errJSON)
            w.Write([]byte("\n"))
            return
        }
        if row == nil {
            break
        }
        
        encoder.Encode(map[string]interface{}{
            "type": "row",
            "data": row,
        })
        flusher.Flush()
    }
    
    // Write completion
    doneJSON, _ := json.Marshal(map[string]interface{}{
        "type": "done",
    })
    w.Write(doneJSON)
    w.Write([]byte("\n"))
}
```

---

#### 6.2 Client-Side Streaming

```go
// internal/cli/streaming.go

func (c *GatewayClient) QueryStreaming(
    ctx context.Context,
    sql string,
    rowHandler func(Row) error,
) error {
    req, _ := http.NewRequestWithContext(ctx, "POST",
        c.baseURL+"/query/stream",
        strings.NewReader(fmt.Sprintf(`{"sql":%q}`, sql)))
    req.Header.Set("Content-Type", "application/json")
    
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    
    scanner := bufio.NewScanner(resp.Body)
    for scanner.Scan() {
        var msg map[string]interface{}
        if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
            continue
        }
        
        switch msg["type"] {
        case "schema":
            // Handle schema if needed
        case "row":
            if err := rowHandler(msg["data"].(map[string]interface{})); err != nil {
                return err
            }
        case "error":
            return fmt.Errorf("%s", msg["error"])
        case "done":
            return nil
        }
    }
    
    return scanner.Err()
}
```

---

## Out-of-Scope (Deferred to Future)

| Item | Reason |
|------|--------|
| Write operations (INSERT/UPDATE) | Read-only for now |
| Real-time streaming (Kafka) | Requires different architecture |
| Materialized views | Requires write support |
| Query caching | Performance optimization for later |
| Distributed gateway | Single instance for now |

---

## Exit Criteria

Phase 9 is complete when:

1. **Query Decomposition**
   - [ ] Multi-engine queries detected correctly
   - [ ] Sub-queries generated for each engine
   - [ ] Join conditions preserved

2. **Predicate Pushdown**
   - [ ] Filter predicates pushed to source engines
   - [ ] Projection (column selection) pushed down
   - [ ] Aggregations pushed when possible

3. **Join Execution**
   - [ ] Hash join works for small tables
   - [ ] Spill-to-disk works for large tables
   - [ ] Multi-way joins execute correctly

4. **Cost-Based Selection**
   - [ ] Cost estimates calculated per engine
   - [ ] Optimal engine selected based on cost
   - [ ] Join ordering optimized

5. **Result Streaming**
   - [ ] Results stream to client without full buffering
   - [ ] Client can consume rows incrementally

6. **Integration Tests**
   - [ ] Cross-engine JOIN between Trino and Snowflake
   - [ ] Cross-engine JOIN with filter pushdown
   - [ ] Large result set with spill-to-disk

---

## Appendix A: Example Queries

### Simple Cross-Engine Join
```sql
-- Iceberg table (Trino) joined with Snowflake table
SELECT 
    s.order_id,
    s.amount,
    c.name,
    c.tier
FROM iceberg.analytics.sales s
JOIN snowflake.crm.customers c ON s.customer_id = c.id
WHERE s.order_date > '2026-01-01'
  AND c.tier = 'premium'
```

**Decomposed:**
```sql
-- SubQuery 1 (Trino)
SELECT order_id, amount, customer_id 
FROM iceberg.analytics.sales 
WHERE order_date > '2026-01-01'

-- SubQuery 2 (Snowflake)
SELECT id, name, tier 
FROM snowflake.crm.customers 
WHERE tier = 'premium'

-- Join in Gateway
HASH JOIN ON customer_id = id
```

### Aggregation with Cross-Engine Join
```sql
SELECT 
    c.region,
    SUM(s.amount) as total_sales
FROM iceberg.analytics.sales s
JOIN duckdb.reference.regions c ON s.region_id = c.id
GROUP BY c.region
ORDER BY total_sales DESC
LIMIT 10
```

**Decomposed:**
```sql
-- SubQuery 1 (Trino)
SELECT region_id, amount FROM iceberg.analytics.sales

-- SubQuery 2 (DuckDB)
SELECT id, region FROM duckdb.reference.regions

-- Join in Gateway
HASH JOIN ON region_id = id

-- Post-join operations (Gateway)
GROUP BY region
ORDER BY total_sales DESC
LIMIT 10
```

---

## Appendix B: Configuration Reference

```yaml
# Complete Phase 9 configuration

federation:
  enabled: true
  
  # Query execution
  execution:
    parallel_subqueries: true
    max_parallel: 4
    subquery_timeout: 5m
    
  # Intermediate storage
  intermediate_storage:
    memory_threshold_rows: 100000
    memory_threshold_bytes: 100MB
    spill_dir: /tmp/canonic-spill
    auto_cleanup: true
    
  # Join settings
  join:
    default_strategy: hash
    hash_table_size_limit: 500MB
    enable_spill: true
    
  # Cost model
  cost:
    enabled: true
    use_statistics: true
    
  # Pushdown
  pushdown:
    filters: true
    projections: true
    aggregations: true
    limits: true
    
  # Streaming
  streaming:
    enabled: true
    buffer_size: 1000
```

---

## Appendix C: Performance Expectations

| Scenario | Expected Latency |
|----------|------------------|
| Small-small join (< 10K rows each) | < 1 second |
| Small-large join (10K × 1M rows) | 5-30 seconds |
| Large-large join (1M × 1M rows, spill) | 1-5 minutes |
| With predicate pushdown | 30-50% faster |
| With aggregation pushdown | 50-80% faster |

**Note:** Actual performance depends heavily on network latency, engine performance, and data characteristics.
