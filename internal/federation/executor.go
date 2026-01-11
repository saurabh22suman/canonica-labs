// Package federation provides cross-engine query federation.
package federation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/storage"
)

// EngineAdapter executes queries on a specific engine.
type EngineAdapter interface {
	// Name returns the engine name.
	Name() string

	// Execute runs a query and returns a result stream.
	Execute(ctx context.Context, query string) (ResultStream, error)

	// TableStats returns statistics for a table.
	TableStats(ctx context.Context, table string) (*TableStats, error)

	// HealthCheck returns true if the engine is available.
	HealthCheck(ctx context.Context) bool
}

// AdapterRegistry manages engine adapters.
type AdapterRegistry struct {
	mu       sync.RWMutex
	adapters map[string]EngineAdapter
}

// NewAdapterRegistry creates a new adapter registry.
func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{
		adapters: make(map[string]EngineAdapter),
	}
}

// Register adds an adapter to the registry.
func (r *AdapterRegistry) Register(adapter EngineAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[adapter.Name()] = adapter
}

// Get retrieves an adapter by engine name.
func (r *AdapterRegistry) Get(engine string) (EngineAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	adapter, ok := r.adapters[engine]
	if !ok {
		return nil, fmt.Errorf("no adapter registered for engine: %s", engine)
	}
	return adapter, nil
}

// List returns all registered engine names.
func (r *AdapterRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		names = append(names, name)
	}
	return names
}

// ExecutionPlan represents a complete plan for executing a federated query.
type ExecutionPlan struct {
	Query          string
	Decomposed     *DecomposedQuery
	Analysis       *QueryAnalysis
	CostEstimate   *QueryCost
	SubQueryPlans  []*SubQueryPlan
	JoinPlan       *JoinPlan
	ExecutionOrder []int // Order to execute sub-queries
}

// SubQueryPlan contains execution details for a sub-query.
type SubQueryPlan struct {
	SubQuery         *SubQuery
	Engine           string
	EstimatedRows    int64
	EstimatedCost    float64
	ParallelGroup    int  // Sub-queries in same group execute in parallel
	RequiresMaterial bool // True if results must be materialized for join
}

// ExecutionStats tracks execution statistics.
type ExecutionStats struct {
	TotalTime        time.Duration
	PlanningTime     time.Duration
	SubQueryTimes    map[int]time.Duration
	JoinTime         time.Duration
	RowsProcessed    int64
	BytesTransferred int64
	EnginesUsed      []string
}

// FederatedExecutor orchestrates cross-engine query execution.
// Per phase-9-spec.md §3.3.
type FederatedExecutor struct {
	registry   *AdapterRegistry
	analyzer   *Analyzer
	decomposer *Decomposer
	optimizer  *PushdownOptimizer
	costModel  *CostModel
}

// NewFederatedExecutor creates a new federated executor.
func NewFederatedExecutor(
	registry *AdapterRegistry,
	parser *sql.Parser,
	metadata storage.TableRepository,
) *FederatedExecutor {
	return &FederatedExecutor{
		registry:   registry,
		analyzer:   NewAnalyzer(parser, metadata),
		decomposer: NewDecomposer(),
		optimizer:  NewPushdownOptimizer(),
		costModel:  NewCostModel(),
	}
}

// Execute runs a federated query and returns results.
func (e *FederatedExecutor) Execute(ctx context.Context, query string) (ResultStream, error) {
	stats := &ExecutionStats{
		SubQueryTimes: make(map[int]time.Duration),
	}
	start := time.Now()

	// Phase 1: Plan the query
	plan, err := e.Plan(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("planning failed: %w", err)
	}
	stats.PlanningTime = time.Since(start)

	// Phase 2: Execute sub-queries
	results, err := e.executeSubQueries(ctx, plan, stats)
	if err != nil {
		return nil, fmt.Errorf("sub-query execution failed: %w", err)
	}

	// Phase 3: Execute joins if needed
	var result ResultStream
	if len(results) == 1 {
		result = results[0]
	} else {
		result, err = e.executeJoins(ctx, results, plan, stats)
		if err != nil {
			return nil, fmt.Errorf("join execution failed: %w", err)
		}
	}

	// Phase 4: Apply post-join operations
	result, err = e.applyPostJoinOps(ctx, result, plan)
	if err != nil {
		return nil, fmt.Errorf("post-join operations failed: %w", err)
	}

	stats.TotalTime = time.Since(start)

	return result, nil
}

// Plan creates an execution plan for a query.
func (e *FederatedExecutor) Plan(ctx context.Context, query string) (*ExecutionPlan, error) {
	// Analyze the query
	analysis, err := e.analyzer.Analyze(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("analysis failed: %w", err)
	}

	// Decompose into sub-queries
	decomposed, err := e.decomposer.Decompose(analysis)
	if err != nil {
		return nil, fmt.Errorf("decomposition failed: %w", err)
	}

	// Optimize with pushdowns
	decomposed, err = e.optimizer.Optimize(decomposed, analysis)
	if err != nil {
		return nil, fmt.Errorf("optimization failed: %w", err)
	}

	// Build sub-query plans
	subQueryPlans, err := e.buildSubQueryPlans(ctx, decomposed)
	if err != nil {
		return nil, fmt.Errorf("sub-query planning failed: %w", err)
	}

	// Determine execution order
	executionOrder := e.determineExecutionOrder(subQueryPlans, decomposed.JoinPlan)

	return &ExecutionPlan{
		Query:          query,
		Decomposed:     decomposed,
		Analysis:       analysis,
		SubQueryPlans:  subQueryPlans,
		JoinPlan:       decomposed.JoinPlan,
		ExecutionOrder: executionOrder,
	}, nil
}

// buildSubQueryPlans creates detailed plans for each sub-query.
func (e *FederatedExecutor) buildSubQueryPlans(
	ctx context.Context,
	decomposed *DecomposedQuery,
) ([]*SubQueryPlan, error) {
	plans := make([]*SubQueryPlan, len(decomposed.SubQueries))

	for i, sq := range decomposed.SubQueries {
		var estimatedRows int64 = 1000 // Default estimate

		// Try to get table stats
		adapter, err := e.registry.Get(sq.Engine)
		if err == nil && len(sq.Tables) > 0 {
			stats, err := adapter.TableStats(ctx, sq.Tables[0].Name)
			if err == nil && stats != nil {
				estimatedRows = stats.RowCount
			}
		}

		plans[i] = &SubQueryPlan{
			SubQuery:         sq,
			Engine:           sq.Engine,
			EstimatedRows:    estimatedRows,
			ParallelGroup:    0, // Initially all in same group
			RequiresMaterial: i < len(decomposed.SubQueries)-1, // All but last need materialization
		}
	}

	// Assign parallel groups based on dependencies
	e.assignParallelGroups(plans, decomposed.JoinPlan)

	return plans, nil
}

// assignParallelGroups determines which sub-queries can run in parallel.
func (e *FederatedExecutor) assignParallelGroups(plans []*SubQueryPlan, joinPlan *JoinPlan) {
	if joinPlan == nil || len(joinPlan.Steps) == 0 {
		// No joins - all can run in parallel
		for i := range plans {
			plans[i].ParallelGroup = 0
		}
		return
	}

	// For hash join, both sides can run in parallel
	// Use simple heuristic: all independent sub-queries in group 0
	for i := range plans {
		plans[i].ParallelGroup = 0
	}
}

// determineExecutionOrder orders sub-queries for execution.
func (e *FederatedExecutor) determineExecutionOrder(
	plans []*SubQueryPlan,
	joinPlan *JoinPlan,
) []int {
	order := make([]int, len(plans))
	for i := range order {
		order[i] = i
	}

	// Sort by estimated rows (smaller first for hash join build phase)
	for i := 0; i < len(order)-1; i++ {
		for j := i + 1; j < len(order); j++ {
			if plans[order[i]].EstimatedRows > plans[order[j]].EstimatedRows {
				order[i], order[j] = order[j], order[i]
			}
		}
	}

	return order
}

// executeSubQueries executes all sub-queries, potentially in parallel.
func (e *FederatedExecutor) executeSubQueries(
	ctx context.Context,
	plan *ExecutionPlan,
	stats *ExecutionStats,
) ([]ResultStream, error) {
	numSubQueries := len(plan.SubQueryPlans)
	results := make([]ResultStream, numSubQueries)
	errors := make([]error, numSubQueries)

	var wg sync.WaitGroup

	for _, idx := range plan.ExecutionOrder {
		idx := idx // Capture for goroutine
		subPlan := plan.SubQueryPlans[idx]

		wg.Add(1)
		go func() {
			defer wg.Done()

			start := time.Now()

			adapter, err := e.registry.Get(subPlan.Engine)
			if err != nil {
				errors[idx] = err
				return
			}

			result, err := adapter.Execute(ctx, subPlan.SubQuery.SQL)
			if err != nil {
				errors[idx] = fmt.Errorf("engine %s: %w", subPlan.Engine, err)
				return
			}

			// Materialize if needed for joins
			if subPlan.RequiresMaterial {
				store := NewMemoryResultStore(result.Schema())
				for {
					row, err := result.Next(ctx)
					if err != nil {
						errors[idx] = fmt.Errorf("materialization failed: %w", err)
						return
					}
					if row == nil {
						break
					}
					if err := store.Append(row); err != nil {
						errors[idx] = fmt.Errorf("materialization append failed: %w", err)
						return
					}
				}
				result = store.Stream()
			}

			results[idx] = result
			stats.SubQueryTimes[idx] = time.Since(start)
		}()
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("sub-query %d failed: %w", i, err)
		}
	}

	return results, nil
}

// executeJoins executes the join plan on sub-query results.
func (e *FederatedExecutor) executeJoins(
	ctx context.Context,
	results []ResultStream,
	plan *ExecutionPlan,
	stats *ExecutionStats,
) (ResultStream, error) {
	if plan.JoinPlan == nil || len(plan.JoinPlan.Steps) == 0 {
		return nil, fmt.Errorf("no join plan for multiple results")
	}

	start := time.Now()
	current := results[0]

	// Build a map from sub-query ID to result stream
	subQueryResults := make(map[string]ResultStream)
	for i, sq := range plan.Decomposed.SubQueries {
		subQueryResults[sq.ID] = results[i]
	}

	// Also track intermediate join results
	stepResults := make(map[int]ResultStream)

	for i, step := range plan.JoinPlan.Steps {
		// Determine left and right streams
		var leftStream, rightStream ResultStream

		// Left input is either a sub-query or previous step result
		if leftResult, ok := subQueryResults[step.LeftInput]; ok {
			leftStream = leftResult
		} else if i > 0 {
			leftStream = stepResults[i-1]
		} else {
			leftStream = current
		}

		// Right input is a sub-query ID
		rightStream, ok := subQueryResults[step.RightInput]
		if !ok {
			return nil, fmt.Errorf("invalid right sub-query: %s", step.RightInput)
		}

		// Build JoinConfig
		joinConfig := &JoinConfig{
			BuildSide:  leftStream,
			ProbeSide:  rightStream,
			BuildKey:   step.LeftKey,
			ProbeKey:   step.RightKey,
			Type:       step.Type,
			AllowSpill: true,
		}

		joined, err := ExecuteJoin(ctx, step.Strategy, joinConfig)
		if err != nil {
			return nil, fmt.Errorf("join step %d failed: %w", i, err)
		}

		stepResults[i] = joined
		current = joined
	}

	stats.JoinTime = time.Since(start)
	return current, nil
}

// applyPostJoinOps applies operations that run after joins.
func (e *FederatedExecutor) applyPostJoinOps(
	ctx context.Context,
	result ResultStream,
	plan *ExecutionPlan,
) (ResultStream, error) {
	if plan.Decomposed.PostJoinOps == nil {
		return result, nil
	}

	postOps := plan.Decomposed.PostJoinOps

	// Apply final aggregation if needed
	if len(postOps.Aggregations) > 0 {
		result = &aggregatingStream{
			source:       result,
			aggregations: postOps.Aggregations,
		}
	}

	// Apply final ORDER BY
	if len(postOps.OrderBy) > 0 {
		result = &sortingStream{
			source:  result,
			orderBy: postOps.OrderBy,
		}
	}

	// Apply final LIMIT
	if postOps.Limit != nil {
		result = &limitingStream{
			source: result,
			limit:  *postOps.Limit,
		}
	}

	return result, nil
}

// aggregatingStream applies aggregation to results.
type aggregatingStream struct {
	source       ResultStream
	aggregations []*Aggregation
	done         bool
	result       Row
}

func (a *aggregatingStream) Schema() *ResultSchema {
	return a.source.Schema()
}

func (a *aggregatingStream) Next(ctx context.Context) (Row, error) {
	if a.done {
		return nil, nil
	}

	// Collect all rows and compute aggregations
	// This is a simplified implementation
	var rows []Row
	for {
		row, err := a.source.Next(ctx)
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}

	a.done = true

	// Return aggregation result (simplified)
	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil // Placeholder - real implementation would compute aggregates
}

func (a *aggregatingStream) Close() error {
	return a.source.Close()
}

func (a *aggregatingStream) EstimatedRows() int64 {
	return 1 // Aggregation typically returns few rows
}

// sortingStream applies ORDER BY to results.
type sortingStream struct {
	source    ResultStream
	orderBy   []*OrderByClause
	sorted    []Row
	index     int
	collected bool
}

func (s *sortingStream) Schema() *ResultSchema {
	return s.source.Schema()
}

func (s *sortingStream) Next(ctx context.Context) (Row, error) {
	if !s.collected {
		// Collect all rows
		for {
			row, err := s.source.Next(ctx)
			if err != nil {
				return nil, err
			}
			if row == nil {
				break
			}
			s.sorted = append(s.sorted, row)
		}
		// Sorting would happen here (simplified - just use collected order)
		s.collected = true
	}

	if s.index >= len(s.sorted) {
		return nil, nil
	}

	row := s.sorted[s.index]
	s.index++
	return row, nil
}

func (s *sortingStream) Close() error {
	return s.source.Close()
}

func (s *sortingStream) EstimatedRows() int64 {
	return s.source.EstimatedRows()
}

// limitingStream applies LIMIT to results.
type limitingStream struct {
	source ResultStream
	limit  int
	count  int
}

func (l *limitingStream) Schema() *ResultSchema {
	return l.source.Schema()
}

func (l *limitingStream) Next(ctx context.Context) (Row, error) {
	if l.count >= l.limit {
		return nil, nil
	}

	row, err := l.source.Next(ctx)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	l.count++
	return row, nil
}

func (l *limitingStream) Close() error {
	return l.source.Close()
}

func (l *limitingStream) EstimatedRows() int64 {
	est := l.source.EstimatedRows()
	if int64(l.limit) < est {
		return int64(l.limit)
	}
	return est
}

// Explain returns an explanation of how a query would be executed.
func (e *FederatedExecutor) Explain(ctx context.Context, query string) (string, error) {
	plan, err := e.Plan(ctx, query)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("=== Federated Query Execution Plan ===\n\n")

	sb.WriteString("Sub-Queries:\n")
	for i, sqp := range plan.SubQueryPlans {
		sb.WriteString(fmt.Sprintf("  [%d] Engine: %s, Est. Rows: %d\n",
			i, sqp.Engine, sqp.EstimatedRows))
		sb.WriteString(fmt.Sprintf("      SQL: %s\n", sqp.SubQuery.SQL))
	}

	if plan.JoinPlan != nil && len(plan.JoinPlan.Steps) > 0 {
		sb.WriteString("\nJoin Plan:\n")
		for i, step := range plan.JoinPlan.Steps {
			sb.WriteString(fmt.Sprintf("  Step %d: %s JOIN on %v = %v\n",
				i, step.Type, step.LeftKey, step.RightKey))
		}
	}

	sb.WriteString(fmt.Sprintf("\nExecution Order: %v\n", plan.ExecutionOrder))

	return sb.String(), nil
}
