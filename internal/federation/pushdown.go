// Package federation provides cross-engine query federation.
package federation

import (
	"fmt"
	"strings"
)

// Operation represents a query operation that might be pushed down.
type Operation interface {
	Type() string
}

// PredicateOp represents a filter predicate operation.
type PredicateOp struct {
	predicate *Predicate
}

// Type returns "predicate".
func (p *PredicateOp) Type() string {
	return "predicate"
}

// IsSimple returns true if this is a simple predicate.
func (p *PredicateOp) IsSimple() bool {
	// Simple predicates don't have subqueries or complex expressions
	return !strings.Contains(strings.ToUpper(p.predicate.Raw), "SELECT")
}

// HasSubquery returns true if predicate contains a subquery.
func (p *PredicateOp) HasSubquery() bool {
	return strings.Contains(strings.ToUpper(p.predicate.Raw), "SELECT")
}

// ProjectionOp represents a column projection operation.
type ProjectionOp struct {
	columns []string
}

// Type returns "projection".
func (p *ProjectionOp) Type() string {
	return "projection"
}

// AggregationOp represents an aggregation operation.
type AggregationOp struct {
	aggregations []*Aggregation
	groupBy      []string
	tables       []*TableRef
}

// Type returns "aggregation".
func (a *AggregationOp) Type() string {
	return "aggregation"
}

// IsSingleTable returns true if aggregation involves only one table.
func (a *AggregationOp) IsSingleTable() bool {
	return len(a.tables) == 1
}

// LimitOp represents a LIMIT operation.
type LimitOp struct {
	limit   int
	isFinal bool
}

// Type returns "limit".
func (l *LimitOp) Type() string {
	return "limit"
}

// IsFinal returns true if this limit applies to the final result.
func (l *LimitOp) IsFinal() bool {
	return l.isFinal
}

// PushdownRule defines a rule for pushing operations to source engines.
// Per phase-9-spec.md §5.1.
type PushdownRule interface {
	// CanPush returns true if this operation can be pushed to the engine.
	CanPush(op Operation, engine string) bool

	// Rewrite transforms the sub-query to include the pushed operation.
	Rewrite(subQuery *SubQuery, op Operation) *SubQuery
}

// FilterPushdown pushes WHERE predicates to source engines.
type FilterPushdown struct{}

// CanPush checks if filter can be pushed to engine.
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
		return !pred.HasSubquery() // Trino: no correlated subqueries
	case "spark":
		return !pred.HasSubquery()
	case "snowflake":
		return true
	case "bigquery":
		return true
	case "redshift":
		return !pred.HasSubquery()
	default:
		return false
	}
}

// Rewrite adds the predicate to the sub-query WHERE clause.
func (f *FilterPushdown) Rewrite(subQuery *SubQuery, op Operation) *SubQuery {
	pred, ok := op.(*PredicateOp)
	if !ok {
		return subQuery
	}

	// Clone sub-query
	result := *subQuery
	result.Predicates = append(result.Predicates, pred.predicate)

	// Rebuild SQL with new predicate
	if strings.Contains(strings.ToUpper(result.SQL), "WHERE") {
		result.SQL = result.SQL + " AND " + pred.predicate.Raw
	} else {
		result.SQL = result.SQL + " WHERE " + pred.predicate.Raw
	}

	return &result
}

// ProjectionPushdown pushes column selection to source engines.
type ProjectionPushdown struct{}

// CanPush always returns true for projections.
func (p *ProjectionPushdown) CanPush(op Operation, engine string) bool {
	_, ok := op.(*ProjectionOp)
	return ok
}

// Rewrite updates the SELECT clause with specific columns.
func (p *ProjectionPushdown) Rewrite(subQuery *SubQuery, op Operation) *SubQuery {
	proj, ok := op.(*ProjectionOp)
	if !ok {
		return subQuery
	}

	result := *subQuery
	result.Columns = proj.columns

	// Rebuild SQL SELECT clause
	selectIdx := strings.Index(strings.ToUpper(result.SQL), "SELECT")
	fromIdx := strings.Index(strings.ToUpper(result.SQL), "FROM")

	if selectIdx >= 0 && fromIdx > selectIdx {
		result.SQL = result.SQL[:selectIdx+6] + " " +
			strings.Join(proj.columns, ", ") + " " +
			result.SQL[fromIdx:]
	}

	return &result
}

// AggregationPushdown pushes GROUP BY to source engines.
type AggregationPushdown struct{}

// CanPush checks if aggregation can be pushed.
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

// Rewrite adds GROUP BY to the sub-query.
func (a *AggregationPushdown) Rewrite(subQuery *SubQuery, op Operation) *SubQuery {
	agg, ok := op.(*AggregationOp)
	if !ok {
		return subQuery
	}

	result := *subQuery

	// Build aggregation columns for SELECT
	var selectParts []string
	for _, col := range agg.groupBy {
		selectParts = append(selectParts, col)
	}
	for _, aggFn := range agg.aggregations {
		selectParts = append(selectParts, aggFn.Raw)
	}

	// Rebuild SQL with aggregation
	selectIdx := strings.Index(strings.ToUpper(result.SQL), "SELECT")
	fromIdx := strings.Index(strings.ToUpper(result.SQL), "FROM")

	if selectIdx >= 0 && fromIdx > selectIdx {
		result.SQL = result.SQL[:selectIdx+6] + " " +
			strings.Join(selectParts, ", ") + " " +
			result.SQL[fromIdx:]
	}

	// Add GROUP BY
	if len(agg.groupBy) > 0 {
		result.SQL = result.SQL + " GROUP BY " + strings.Join(agg.groupBy, ", ")
	}

	return &result
}

// LimitPushdown pushes LIMIT to source engines.
type LimitPushdown struct{}

// CanPush checks if limit can be pushed.
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

// Rewrite adds LIMIT to the sub-query.
func (l *LimitPushdown) Rewrite(subQuery *SubQuery, op Operation) *SubQuery {
	limit, ok := op.(*LimitOp)
	if !ok {
		return subQuery
	}

	result := *subQuery

	// Add LIMIT clause
	if !strings.Contains(strings.ToUpper(result.SQL), "LIMIT") {
		result.SQL = result.SQL + fmt.Sprintf(" LIMIT %d", limit.limit)
	}

	return &result
}

// PushdownOptimizer optimizes queries by pushing operations to source engines.
// Per phase-9-spec.md §5.2.
type PushdownOptimizer struct {
	rules []PushdownRule
}

// NewPushdownOptimizer creates a new pushdown optimizer.
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

// Optimize applies pushdown optimizations to a decomposed query.
func (o *PushdownOptimizer) Optimize(
	decomposed *DecomposedQuery,
	analysis *QueryAnalysis,
) (*DecomposedQuery, error) {
	// Validate input
	if decomposed == nil {
		return nil, fmt.Errorf("pushdown optimizer: nil decomposed query")
	}
	if analysis == nil {
		return nil, fmt.Errorf("pushdown optimizer: nil analysis")
	}

	optimized := o.cloneDecomposed(decomposed)

	// Extract operations from analysis
	operations := o.extractOperations(analysis)

	// For each sub-query, try to push down operations
	for i, subQuery := range optimized.SubQueries {
		for _, rule := range o.rules {
			for _, op := range operations {
				if rule.CanPush(op, subQuery.Engine) {
					optimized.SubQueries[i] = rule.Rewrite(subQuery, op)
				}
			}
		}
	}

	return optimized, nil
}

// cloneDecomposed creates a deep copy of a decomposed query.
func (o *PushdownOptimizer) cloneDecomposed(d *DecomposedQuery) *DecomposedQuery {
	result := &DecomposedQuery{
		OriginalSQL: d.OriginalSQL,
		SubQueries:  make([]*SubQuery, len(d.SubQueries)),
		JoinPlan:    d.JoinPlan,
		PostJoinOps: d.PostJoinOps,
	}

	for i, sq := range d.SubQueries {
		clone := *sq
		clone.Tables = make([]*TableRef, len(sq.Tables))
		copy(clone.Tables, sq.Tables)
		clone.Predicates = make([]*Predicate, len(sq.Predicates))
		copy(clone.Predicates, sq.Predicates)
		clone.Columns = make([]string, len(sq.Columns))
		copy(clone.Columns, sq.Columns)
		result.SubQueries[i] = &clone
	}

	return result
}

// extractOperations converts analysis results to operations.
func (o *PushdownOptimizer) extractOperations(analysis *QueryAnalysis) []Operation {
	var ops []Operation

	// Add predicate operations
	for _, preds := range analysis.PushablePredicates {
		for _, pred := range preds {
			ops = append(ops, &PredicateOp{predicate: pred})
		}
	}

	// Add aggregation operations
	if len(analysis.Aggregations) > 0 {
		// Extract GROUP BY columns from query (simplified)
		ops = append(ops, &AggregationOp{
			aggregations: analysis.Aggregations,
		})
	}

	// Add limit operation
	if analysis.Limit != nil {
		ops = append(ops, &LimitOp{
			limit:   *analysis.Limit,
			isFinal: true, // After joins
		})
	}

	return ops
}

// PushdownStats tracks pushdown optimization statistics.
type PushdownStats struct {
	FiltersPushed      int
	ProjectionsPushed  int
	AggregationsPushed int
	LimitsPushed       int
}

// AnalyzePushdown analyzes what was pushed down.
func (o *PushdownOptimizer) AnalyzePushdown(
	original *DecomposedQuery,
	optimized *DecomposedQuery,
) *PushdownStats {
	stats := &PushdownStats{}

	for i, sq := range optimized.SubQueries {
		origSQ := original.SubQueries[i]

		// Count additional predicates
		stats.FiltersPushed += len(sq.Predicates) - len(origSQ.Predicates)

		// Check for aggregations (simplified)
		if strings.Contains(strings.ToUpper(sq.SQL), "GROUP BY") &&
			!strings.Contains(strings.ToUpper(origSQ.SQL), "GROUP BY") {
			stats.AggregationsPushed++
		}

		// Check for limits
		if strings.Contains(strings.ToUpper(sq.SQL), "LIMIT") &&
			!strings.Contains(strings.ToUpper(origSQ.SQL), "LIMIT") {
			stats.LimitsPushed++
		}
	}

	return stats
}
