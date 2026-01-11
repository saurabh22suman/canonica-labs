// Package federation provides cross-engine query federation.
package federation

import (
	"fmt"
	"strings"
)

// JoinStrategy represents the join execution strategy.
type JoinStrategy string

const (
	JoinStrategyHash       JoinStrategy = "hash"
	JoinStrategyMerge      JoinStrategy = "merge"
	JoinStrategyNestedLoop JoinStrategy = "nested_loop"
)

// SubQuery represents a sub-query to be executed on a single engine.
type SubQuery struct {
	// ID is a unique identifier for this sub-query.
	ID string

	// Engine is the target execution engine.
	Engine string

	// SQL is the sub-query SQL text.
	SQL string

	// Tables are the tables involved in this sub-query.
	Tables []*TableRef

	// Predicates are the predicates pushed to this sub-query.
	Predicates []*Predicate

	// Columns are the columns to select.
	Columns []string

	// EstimatedRows is the estimated row count (-1 if unknown).
	EstimatedRows int64
}

// JoinStep represents a single join operation in the join plan.
type JoinStep struct {
	// StepID is the step identifier.
	StepID int

	// Type is the join type.
	Type JoinType

	// LeftInput is the sub-query ID or previous step ID.
	LeftInput string

	// RightInput is the sub-query ID.
	RightInput string

	// LeftKey is the join key on the left side.
	LeftKey string

	// RightKey is the join key on the right side.
	RightKey string

	// Strategy is the join execution strategy.
	Strategy JoinStrategy
}

// JoinPlan represents the complete join execution plan.
type JoinPlan struct {
	Steps []JoinStep
}

// PostJoinOperations are operations applied after all joins.
type PostJoinOperations struct {
	Aggregations []*Aggregation
	OrderBy      []*OrderByClause
	Limit        *int
}

// DecomposedQuery is the result of decomposing a cross-engine query.
// Per phase-9-spec.md §1.4-1.5.
type DecomposedQuery struct {
	// OriginalSQL is the original query.
	OriginalSQL string

	// SubQueries are the sub-queries for each engine.
	SubQueries []*SubQuery

	// JoinPlan is the plan for joining sub-query results.
	JoinPlan *JoinPlan

	// PostJoinOps are operations after joins (aggregation, sort, limit).
	PostJoinOps *PostJoinOperations
}

// Decomposer decomposes multi-engine queries into sub-queries.
type Decomposer struct{}

// NewDecomposer creates a new query decomposer.
func NewDecomposer() *Decomposer {
	return &Decomposer{}
}

// Decompose splits a cross-engine query into sub-queries.
// Per phase-9-spec.md §1.5.
func (d *Decomposer) Decompose(analysis *QueryAnalysis) (*DecomposedQuery, error) {
	if !analysis.IsCrossEngine {
		return nil, fmt.Errorf("decomposer: not a cross-engine query")
	}

	if len(analysis.TablesByEngine) == 0 {
		return nil, fmt.Errorf("decomposer: no tables found")
	}

	result := &DecomposedQuery{
		OriginalSQL: analysis.OriginalSQL,
		SubQueries:  make([]*SubQuery, 0),
	}

	// Generate sub-query for each engine
	subQueryID := 0
	engineToSubQuery := make(map[string]string) // engine -> subQuery ID

	for engine, tables := range analysis.TablesByEngine {
		subQuery, err := d.generateSubQuery(subQueryID, engine, tables, analysis)
		if err != nil {
			return nil, fmt.Errorf("decomposer: failed to generate sub-query for %s: %w", engine, err)
		}
		result.SubQueries = append(result.SubQueries, subQuery)
		engineToSubQuery[engine] = subQuery.ID
		subQueryID++
	}

	// Generate join plan
	joinPlan, err := d.generateJoinPlan(analysis, result.SubQueries)
	if err != nil {
		return nil, fmt.Errorf("decomposer: failed to generate join plan: %w", err)
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

// generateSubQuery generates a sub-query for a specific engine.
func (d *Decomposer) generateSubQuery(
	id int,
	engine string,
	tables []*TableRef,
	analysis *QueryAnalysis,
) (*SubQuery, error) {
	subQueryID := fmt.Sprintf("sq_%d_%s", id, engine)

	// Collect required columns for this engine's tables
	var columns []string
	var tableAliases []string

	for _, table := range tables {
		// Get columns required for this table
		tableCols := analysis.RequiredColumns[table.FullName()]
		alias := table.DisplayName()
		tableAliases = append(tableAliases, alias)

		for _, col := range tableCols {
			colRef := fmt.Sprintf("%s.%s", alias, col)
			if !contains(columns, colRef) {
				columns = append(columns, colRef)
			}
		}

		// Ensure join keys are included
		for _, join := range analysis.Joins {
			if join.LeftTable == alias || join.LeftTable == table.Name {
				colRef := fmt.Sprintf("%s.%s", alias, join.LeftCol)
				if !contains(columns, colRef) {
					columns = append(columns, colRef)
				}
			}
			if join.RightTable == alias || join.RightTable == table.Name {
				colRef := fmt.Sprintf("%s.%s", alias, join.RightCol)
				if !contains(columns, colRef) {
					columns = append(columns, colRef)
				}
			}
		}
	}

	// If no columns found, select all
	if len(columns) == 0 {
		for _, table := range tables {
			columns = append(columns, table.DisplayName()+".*")
		}
	}

	// Build FROM clause
	var fromParts []string
	for _, table := range tables {
		if table.Alias != "" && table.Alias != table.Name {
			fromParts = append(fromParts, fmt.Sprintf("%s AS %s", table.FullName(), table.Alias))
		} else {
			fromParts = append(fromParts, table.FullName())
		}
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

	// Collect predicates
	var predicates []*Predicate
	for _, table := range tables {
		predicates = append(predicates, analysis.PushablePredicates[table.FullName()]...)
	}

	return &SubQuery{
		ID:            subQueryID,
		Engine:        engine,
		SQL:           sql,
		Tables:        tables,
		Predicates:    predicates,
		Columns:       columns,
		EstimatedRows: -1, // Unknown at decomposition time
	}, nil
}

// generateJoinPlan creates a plan for joining sub-query results.
func (d *Decomposer) generateJoinPlan(
	analysis *QueryAnalysis,
	subQueries []*SubQuery,
) (*JoinPlan, error) {
	if len(subQueries) < 2 {
		return nil, fmt.Errorf("join plan requires at least 2 sub-queries")
	}

	plan := &JoinPlan{
		Steps: make([]JoinStep, 0),
	}

	// Map table references to their sub-query IDs
	tableToSubQuery := make(map[string]string)
	for _, sq := range subQueries {
		for _, table := range sq.Tables {
			tableToSubQuery[table.DisplayName()] = sq.ID
			tableToSubQuery[table.Name] = sq.ID
			tableToSubQuery[table.FullName()] = sq.ID
		}
	}

	// Generate join steps from join conditions
	// Use left-deep tree: ((sq0 JOIN sq1) JOIN sq2) ...
	usedSubQueries := make(map[string]bool)
	var lastStepResult string

	for i, join := range analysis.Joins {
		leftSQ := tableToSubQuery[join.LeftTable]
		rightSQ := tableToSubQuery[join.RightTable]

		if leftSQ == "" || rightSQ == "" {
			continue // Skip if tables not found
		}

		// Determine inputs for this join step
		var leftInput, rightInput string

		if lastStepResult == "" {
			// First join
			leftInput = leftSQ
			rightInput = rightSQ
			usedSubQueries[leftSQ] = true
			usedSubQueries[rightSQ] = true
		} else {
			// Subsequent join - left side is previous result
			leftInput = lastStepResult

			// Right side is the sub-query not yet used
			if !usedSubQueries[leftSQ] {
				rightInput = leftSQ
				usedSubQueries[leftSQ] = true
			} else if !usedSubQueries[rightSQ] {
				rightInput = rightSQ
				usedSubQueries[rightSQ] = true
			} else {
				// Both already used, this is a self-join or complex case
				rightInput = rightSQ
			}
		}

		stepID := fmt.Sprintf("step_%d", i)
		plan.Steps = append(plan.Steps, JoinStep{
			StepID:     i,
			Type:       join.Type,
			LeftInput:  leftInput,
			RightInput: rightInput,
			LeftKey:    join.LeftCol,
			RightKey:   join.RightCol,
			Strategy:   JoinStrategyHash, // Default to hash join
		})

		lastStepResult = stepID
	}

	// If no joins found but multiple sub-queries, create implicit cross join
	if len(plan.Steps) == 0 && len(subQueries) >= 2 {
		plan.Steps = append(plan.Steps, JoinStep{
			StepID:     0,
			Type:       JoinTypeCross,
			LeftInput:  subQueries[0].ID,
			RightInput: subQueries[1].ID,
			Strategy:   JoinStrategyNestedLoop,
		})
	}

	return plan, nil
}

// Validate checks if the decomposed query is valid for execution.
func (d *DecomposedQuery) Validate() error {
	if len(d.SubQueries) == 0 {
		return fmt.Errorf("decomposed query has no sub-queries")
	}

	if d.JoinPlan == nil || len(d.JoinPlan.Steps) == 0 {
		if len(d.SubQueries) > 1 {
			return fmt.Errorf("decomposed query has multiple sub-queries but no join plan")
		}
	}

	// Validate each sub-query has required fields
	for i, sq := range d.SubQueries {
		if sq.Engine == "" {
			return fmt.Errorf("sub-query %d has no engine", i)
		}
		if sq.SQL == "" {
			return fmt.Errorf("sub-query %d has no SQL", i)
		}
	}

	return nil
}
