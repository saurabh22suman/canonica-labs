// Package federation provides cross-engine query federation.
package federation

import (
	"context"
	"strings"
	"time"
)

// EngineCostFactors contains cost factors for a specific engine.
// Per phase-9-spec.md §4.1.
type EngineCostFactors struct {
	// QueryOverhead is fixed startup cost per query.
	QueryOverhead time.Duration

	// ScanCostPerRow is cost per row scanned.
	ScanCostPerRow float64

	// TransferCostPerRow is cost per row transferred to gateway.
	TransferCostPerRow float64

	// FilterCostPerRow is cost per filter evaluation.
	FilterCostPerRow float64

	// AggCostPerRow is cost per aggregation.
	AggCostPerRow float64

	// NetworkLatency is one-way network latency.
	NetworkLatency time.Duration
}

// DefaultCostFactors provides default cost factors for known engines.
// Per phase-9-spec.md §4.1.
var DefaultCostFactors = map[string]*EngineCostFactors{
	"duckdb": {
		QueryOverhead:      1 * time.Millisecond,
		ScanCostPerRow:     0.00001,
		TransferCostPerRow: 0.0,
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
	"spark": {
		QueryOverhead:      200 * time.Millisecond,
		ScanCostPerRow:     0.0002,
		TransferCostPerRow: 0.001,
		FilterCostPerRow:   0.0001,
		AggCostPerRow:      0.0002,
		NetworkLatency:     10 * time.Millisecond,
	},
	"snowflake": {
		QueryOverhead:      500 * time.Millisecond,
		ScanCostPerRow:     0.0005,
		TransferCostPerRow: 0.005,
		FilterCostPerRow:   0.0001,
		AggCostPerRow:      0.0001, // Very efficient aggregation
		NetworkLatency:     20 * time.Millisecond,
	},
	"bigquery": {
		QueryOverhead:      400 * time.Millisecond,
		ScanCostPerRow:     0.0004,
		TransferCostPerRow: 0.003,
		FilterCostPerRow:   0.0001,
		AggCostPerRow:      0.0001,
		NetworkLatency:     25 * time.Millisecond,
	},
	"redshift": {
		QueryOverhead:      300 * time.Millisecond,
		ScanCostPerRow:     0.0003,
		TransferCostPerRow: 0.002,
		FilterCostPerRow:   0.0001,
		AggCostPerRow:      0.0001,
		NetworkLatency:     15 * time.Millisecond,
	},
}

// CostModel holds cost factors for all engines.
type CostModel struct {
	engineCosts map[string]*EngineCostFactors
}

// NewCostModel creates a cost model with default factors.
func NewCostModel() *CostModel {
	return &CostModel{
		engineCosts: DefaultCostFactors,
	}
}

// NewCostModelWithFactors creates a cost model with custom factors.
func NewCostModelWithFactors(factors map[string]*EngineCostFactors) *CostModel {
	// Merge with defaults
	merged := make(map[string]*EngineCostFactors)
	for k, v := range DefaultCostFactors {
		merged[k] = v
	}
	for k, v := range factors {
		merged[k] = v
	}
	return &CostModel{engineCosts: merged}
}

// GetFactors returns cost factors for an engine.
func (m *CostModel) GetFactors(engine string) *EngineCostFactors {
	if factors, ok := m.engineCosts[engine]; ok {
		return factors
	}
	// Fallback to DuckDB as default
	return m.engineCosts["duckdb"]
}

// CostBreakdown details the components of a cost estimate.
type CostBreakdown struct {
	ScanCost     time.Duration
	FilterCost   time.Duration
	AggCost      time.Duration
	TransferCost time.Duration
	Overhead     time.Duration
}

// Total returns the total estimated time.
func (b *CostBreakdown) Total() time.Duration {
	return b.Overhead + b.ScanCost + b.FilterCost + b.AggCost + b.TransferCost
}

// QueryCost represents the estimated cost of a query.
// Per phase-9-spec.md §4.2.
type QueryCost struct {
	Engine        string
	EstimatedTime time.Duration
	EstimatedRows int64
	Breakdown     *CostBreakdown
}

// TableStats holds statistics for cost estimation.
type TableStats struct {
	RowCount       int64
	DistinctValues map[string]int64
	MinValues      map[string]interface{}
	MaxValues      map[string]interface{}
}

// StatsProvider provides table statistics for cost estimation.
type StatsProvider interface {
	GetTableStats(ctx context.Context, tableName string) (*TableStats, error)
}

// CostEstimator estimates query costs.
// Per phase-9-spec.md §4.2.
type CostEstimator struct {
	model         *CostModel
	statsProvider StatsProvider
}

// NewCostEstimator creates a new cost estimator.
func NewCostEstimator(model *CostModel, stats StatsProvider) *CostEstimator {
	if model == nil {
		model = NewCostModel()
	}
	return &CostEstimator{
		model:         model,
		statsProvider: stats,
	}
}

// EstimateCost estimates the cost of executing a sub-query on an engine.
func (e *CostEstimator) EstimateCost(
	ctx context.Context,
	subQuery *SubQuery,
	engine string,
) (*QueryCost, error) {
	factors := e.model.GetFactors(engine)

	// Get table statistics
	var totalRows int64
	var selectivity float64 = 1.0

	for _, table := range subQuery.Tables {
		if e.statsProvider != nil {
			stats, err := e.statsProvider.GetTableStats(ctx, table.FullName())
			if err == nil && stats != nil {
				totalRows += stats.RowCount

				// Estimate selectivity from predicates
				for _, pred := range subQuery.Predicates {
					if pred.Table == table.FullName() {
						selectivity *= e.estimatePredicateSelectivity(pred, stats)
					}
				}
				continue
			}
		}
		// Default if stats unavailable
		totalRows += 1000000
	}

	// Calculate costs
	scanTime := time.Duration(float64(totalRows) * factors.ScanCostPerRow * float64(time.Microsecond))
	filterTime := time.Duration(float64(totalRows) * factors.FilterCostPerRow * float64(time.Microsecond))
	transferTime := time.Duration(float64(totalRows) * selectivity * factors.TransferCostPerRow * float64(time.Microsecond))

	breakdown := &CostBreakdown{
		Overhead:     factors.QueryOverhead + factors.NetworkLatency,
		ScanCost:     scanTime,
		FilterCost:   filterTime,
		TransferCost: transferTime,
	}

	// Add aggregation cost if applicable
	if len(subQuery.Predicates) > 0 || subQuery.EstimatedRows > 0 {
		breakdown.AggCost = time.Duration(
			float64(totalRows) * selectivity * factors.AggCostPerRow * float64(time.Microsecond))
	}

	return &QueryCost{
		Engine:        engine,
		EstimatedTime: breakdown.Total(),
		EstimatedRows: int64(float64(totalRows) * selectivity),
		Breakdown:     breakdown,
	}, nil
}

// estimatePredicateSelectivity estimates how selective a predicate is.
func (e *CostEstimator) estimatePredicateSelectivity(
	pred *Predicate,
	stats *TableStats,
) float64 {
	switch strings.ToUpper(pred.Operator) {
	case "=":
		if stats.DistinctValues != nil {
			if distinct, ok := stats.DistinctValues[pred.Column]; ok && distinct > 0 {
				return 1.0 / float64(distinct)
			}
		}
		return 0.1 // Default for equality

	case "<", ">", "<=", ">=":
		return 0.33 // Range predicates typically filter ~1/3

	case "LIKE":
		valueStr, ok := pred.Value.(string)
		if ok && strings.HasPrefix(valueStr, "%") {
			return 0.5 // Leading wildcard: poor selectivity
		}
		return 0.1 // Prefix match: better selectivity

	case "IN":
		// Estimate based on number of values in IN list
		return 0.2 // Default for IN

	case "<>", "!=":
		return 0.9 // NOT EQUAL typically keeps most rows

	default:
		return 0.5 // Unknown operator
	}
}

// CompareEngines compares cost estimates across engines.
func (e *CostEstimator) CompareEngines(
	ctx context.Context,
	subQuery *SubQuery,
	engines []string,
) ([]*QueryCost, error) {
	var costs []*QueryCost

	for _, engine := range engines {
		cost, err := e.EstimateCost(ctx, subQuery, engine)
		if err != nil {
			continue
		}
		costs = append(costs, cost)
	}

	return costs, nil
}

// QueryOptimizer selects optimal engines for queries.
// Per phase-9-spec.md §4.3.
type QueryOptimizer struct {
	estimator *CostEstimator
}

// NewQueryOptimizer creates a new query optimizer.
func NewQueryOptimizer(estimator *CostEstimator) *QueryOptimizer {
	return &QueryOptimizer{estimator: estimator}
}

// SelectOptimalEngine chooses the best engine for a table.
func (o *QueryOptimizer) SelectOptimalEngine(
	ctx context.Context,
	table *TableRef,
	predicates []*Predicate,
	candidateEngines []string,
) (string, *QueryCost, error) {
	if len(candidateEngines) == 0 {
		return "", nil, nil
	}

	if len(candidateEngines) == 1 {
		return candidateEngines[0], nil, nil
	}

	subQuery := &SubQuery{
		Tables:     []*TableRef{table},
		Predicates: predicates,
	}

	costs, err := o.estimator.CompareEngines(ctx, subQuery, candidateEngines)
	if err != nil {
		return "", nil, err
	}

	if len(costs) == 0 {
		return candidateEngines[0], nil, nil
	}

	// Find minimum cost
	var bestCost *QueryCost
	for _, cost := range costs {
		if bestCost == nil || cost.EstimatedTime < bestCost.EstimatedTime {
			bestCost = cost
		}
	}

	return bestCost.Engine, bestCost, nil
}

// OptimizeDecomposition optimizes engine selection for a decomposed query.
func (o *QueryOptimizer) OptimizeDecomposition(
	ctx context.Context,
	decomposed *DecomposedQuery,
) (*DecomposedQuery, error) {
	// For now, keep the default engine assignments
	// Future: re-evaluate engine choices based on costs
	return decomposed, nil
}
