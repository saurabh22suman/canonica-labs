// Package planner provides rule-based query planning for the canonica gateway.
// The planner determines which engine should execute a query based on capabilities.
//
// Per docs/plan.md: "Rule-based, deterministic. No machine learning. No cost estimation."
package planner

import (
	"context"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/sql"
	"github.com/canonica-labs/canonica/internal/tables"
)

// ExecutionPlan represents a planned query ready for execution.
type ExecutionPlan struct {
	// LogicalPlan is the parsed SQL query.
	LogicalPlan *sql.LogicalPlan

	// Engine is the selected engine for execution.
	Engine string

	// ResolvedTables are the virtual tables referenced in the query.
	ResolvedTables []*tables.VirtualTable

	// RequiredCapabilities are the capabilities needed for this query.
	RequiredCapabilities []capabilities.Capability
}

// Planner creates execution plans from logical plans.
type Planner struct {
	tableRegistry TableRegistry
	engineMatcher EngineMatcher
}

// TableRegistry provides access to registered virtual tables.
type TableRegistry interface {
	// GetTable returns a virtual table by name.
	// Returns ErrTableNotFound if the table doesn't exist.
	GetTable(ctx context.Context, name string) (*tables.VirtualTable, error)
}

// EngineMatcher selects an appropriate engine for a query.
type EngineMatcher interface {
	// SelectEngine returns the best engine for the given capabilities.
	// Returns ErrEngineUnavailable if no engine can satisfy the requirements.
	SelectEngine(ctx context.Context, required []capabilities.Capability) (string, error)

	// AvailableEngines returns the list of available engines.
	AvailableEngines(ctx context.Context) []string
}

// NewPlanner creates a new planner with the given dependencies.
func NewPlanner(registry TableRegistry, matcher EngineMatcher) *Planner {
	return &Planner{
		tableRegistry: registry,
		engineMatcher: matcher,
	}
}

// Plan creates an execution plan from a logical plan.
// Returns an error if the query cannot be planned.
func (p *Planner) Plan(ctx context.Context, logical *sql.LogicalPlan) (*ExecutionPlan, error) {
	// Resolve all referenced tables
	resolvedTables := make([]*tables.VirtualTable, 0, len(logical.Tables))
	for _, tableName := range logical.Tables {
		vt, err := p.tableRegistry.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		resolvedTables = append(resolvedTables, vt)
	}

	// Phase 9: Check for cross-engine queries
	// Per phase-9-spec.md: Queries spanning multiple engines require federation
	if err := p.checkCrossEngine(resolvedTables); err != nil {
		return nil, err
	}

	// Check SNAPSHOT_CONSISTENT constraints
	// Per phase-1-spec.md: SNAPSHOT_CONSISTENT must be enforced
	if err := p.checkSnapshotConsistency(logical, resolvedTables); err != nil {
		return nil, err
	}

	// Determine required capabilities
	required := p.determineRequiredCapabilities(logical, resolvedTables)

	// Check that all tables support the required capabilities
	for _, vt := range resolvedTables {
		if err := p.checkTableCapabilities(vt, logical.Operation, required); err != nil {
			return nil, err
		}
	}

	// Select engine based on required capabilities
	engine, err := p.engineMatcher.SelectEngine(ctx, required)
	if err != nil {
		return nil, err
	}

	return &ExecutionPlan{
		LogicalPlan:          logical,
		Engine:               engine,
		ResolvedTables:       resolvedTables,
		RequiredCapabilities: required,
	}, nil
}

// checkCrossEngine detects queries that span multiple engines.
// Per phase-9-spec.md: Returns ErrCrossEngineQuery when tables require different engines.
func (p *Planner) checkCrossEngine(resolvedTables []*tables.VirtualTable) error {
	if len(resolvedTables) <= 1 {
		return nil // Single table queries can't be cross-engine
	}

	// Collect preferred engines for each table based on format
	engines := make(map[string]bool)
	for _, vt := range resolvedTables {
		engine := p.preferredEngineForTable(vt)
		engines[engine] = true
	}

	// If more than one engine is preferred, this is a cross-engine query
	if len(engines) > 1 {
		engineList := make([]string, 0, len(engines))
		for e := range engines {
			engineList = append(engineList, e)
		}
		return errors.NewCrossEngineQuery(engineList)
	}

	return nil
}

// preferredEngineForTable determines the preferred engine for a table.
// Per phase-8-spec.md §7.1: Engine selection rules.
func (p *Planner) preferredEngineForTable(vt *tables.VirtualTable) string {
	// Rule 1: Explicit engine assignment
	if len(vt.Sources) > 0 && vt.Sources[0].Engine != "" {
		return vt.Sources[0].Engine
	}

	// Rule 2: Based on format
	if len(vt.Sources) > 0 {
		switch vt.Sources[0].Format {
		case "iceberg":
			return "trino" // Best Iceberg support
		case "delta":
			return "spark" // Native Delta support
		case "hudi":
			return "spark" // Hudi is Spark-native
		case "parquet":
			return "duckdb" // Fast for raw Parquet
		case "orc":
			return "trino" // Good ORC support
		case "csv":
			return "duckdb" // Efficient for CSV
		}
	}

	// Default to duckdb
	return "duckdb"
}

// checkSnapshotConsistency enforces SNAPSHOT_CONSISTENT constraint rules.
// Per phase-1-spec.md:
// - Queries on SNAPSHOT_CONSISTENT tables MUST declare snapshot intent (AS OF)
// - All SNAPSHOT_CONSISTENT tables in a query must be compatible
func (p *Planner) checkSnapshotConsistency(logical *sql.LogicalPlan, resolvedTables []*tables.VirtualTable) error {
	// Find all tables with SNAPSHOT_CONSISTENT constraint
	var snapshotTables []*tables.VirtualTable
	var nonSnapshotTables []*tables.VirtualTable

	for _, vt := range resolvedTables {
		if vt.HasConstraint(capabilities.ConstraintSnapshotConsistent) {
			snapshotTables = append(snapshotTables, vt)
		} else {
			nonSnapshotTables = append(nonSnapshotTables, vt)
		}
	}

	// If no SNAPSHOT_CONSISTENT tables, no enforcement needed
	if len(snapshotTables) == 0 {
		return nil
	}

	// Rule 1: SNAPSHOT_CONSISTENT tables require AS OF
	if !logical.HasTimeTravel {
		tableNames := make([]string, 0, len(snapshotTables))
		for _, vt := range snapshotTables {
			tableNames = append(tableNames, vt.Name)
		}
		return errors.NewConstraintViolation(
			tableNames[0],
			string(capabilities.ConstraintSnapshotConsistent),
			"query must include AS OF clause for snapshot-consistent tables",
		)
	}

	// Rule 2: Cannot mix SNAPSHOT_CONSISTENT and non-SNAPSHOT_CONSISTENT tables
	// This is because we cannot guarantee consistency across tables with different
	// snapshot semantics
	if len(snapshotTables) > 0 && len(nonSnapshotTables) > 0 {
		return errors.NewConstraintViolation(
			snapshotTables[0].Name,
			string(capabilities.ConstraintSnapshotConsistent),
			"cannot mix SNAPSHOT_CONSISTENT tables with non-snapshot tables in same query",
		)
	}

	// Rule 3: All SNAPSHOT_CONSISTENT tables must have the same snapshot timestamp
	// This is because different snapshots could see inconsistent data states
	if len(snapshotTables) > 1 && len(logical.TimeTravelPerTable) > 0 {
		var firstTimestamp string
		var firstTable string
		for _, vt := range snapshotTables {
			ts, ok := logical.TimeTravelPerTable[vt.Name]
			if !ok {
				// Table has SNAPSHOT_CONSISTENT but no per-table AS OF
				// This is allowed if we have a global timestamp
				continue
			}
			if firstTimestamp == "" {
				firstTimestamp = ts
				firstTable = vt.Name
			} else if ts != firstTimestamp {
				return errors.NewConstraintViolation(
					vt.Name,
					string(capabilities.ConstraintSnapshotConsistent),
					"all SNAPSHOT_CONSISTENT tables must use the same snapshot timestamp; "+
						firstTable+" uses "+firstTimestamp+" but "+vt.Name+" uses "+ts,
				)
			}
		}
	}

	return nil
}

// determineRequiredCapabilities determines what capabilities are needed for a query.
func (p *Planner) determineRequiredCapabilities(logical *sql.LogicalPlan, _ []*tables.VirtualTable) []capabilities.Capability {
	required := []capabilities.Capability{}

	// Base capability from operation type
	if baseCap := logical.Operation.RequiredCapability(); baseCap != "" {
		required = append(required, baseCap)
	}

	// Time travel requires TIME_TRAVEL capability
	if logical.HasTimeTravel {
		required = append(required, capabilities.CapabilityTimeTravel)
	}

	return required
}

// checkTableCapabilities verifies a table can perform the required operation.
func (p *Planner) checkTableCapabilities(vt *tables.VirtualTable, op capabilities.OperationType, required []capabilities.Capability) error {
	// First check if operation is allowed (handles constraints)
	if err := vt.CanPerform(op); err != nil {
		return err
	}

	// Then check each required capability
	for _, cap := range required {
		if !vt.HasCapability(cap) {
			return errors.NewCapabilityDenied(vt.Name, string(cap), string(op))
		}
	}

	return nil
}

// Explain returns a human-readable explanation of how a query would be executed.
func (p *Planner) Explain(ctx context.Context, logical *sql.LogicalPlan) (string, error) {
	plan, err := p.Plan(ctx, logical)
	if err != nil {
		return "", err
	}

	explanation := "Query Plan:\n"
	explanation += "  Operation: " + string(plan.LogicalPlan.Operation) + "\n"
	explanation += "  Tables:\n"
	for _, vt := range plan.ResolvedTables {
		explanation += "    - " + vt.Name + "\n"
		explanation += "      Capabilities: " + formatCapabilities(vt.Capabilities) + "\n"
		if len(vt.Constraints) > 0 {
			explanation += "      Constraints: " + formatConstraints(vt.Constraints) + "\n"
		}
	}
	explanation += "  Required Capabilities: " + formatCapabilities(plan.RequiredCapabilities) + "\n"
	explanation += "  Selected Engine: " + plan.Engine + "\n"

	return explanation, nil
}

func formatCapabilities(caps []capabilities.Capability) string {
	if len(caps) == 0 {
		return "(none)"
	}
	result := ""
	for i, c := range caps {
		if i > 0 {
			result += ", "
		}
		result += string(c)
	}
	return result
}

func formatConstraints(cons []capabilities.Constraint) string {
	if len(cons) == 0 {
		return "(none)"
	}
	result := ""
	for i, c := range cons {
		if i > 0 {
			result += ", "
		}
		result += string(c)
	}
	return result
}
