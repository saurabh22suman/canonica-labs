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
