package cli

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/sql"
)

func (c *CLI) newQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query execution commands",
		Long:  `Execute, explain, and validate SQL queries through the canonica gateway.`,
	}

	cmd.AddCommand(c.newQueryExecCmd())
	cmd.AddCommand(c.newQueryExplainCmd())
	cmd.AddCommand(c.newQueryValidateCmd())

	return cmd
}

func (c *CLI) newQueryExecCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "exec <SQL>",
		Short: "Execute a SQL query",
		Long: `Execute a SQL query through the canonica gateway.

The query is validated, routed to the appropriate engine, and executed.
Results are streamed to stdout.

Example:
  canonic query exec "SELECT * FROM analytics.sales_orders LIMIT 10"`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runQueryExec(args[0])
		},
	}
}

func (c *CLI) runQueryExec(sqlQuery string) error {
	// First, validate and parse the query
	parser := sql.NewParser()
	plan, err := parser.Parse(sqlQuery)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
		}
		c.errorf("Query rejected: %v\n", err)
		return err
	}

	c.debugf("Parsed query: %+v\n", plan)

	// TODO: Send to gateway for execution
	// For now, show what would happen
	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"success":   false,
			"message":   "gateway execution not yet implemented",
			"operation": plan.Operation,
			"tables":    plan.Tables,
		})
	}

	c.println("Query parsed successfully:")
	c.printf("  Operation: %s\n", plan.Operation)
	c.printf("  Tables: %s\n", strings.Join(plan.Tables, ", "))
	if plan.HasTimeTravel {
		c.printf("  Time Travel: AS OF %s\n", plan.TimeTravelTimestamp)
	}
	c.println("")
	c.println("Note: Gateway execution not yet implemented.")
	c.println("      Query validation passed - execution would proceed.")

	return nil
}

func (c *CLI) newQueryExplainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "explain <SQL>",
		Short: "Explain how a query will be executed",
		Long: `Show detailed explanation of how a query will be executed.

Displays:
  - tables referenced
  - capabilities required
  - selected engine
  - blocked operations (if any)

Example:
  canonic query explain "SELECT * FROM analytics.sales_orders WHERE date > '2024-01-01'"`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runQueryExplain(args[0])
		},
	}
}

func (c *CLI) runQueryExplain(sqlQuery string) error {
	// Parse the query
	parser := sql.NewParser()
	plan, err := parser.Parse(sqlQuery)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"error":  err.Error(),
				"query":  sqlQuery,
			})
		}
		c.errorf("Query rejected: %v\n", err)
		return err
	}

	// Determine required capabilities
	requiredCaps := []capabilities.Capability{capabilities.CapabilityRead}
	if plan.HasTimeTravel {
		requiredCaps = append(requiredCaps, capabilities.CapabilityTimeTravel)
	}

	// Get default router for engine selection
	r := router.DefaultRouter()
	engine, engineErr := r.SelectEngine(nil, requiredCaps)

	explanation := QueryExplanation{
		Query:                sqlQuery,
		Valid:                true,
		Operation:            string(plan.Operation),
		Tables:               plan.Tables,
		RequiredCapabilities: capabilitiesToStrings(requiredCaps),
		HasTimeTravel:        plan.HasTimeTravel,
	}

	if engineErr == nil {
		explanation.SelectedEngine = engine
	} else {
		explanation.EngineError = engineErr.Error()
	}

	if c.jsonOutput {
		return c.outputJSON(explanation)
	}

	c.println("Query Explanation")
	c.println("=================")
	c.println("")
	c.println("Query:")
	c.printf("  %s\n", sqlQuery)
	c.println("")
	c.println("Analysis:")
	c.printf("  Operation: %s\n", plan.Operation)
	if len(plan.Tables) > 0 {
		c.printf("  Tables: %s\n", strings.Join(plan.Tables, ", "))
	} else {
		c.println("  Tables: (none detected)")
	}
	c.printf("  Time Travel: %v\n", plan.HasTimeTravel)
	if plan.HasTimeTravel {
		c.printf("  Timestamp: %s\n", plan.TimeTravelTimestamp)
	}
	c.println("")
	c.println("Requirements:")
	c.printf("  Capabilities: %s\n", strings.Join(capabilitiesToStrings(requiredCaps), ", "))
	c.println("")
	c.println("Routing Decision:")
	if engineErr == nil {
		c.printf("  Selected Engine: %s\n", engine)
		c.println("  Status: ✓ Query can be executed")
	} else {
		c.printf("  Error: %v\n", engineErr)
		c.println("  Status: ✗ Query cannot be executed")
	}

	return nil
}

func (c *CLI) newQueryValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <SQL>",
		Short: "Validate a query without execution",
		Long: `Validate a SQL query without executing it.

Useful for CI/CD pipelines and pre-flight checks.
Exit code 0 means valid, exit code 1 means invalid.

Example:
  canonic query validate "SELECT * FROM analytics.sales_orders"`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runQueryValidate(args[0])
		},
	}
}

func (c *CLI) runQueryValidate(sqlQuery string) error {
	parser := sql.NewParser()
	plan, err := parser.Parse(sqlQuery)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"query":  sqlQuery,
				"errors": []string{err.Error()},
			})
		}
		c.errorf("✗ Invalid: %v\n", err)
		return err
	}

	// Determine required capabilities
	requiredCaps := []capabilities.Capability{capabilities.CapabilityRead}
	if plan.HasTimeTravel {
		requiredCaps = append(requiredCaps, capabilities.CapabilityTimeTravel)
	}

	// Check if any engine can handle this
	r := router.DefaultRouter()
	engine, engineErr := r.SelectEngine(nil, requiredCaps)

	if engineErr != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"query":  sqlQuery,
				"errors": []string{engineErr.Error()},
			})
		}
		c.errorf("✗ No engine available: %v\n", engineErr)
		return engineErr
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"valid":     true,
			"query":     sqlQuery,
			"operation": plan.Operation,
			"tables":    plan.Tables,
			"engine":    engine,
		})
	}

	c.printf("✓ Valid (would use engine: %s)\n", engine)
	return nil
}

// QueryExplanation represents the JSON output for query explain.
type QueryExplanation struct {
	Query                string   `json:"query"`
	Valid                bool     `json:"valid"`
	Operation            string   `json:"operation"`
	Tables               []string `json:"tables"`
	RequiredCapabilities []string `json:"required_capabilities"`
	HasTimeTravel        bool     `json:"has_time_travel"`
	SelectedEngine       string   `json:"selected_engine,omitempty"`
	EngineError          string   `json:"engine_error,omitempty"`
}

func capabilitiesToStrings(caps []capabilities.Capability) []string {
	result := make([]string, len(caps))
	for i, c := range caps {
		result[i] = string(c)
	}
	return result
}
