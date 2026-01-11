package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
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
	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	// No local parsing - all validation happens on the gateway
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.ExecuteQuery(ctx, sqlQuery)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
		}
		c.errorf("Query failed: %v\n", err)
		return err
	}

	if c.jsonOutput {
		return c.outputJSON(result)
	}

	c.printf("Query ID: %s\n", result.QueryID)
	c.printf("Engine: %s\n", result.Engine)
	c.printf("Duration: %s\n", result.Duration)
	c.printf("Rows: %d\n", result.RowCount)

	if len(result.Columns) > 0 && len(result.Rows) > 0 {
		c.println("")
		c.println(strings.Join(result.Columns, "\t"))
		for _, row := range result.Rows {
			var values []string
			for _, col := range result.Columns {
				if v, ok := row[col]; ok {
					values = append(values, formatValue(v))
				}
			}
			c.println(strings.Join(values, "\t"))
		}
	}

	return nil
}

// formatValue formats a value for display
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(
		strings.ReplaceAll(fmt.Sprintf("%v", v), "\n", " "), "\t", " "), "  ", " "))
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
	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	// No local parsing - all explanation happens on the gateway
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.ExplainQuery(ctx, sqlQuery)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid": false,
				"error": err.Error(),
				"query": sqlQuery,
			})
		}
		c.errorf("Explain failed: %v\n", err)
		return err
	}

	if c.jsonOutput {
		return c.outputJSON(result)
	}

	c.println("Query Explanation")
	c.println("=================")
	c.println("")
	c.println("Query:")
	c.printf("  %s\n", result.SQL)
	c.println("")
	c.println("Analysis:")
	if len(result.Tables) > 0 {
		c.printf("  Tables: %s\n", strings.Join(result.Tables, ", "))
	} else {
		c.println("  Tables: (none detected)")
	}
	if len(result.Capabilities) > 0 {
		c.printf("  Capabilities: %s\n", strings.Join(result.Capabilities, ", "))
	}
	c.println("")
	c.println("Routing Decision:")
	c.printf("  Selected Engine: %s\n", result.Engine)
	if result.Plan != "" {
		c.println("")
		c.println("Execution Plan:")
		c.printf("  %s\n", result.Plan)
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
	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	// No local parsing - all validation happens on the gateway
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.ValidateQuery(ctx, sqlQuery)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"query":  sqlQuery,
				"errors": []string{err.Error()},
			})
		}
		c.errorf("✗ Validation failed: %v\n", err)
		return err
	}

	if !result.Valid {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"query":  sqlQuery,
				"errors": []string{result.Error},
			})
		}
		c.errorf("✗ Invalid: %s\n", result.Error)
		return fmt.Errorf("validation failed: %s", result.Error)
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"valid": true,
			"query": sqlQuery,
		})
	}

	c.println("✓ Valid")
	return nil
}
