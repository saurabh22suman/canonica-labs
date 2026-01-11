package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/tables"
	"github.com/canonica-labs/canonica/pkg/models"
)

func (c *CLI) newTableCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table",
		Short: "Virtual table management",
		Long:  `Manage virtual tables - the core abstraction of canonica.`,
	}

	cmd.AddCommand(c.newTableRegisterCmd())
	cmd.AddCommand(c.newTableValidateCmd())
	cmd.AddCommand(c.newTableDescribeCmd())
	cmd.AddCommand(c.newTableListCmd())
	cmd.AddCommand(c.newTableDeleteCmd())

	return cmd
}

func (c *CLI) newTableRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register <file.yaml>",
		Short: "Register a virtual table",
		Long: `Register or update a virtual table from a YAML definition file.

The definition file must include:
  - name: unique table identifier
  - sources: list of physical storage locations
  - capabilities: operations the table supports (READ, TIME_TRAVEL)
  - constraints: restrictions on operations (READ_ONLY, SNAPSHOT_CONSISTENT)

Example file:
  name: analytics.sales_orders
  description: Sales order data from the warehouse
  sources:
    - format: DELTA
      location: s3://data-lake/sales/orders
  capabilities:
    - READ
    - TIME_TRAVEL
  constraints:
    - READ_ONLY`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runTableRegister(args[0])
		},
	}
}

func (c *CLI) runTableRegister(filePath string) error {
	// Parse the table definition
	vt, err := c.parseTableDefinition(filePath)
	if err != nil {
		c.errorf("Error: %v\n", err)
		return err
	}

	// Validate the table structure locally first (fail-fast)
	if err := vt.Validate(); err != nil {
		c.errorf("Validation failed: %v\n", err)
		return err
	}

	c.debugf("Table definition valid: %s\n", vt.Name)

	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	// CLI fails hard if gateway is unreachable
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Convert to gateway request
	req := &RegisterTableRequest{
		Name:        vt.Name,
		Description: vt.Description,
	}
	for _, src := range vt.Sources {
		req.Sources = append(req.Sources, SourceInfo{
			Format:   string(src.Format),
			Location: src.Location,
		})
	}
	for _, cap := range vt.Capabilities {
		req.Capabilities = append(req.Capabilities, string(cap))
	}
	for _, con := range vt.Constraints {
		req.Constraints = append(req.Constraints, string(con))
	}

	if err := client.RegisterTable(ctx, req); err != nil {
		c.errorf("Registration failed: %v\n", err)
		return err
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"status": "registered",
			"table":  vt.Name,
		})
	}

	c.printf("✓ Table '%s' registered successfully\n", vt.Name)
	return nil
}

func (c *CLI) newTableValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <file.yaml>",
		Short: "Validate a table definition without registering",
		Long: `Validate a table definition file without registering it.

This is useful for CI/CD pipelines to catch errors before deployment.
Exit code 0 means valid, exit code 1 means validation failed.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runTableValidate(args[0])
		},
	}
}

func (c *CLI) runTableValidate(filePath string) error {
	// Parse the table definition
	vt, err := c.parseTableDefinition(filePath)
	if err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"file":   filePath,
				"errors": []string{err.Error()},
			})
		}
		c.errorf("Parse error: %v\n", err)
		return err
	}

	// Validate the table
	if err := vt.Validate(); err != nil {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"valid":  false,
				"file":   filePath,
				"table":  vt.Name,
				"errors": []string{err.Error()},
			})
		}
		c.errorf("Validation failed: %v\n", err)
		return err
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"valid": true,
			"file":  filePath,
			"table": vt.Name,
		})
	}

	c.printf("✓ %s: valid\n", filePath)
	c.printf("  Table: %s\n", vt.Name)
	c.printf("  Sources: %d\n", len(vt.Sources))
	c.printf("  Capabilities: %s\n", formatCapabilities(vt.Capabilities))
	if len(vt.Constraints) > 0 {
		c.printf("  Constraints: %s\n", formatConstraints(vt.Constraints))
	}

	return nil
}

func (c *CLI) newTableDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe <table_name>",
		Short: "Describe a registered virtual table",
		Long: `Display detailed information about a registered virtual table.

Shows:
  - capabilities
  - constraints
  - physical sources
  - engine compatibility
  - health status`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runTableDescribe(args[0])
		},
	}
}

func (c *CLI) runTableDescribe(tableName string) error {
	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table, err := client.DescribeTable(ctx, tableName)
	if err != nil {
		c.errorf("Failed to describe table: %v\n", err)
		return err
	}

	if c.jsonOutput {
		return c.outputJSON(table)
	}

	c.println("Table:", table.Name)
	c.printf("  Capabilities: %s\n", strings.Join(table.Capabilities, ", "))
	if len(table.Constraints) > 0 {
		c.printf("  Constraints: %s\n", strings.Join(table.Constraints, ", "))
	}
	c.println("  Sources:")
	for _, src := range table.Sources {
		c.printf("    - %s: %s\n", src.Format, src.Location)
	}

	return nil
}

func (c *CLI) newTableListCmd() *cobra.Command {
	var (
		filterEngine     string
		filterCapability string
		filterConstraint string
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered virtual tables",
		Long:  `List all registered virtual tables with optional filtering.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runTableList(filterEngine, filterCapability, filterConstraint)
		},
	}

	cmd.Flags().StringVar(&filterEngine, "engine", "", "filter by engine")
	cmd.Flags().StringVar(&filterCapability, "capability", "", "filter by capability")
	cmd.Flags().StringVar(&filterConstraint, "constraint", "", "filter by constraint")

	return cmd
}

func (c *CLI) runTableList(engine, capability, constraint string) error {
	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tables, err := client.ListTables(ctx)
	if err != nil {
		c.errorf("Failed to list tables: %v\n", err)
		return err
	}

	// Client-side filtering (gateway may support this in the future)
	var filtered []TableInfo
	for _, t := range tables {
		if capability != "" {
			found := false
			for _, cap := range t.Capabilities {
				if strings.EqualFold(cap, capability) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if constraint != "" {
			found := false
			for _, con := range t.Constraints {
				if strings.EqualFold(con, constraint) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		filtered = append(filtered, t)
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"tables": filtered,
		})
	}

	if len(filtered) == 0 {
		c.println("No tables registered")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	printTableHeader(w)
	for _, t := range filtered {
		fmt.Fprintf(w, "%s\t%s\t%s\t-\n",
			t.Name,
			strings.Join(t.Capabilities, ", "),
			strings.Join(t.Constraints, ", "))
	}
	w.Flush()

	return nil
}

func (c *CLI) newTableDeleteCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "delete <table_name>",
		Short: "Delete a virtual table",
		Long: `Delete a registered virtual table.

Requires confirmation unless --force is provided.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runTableDelete(args[0], force)
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "skip confirmation")

	return cmd
}

func (c *CLI) runTableDelete(tableName string, force bool) error {
	if !force {
		c.printf("Delete table '%s'? [y/N]: ", tableName)
		var confirm string
		fmt.Scanln(&confirm)
		if strings.ToLower(confirm) != "y" {
			c.println("Cancelled")
			return nil
		}
	}

	// Per execution-checklist.md 4.2: CLI uses GatewayClient exclusively
	client := c.newGatewayClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.DeleteTable(ctx, tableName); err != nil {
		c.errorf("Failed to delete table: %v\n", err)
		return err
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"status": "deleted",
			"table":  tableName,
		})
	}

	c.printf("✓ Table '%s' deleted\n", tableName)
	return nil
}

// Helper functions

func (c *CLI) parseTableDefinition(filePath string) (*tables.VirtualTable, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var def models.TableDefinition
	if err := yaml.Unmarshal(data, &def); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Convert to internal model
	vt := &tables.VirtualTable{
		Name:        def.Name,
		Description: def.Description,
	}

	// Parse sources
	for _, src := range def.Sources {
		vt.Sources = append(vt.Sources, tables.PhysicalSource{
			Format:   tables.StorageFormat(strings.ToUpper(src.Format)),
			Location: src.Location,
			Engine:   src.Engine,
		})
	}

	// Parse capabilities
	for _, capStr := range def.Capabilities {
		cap, err := capabilities.ParseCapability(capStr)
		if err != nil {
			return nil, err
		}
		vt.Capabilities = append(vt.Capabilities, cap)
	}

	// Parse constraints
	for _, conStr := range def.Constraints {
		con, err := capabilities.ParseConstraint(conStr)
		if err != nil {
			return nil, err
		}
		vt.Constraints = append(vt.Constraints, con)
	}

	return vt, nil
}

func formatCapabilities(caps []capabilities.Capability) string {
	if len(caps) == 0 {
		return "(none)"
	}
	strs := make([]string, len(caps))
	for i, c := range caps {
		strs[i] = string(c)
	}
	return strings.Join(strs, ", ")
}

func formatConstraints(cons []capabilities.Constraint) string {
	if len(cons) == 0 {
		return "(none)"
	}
	strs := make([]string, len(cons))
	for i, c := range cons {
		strs[i] = string(c)
	}
	return strings.Join(strs, ", ")
}

func printTableHeader(w *tabwriter.Writer) {
	fmt.Fprintln(w, "NAME\tCAPABILITIES\tCONSTRAINTS\tSOURCES")
}
