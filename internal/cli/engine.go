package cli

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"os"

	"github.com/spf13/cobra"

	"github.com/canonica-labs/canonica/internal/router"
)

func (c *CLI) newEngineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "engine",
		Short: "Engine inspection commands",
		Long:  `Inspect available query engines and their capabilities.`,
	}

	cmd.AddCommand(c.newEngineListCmd())
	cmd.AddCommand(c.newEngineDescribeCmd())

	return cmd
}

func (c *CLI) newEngineListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List available engines",
		Long: `List all configured query engines and their status.

Shows:
  - engine name
  - availability status
  - supported capabilities`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runEngineList()
		},
	}
}

func (c *CLI) runEngineList() error {
	// Get default router with configured engines
	r := router.DefaultRouter()

	// Collect engine info
	engines := []EngineInfo{
		c.getEngineInfo(r, "duckdb"),
		c.getEngineInfo(r, "trino"),
		c.getEngineInfo(r, "spark"),
	}

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"engines": engines,
		})
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tSTATUS\tCAPABILITIES\tPRIORITY")
	fmt.Fprintln(w, "----\t------\t------------\t--------")

	for _, eng := range engines {
		status := "unavailable"
		if eng.Available {
			status = "available"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n",
			eng.Name,
			status,
			strings.Join(eng.Capabilities, ", "),
			eng.Priority,
		)
	}
	w.Flush()

	c.println("")
	c.println("Note: Trino and Spark are placeholders. See tracker.md T002, T003.")

	return nil
}

func (c *CLI) newEngineDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe <engine_name>",
		Short: "Describe a specific engine",
		Long: `Display detailed information about a specific engine.

Shows:
  - configuration
  - capabilities
  - health status
  - connection info`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runEngineDescribe(args[0])
		},
	}
}

func (c *CLI) runEngineDescribe(engineName string) error {
	r := router.DefaultRouter()
	engine, ok := r.GetEngine(engineName)

	if !ok {
		if c.jsonOutput {
			return c.outputJSON(map[string]interface{}{
				"error": fmt.Sprintf("engine not found: %s", engineName),
			})
		}
		c.errorf("Engine not found: %s\n", engineName)
		c.errorf("Use 'canonic engine list' to see available engines\n")
		return fmt.Errorf("engine not found: %s", engineName)
	}

	caps := make([]string, len(engine.Capabilities))
	for i, cap := range engine.Capabilities {
		caps[i] = string(cap)
	}

	info := EngineInfo{
		Name:         engine.Name,
		Available:    engine.Available,
		Capabilities: caps,
		Priority:     engine.Priority,
	}

	if c.jsonOutput {
		return c.outputJSON(info)
	}

	c.printf("Engine: %s\n", engine.Name)
	c.println("========" + strings.Repeat("=", len(engine.Name)))
	c.println("")
	
	status := "✗ unavailable"
	if engine.Available {
		status = "✓ available"
	}
	c.printf("Status: %s\n", status)
	c.printf("Priority: %d (lower = preferred)\n", engine.Priority)
	c.println("")
	c.println("Capabilities:")
	for _, cap := range caps {
		c.printf("  • %s\n", cap)
	}
	c.println("")

	// Engine-specific info
	switch engine.Name {
	case "duckdb":
		c.println("Configuration:")
		if c.cfg != nil {
			c.printf("  Database: %s\n", c.cfg.Engines.DuckDB.Database)
		} else {
			c.println("  Database: :memory: (default)")
		}
		c.println("")
		c.println("DuckDB is the MVP engine for local development and testing.")
	case "trino":
		c.println("Status: Not yet implemented")
		c.println("See tracker.md T002 for Trino adapter implementation.")
	case "spark":
		c.println("Status: Not yet implemented")
		c.println("See tracker.md T003 for Spark adapter implementation.")
	}

	return nil
}

// EngineInfo represents engine information for JSON output.
type EngineInfo struct {
	Name         string   `json:"name"`
	Available    bool     `json:"available"`
	Capabilities []string `json:"capabilities"`
	Priority     int      `json:"priority"`
}

func (c *CLI) getEngineInfo(r *router.Router, name string) EngineInfo {
	engine, ok := r.GetEngine(name)
	if !ok {
		return EngineInfo{Name: name, Available: false}
	}

	caps := make([]string, len(engine.Capabilities))
	for i, cap := range engine.Capabilities {
		caps[i] = string(cap)
	}

	return EngineInfo{
		Name:         engine.Name,
		Available:    engine.Available,
		Capabilities: caps,
		Priority:     engine.Priority,
	}
}
