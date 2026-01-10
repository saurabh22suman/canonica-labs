package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/canonica-labs/canonica/internal/bootstrap"
)

func (c *CLI) newBootstrapCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap and configuration management",
		Long: `Manage canonica configuration and system initialization.

Per phase-5-spec.md: "Provide a safe, explicit install path for new environments."

Commands:
  init     - Generate example configuration
  validate - Validate configuration against schema
  apply    - Apply configuration to system`,
	}

	cmd.AddCommand(c.newBootstrapInitCmd())
	cmd.AddCommand(c.newBootstrapValidateCmd())
	cmd.AddCommand(c.newBootstrapApplyCmd())

	return cmd
}

func (c *CLI) newBootstrapInitCmd() *cobra.Command {
	var outputDir string

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Generate example configuration",
		Long: `Generate an example configuration file for canonica.

Per phase-5-spec.md §2: "bootstrap init generates example configuration"
This command does NOT modify system state - it only creates a template file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runBootstrapInit(outputDir)
		},
	}

	cmd.Flags().StringVarP(&outputDir, "output", "o", ".", "output directory for configuration file")

	return cmd
}

func (c *CLI) runBootstrapInit(outputDir string) error {
	bootstrapper := bootstrap.NewBootstrapper(nil)

	configPath, err := bootstrapper.Init(outputDir)
	if err != nil {
		c.errorf("Error: %v\n", err)
		return err
	}

	absPath, _ := filepath.Abs(configPath)
	c.printf("✓ Configuration file created: %s\n", absPath)
	c.println("\nNext steps:")
	c.println("  1. Edit the configuration file to match your environment")
	c.println("  2. Run 'canonic bootstrap validate' to check configuration")
	c.println("  3. Run 'canonic bootstrap apply' to apply configuration")

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"status": "created",
			"path":   absPath,
		})
	}

	return nil
}

func (c *CLI) newBootstrapValidateCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate configuration",
		Long: `Validate configuration file against schema and perform dry-run checks.

Per phase-5-spec.md §2: "bootstrap validate performs dry-run invariant checks"

This command:
  - Validates configuration syntax
  - Checks all required sections are present
  - Validates engine references
  - Validates table definitions
  - Fails on ambiguity

No system state is modified.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runBootstrapValidate(configPath)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "canonic.yaml", "configuration file path")

	return cmd
}

func (c *CLI) runBootstrapValidate(configPath string) error {
	c.debugf("Validating configuration: %s\n", configPath)

	// Check file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		c.errorf("Error: configuration file not found: %s\n", configPath)
		c.errorf("Suggestion: run 'canonic bootstrap init' to create one\n")
		return err
	}

	// Load configuration
	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		c.errorf("Error: %v\n", err)
		return err
	}

	c.debugf("Configuration loaded successfully\n")

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		c.errorf("Validation failed: %v\n", err)
		return err
	}

	c.printf("✓ Configuration is valid: %s\n", configPath)

	// Show summary
	c.println("\nConfiguration summary:")
	c.printf("  Gateway:    %s\n", cfg.Gateway.Listen)
	c.printf("  Repository: PostgreSQL\n")
	c.printf("  Engines:    %d configured\n", len(cfg.Engines))
	c.printf("  Tables:     %d defined\n", len(cfg.Tables))
	c.printf("  Roles:      %d defined\n", len(cfg.Roles))

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"status":       "valid",
			"path":         configPath,
			"engine_count": len(cfg.Engines),
			"table_count":  len(cfg.Tables),
			"role_count":   len(cfg.Roles),
		})
	}

	return nil
}

func (c *CLI) newBootstrapApplyCmd() *cobra.Command {
	var (
		configPath string
		confirm    bool
		dryRun     bool
	)

	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply configuration to system",
		Long: `Apply configuration to the canonica system.

Per phase-5-spec.md §2:
  - Apply is idempotent
  - Refuses destructive changes unless explicitly acknowledged
  - Any invariant violation blocks apply

Requirements:
  - Configuration must be validated first
  - PostgreSQL must be accessible
  - Destructive changes require --confirm flag`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runBootstrapApply(configPath, confirm, dryRun)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "canonic.yaml", "configuration file path")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "confirm destructive changes")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be changed without applying")

	return cmd
}

func (c *CLI) runBootstrapApply(configPath string, confirm, dryRun bool) error {
	c.debugf("Applying configuration: %s\n", configPath)

	// Check file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		c.errorf("Error: configuration file not found: %s\n", configPath)
		return err
	}

	// Load configuration
	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		c.errorf("Error loading configuration: %v\n", err)
		return err
	}

	// Validate configuration (required before apply)
	if err := cfg.Validate(); err != nil {
		c.errorf("Validation failed: %v\n", err)
		c.errorf("Run 'canonic bootstrap validate' for details\n")
		return err
	}

	c.printf("✓ Configuration validated\n")

	if dryRun {
		c.println("\nDry-run mode: showing what would be applied")
		c.println("\nTables to create/update:")
		for tableName := range cfg.Tables {
			c.printf("  - %s\n", tableName)
		}
		c.println("\nNo changes were made.")
		return nil
	}

	// Connect to database
	// TODO: Use actual database connection from config
	c.println("\nConnecting to database...")

	// For now, we can't actually connect without the database
	// This would need the gateway client or direct DB connection
	c.errorf("Error: database connection not yet implemented\n")
	c.errorf("Use 'canonic bootstrap validate' to verify configuration\n")

	return fmt.Errorf("apply not yet implemented - use gateway for table registration")
}

// newStatusCmd creates the status command.
func (c *CLI) newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show system status",
		Long: `Display system status including component health.

Per phase-5-spec.md §4: canonic status displays:
  - Gateway readiness
  - Repository health
  - Engine availability
  - Active configuration version`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runStatus()
		},
	}

	return cmd
}

func (c *CLI) runStatus() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use gateway client to get status
	client := c.newGatewayClient()

	// Check gateway health
	healthy, err := client.CheckHealth(ctx)
	if err != nil {
		c.errorf("✗ Gateway: unreachable (%s)\n", c.cfg.Endpoint)
		c.errorf("  Error: %v\n", err)
		return err
	}

	if healthy {
		c.printf("✓ Gateway: healthy (%s)\n", c.cfg.Endpoint)
	} else {
		c.errorf("✗ Gateway: unhealthy\n")
	}

	// Get detailed status via readiness endpoint
	// TODO: Add dedicated status endpoint to gateway

	if c.jsonOutput {
		return c.outputJSON(map[string]interface{}{
			"gateway_healthy": healthy,
			"endpoint":        c.cfg.Endpoint,
		})
	}

	return nil
}

// newAuditCmd creates the audit command.
func (c *CLI) newAuditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Audit and reporting commands",
		Long:  `Commands for audit logs and operational reports.`,
	}

	cmd.AddCommand(c.newAuditSummaryCmd())

	return cmd
}

func (c *CLI) newAuditSummaryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "summary",
		Short: "Show audit summary",
		Long: `Display aggregated audit statistics.

Per phase-5-spec.md §4: canonic audit summary displays:
  - Accepted vs rejected query counts
  - Top rejection reasons
  - Top queried tables

No raw data is exposed.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAuditSummary()
		},
	}

	return cmd
}

func (c *CLI) runAuditSummary() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use gateway client to get audit summary
	client := c.newGatewayClient()

	summary, err := client.GetAuditSummary(ctx)
	if err != nil {
		c.errorf("Error: %v\n", err)
		return err
	}

	c.println("Query Summary:")
	c.printf("  Accepted: %d\n", summary.AcceptedCount)
	c.printf("  Rejected: %d\n", summary.RejectedCount)

	if len(summary.TopRejectionReasons) > 0 {
		c.println("\nTop Rejection Reasons:")
		for _, r := range summary.TopRejectionReasons {
			c.printf("  - %s: %d\n", r.Reason, r.Count)
		}
	}

	if len(summary.TopQueriedTables) > 0 {
		c.println("\nTop Queried Tables:")
		for _, t := range summary.TopQueriedTables {
			c.printf("  - %s: %d\n", t.Table, t.Count)
		}
	}

	if c.jsonOutput {
		return c.outputJSON(summary)
	}

	return nil
}
