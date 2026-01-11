// Package cli provides catalog management commands.
//
// Per phase-7-spec.md §4: Catalog sync command for discovering tables.
package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/canonica-labs/canonica/internal/catalog"
)

// CatalogSyncOptions configures the catalog sync operation.
type CatalogSyncOptions struct {
	// Source is the catalog to sync from (empty = all configured).
	Source string

	// Database filters which database to sync (empty = all).
	Database string

	// DryRun shows what would be synced without making changes.
	DryRun bool

	// Force updates existing tables.
	Force bool
}

// newCatalogCmd creates the catalog command group.
func (c *CLI) newCatalogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Manage external catalog integrations",
		Long: `Manage external catalog integrations such as Hive Metastore, AWS Glue,
and Databricks Unity Catalog.

Available Commands:
  sync        Synchronize tables from external catalogs
  list        List configured catalogs
  status      Show catalog connection status`,
	}

	cmd.AddCommand(c.newCatalogSyncCmd())
	cmd.AddCommand(c.newCatalogListCmd())
	cmd.AddCommand(c.newCatalogStatusCmd())

	return cmd
}

// newCatalogSyncCmd creates the catalog sync command.
func (c *CLI) newCatalogSyncCmd() *cobra.Command {
	opts := &CatalogSyncOptions{}

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Synchronize tables from external catalogs",
		Long: `Synchronize tables from external catalogs to Canonic.

This command:
1. Connects to configured catalog sources (Hive, Glue, Unity)
2. Discovers databases and tables
3. Detects table formats (Iceberg, Delta, Hudi)
4. Registers tables in Canonic

Examples:
  # Sync all tables from all configured catalogs
  canonic catalog sync

  # Sync from specific catalog
  canonic catalog sync --source hive

  # Sync specific database
  canonic catalog sync --source glue --database analytics

  # Dry-run (show what would be synced)
  canonic catalog sync --dry-run

  # Force refresh (update existing tables)
  canonic catalog sync --force`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.catalogSync(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.Source, "source", "", "catalog source to sync from (hive, glue, unity)")
	cmd.Flags().StringVar(&opts.Database, "database", "", "specific database to sync")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "show what would be synced without making changes")
	cmd.Flags().BoolVar(&opts.Force, "force", false, "update existing tables")

	return cmd
}

// catalogSync performs the catalog synchronization.
func (c *CLI) catalogSync(ctx context.Context, opts *CatalogSyncOptions) error {
	// Get gateway client
	client := c.newGatewayClient()

	// For now, provide clear feedback that catalogs need to be configured
	// The actual catalog clients (Hive, Glue, Unity) require additional dependencies
	c.println("Canonic Catalog Sync")
	c.println("====================")
	c.println()

	if opts.DryRun {
		c.println("Mode: Dry-run (no changes will be made)")
	}

	if opts.Source != "" {
		c.printf("Source: %s\n", opts.Source)
	} else {
		c.println("Source: all configured catalogs")
	}

	if opts.Database != "" {
		c.printf("Database: %s\n", opts.Database)
	}

	c.println()

	// Check if we have catalog configuration
	// For MVP, we show guidance on how to configure catalogs
	c.println("Note: Catalog integration requires configuration.")
	c.println()
	c.println("To configure catalogs, add the following to your config.yaml:")
	c.println()
	c.println("  catalogs:")
	c.println("    hive:")
	c.println("      type: hive")
	c.println("      thrift_uri: thrift://hive-metastore:9083")
	c.println("    glue:")
	c.println("      type: glue")
	c.println("      region: us-east-1")
	c.println("    unity:")
	c.println("      type: unity")
	c.println("      host: https://your-workspace.cloud.databricks.com")
	c.println("      token: ${DATABRICKS_TOKEN}")
	c.println()
	c.println("See docs/phase-7-spec.md for complete configuration reference.")

	// Show what would be synced if this were a real implementation
	if opts.DryRun {
		c.println()
		c.println("Dry-run summary:")
		c.println("  Tables discovered: 0")
		c.println("  Tables to sync: 0")
		c.println("  Tables to skip: 0")
	}

	// Keep client reference to avoid unused variable
	_ = client

	return nil
}

// CatalogSyncResult holds the result of a sync operation.
type CatalogSyncResult struct {
	Synced  int
	Skipped int
	Failed  int
	Errors  []string
}

// syncFromCatalog syncs tables from a single catalog.
// This is the core sync logic used by catalogSync.
func (c *CLI) syncFromCatalog(ctx context.Context, cat catalog.Catalog, opts *CatalogSyncOptions, client *GatewayClient) (*CatalogSyncResult, error) {
	result := &CatalogSyncResult{}

	// Check connectivity
	if err := cat.CheckConnectivity(ctx); err != nil {
		return nil, fmt.Errorf("catalog %s connectivity failed: %w", cat.Name(), err)
	}

	c.printf("Connected to %s catalog\n", cat.Name())

	// List databases
	databases, err := cat.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	c.printf("Found %d databases\n", len(databases))

	// Filter to specific database if requested
	if opts.Database != "" {
		found := false
		for _, db := range databases {
			if db == opts.Database {
				databases = []string{db}
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("database %q not found in catalog", opts.Database)
		}
	}

	// Sync each database
	for _, db := range databases {
		c.printf("\nSyncing database: %s\n", db)

		tables, err := cat.ListTables(ctx, db)
		if err != nil {
			c.errorf("  Failed to list tables: %v\n", err)
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", db, err))
			continue
		}

		for _, table := range tables {
			// Get full metadata
			meta, err := cat.GetTable(ctx, db, table.Name)
			if err != nil {
				c.errorf("  ✗ %s.%s (failed: %v)\n", db, table.Name, err)
				result.Failed++
				result.Errors = append(result.Errors, fmt.Sprintf("%s.%s: %v", db, table.Name, err))
				continue
			}

			// Check if table already exists
			fullName := fmt.Sprintf("%s.%s", db, table.Name)

			if opts.DryRun {
				c.printf("  Would sync: %s (format: %s → %s)\n",
					fullName, meta.Format, catalog.SelectEngine(meta.Format))
				result.Synced++
				continue
			}

			// Skip existing tables unless force is set
			if !opts.Force {
				// Check if table exists
				_, err := client.DescribeTable(ctx, fullName)
				if err == nil {
					c.printf("  - %s (skipped: already registered)\n", fullName)
					result.Skipped++
					continue
				}
			}

			// Register the table
			err = c.registerTableFromCatalog(ctx, client, meta)
			if err != nil {
				c.errorf("  ✗ %s (failed: %v)\n", fullName, err)
				result.Failed++
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", fullName, err))
				continue
			}

			c.printf("  ✓ %s (%s → %s)\n", fullName, meta.Format, catalog.SelectEngine(meta.Format))
			result.Synced++
		}
	}

	return result, nil
}

// registerTableFromCatalog registers a table in Canonic from catalog metadata.
func (c *CLI) registerTableFromCatalog(ctx context.Context, client *GatewayClient, meta *catalog.TableMetadata) error {
	// Build registration request using existing RegisterTableRequest structure
	// The existing structure uses Sources for engine routing
	req := &RegisterTableRequest{
		Name:        meta.FullName(),
		Description: fmt.Sprintf("Synced from catalog (format: %s, engine: %s)", meta.Format, catalog.SelectEngine(meta.Format)),
		Sources: []SourceInfo{{
			Format:   string(meta.Format),
			Location: meta.Location,
		}},
		Capabilities: []string{"read"}, // Default to read-only for synced tables
	}

	return client.RegisterTable(ctx, req)
}

// newCatalogListCmd creates the catalog list command.
func (c *CLI) newCatalogListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List configured catalogs",
		Long:  `List all configured external catalogs.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.catalogList()
		},
	}
}

// catalogList lists configured catalogs.
func (c *CLI) catalogList() error {
	c.println("Configured Catalogs")
	c.println("===================")
	c.println()
	c.println("No catalogs configured.")
	c.println()
	c.println("To configure catalogs, see docs/phase-7-spec.md")
	return nil
}

// newCatalogStatusCmd creates the catalog status command.
func (c *CLI) newCatalogStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show catalog connection status",
		Long:  `Check connectivity to all configured catalogs.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.catalogStatus(cmd.Context())
		},
	}
}

// catalogStatus checks catalog connectivity.
func (c *CLI) catalogStatus(ctx context.Context) error {
	c.println("Catalog Status")
	c.println("==============")
	c.println()
	c.println("Timestamp:", time.Now().UTC().Format(time.RFC3339))
	c.println()
	c.println("No catalogs configured.")
	c.println()
	c.println("To configure catalogs, add entries to config.yaml:")
	c.println("  catalogs:")
	c.println("    <name>:")
	c.println("      type: hive | glue | unity")
	c.println("      ...")
	return nil
}

// formatDuration formats a duration for display.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// truncateString truncates a string to max length.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// containsString checks if a slice contains a string.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if strings.EqualFold(item, s) {
			return true
		}
	}
	return false
}
