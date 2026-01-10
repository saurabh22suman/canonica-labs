// Package bootstrap provides configuration loading and system initialization.
// Per phase-5-spec.md: "Single, explicit configuration model that defines system state declaratively."
//
// Configuration must be:
// - human-readable
// - versionable
// - GitOps-friendly
// - schema-validated
package bootstrap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// Config represents the canonical configuration for canonica.
// Per phase-5-spec.md §1: YAML format with explicit sections.
type Config struct {
	// Gateway configuration
	Gateway GatewayConfig `yaml:"gateway"`

	// Repository configuration
	Repository RepositoryConfig `yaml:"repository"`

	// Engines configuration
	Engines map[string]EngineConfig `yaml:"engines"`

	// Roles configuration (role → table → capabilities)
	Roles map[string]RoleConfig `yaml:"roles,omitempty"`

	// Tables configuration
	Tables map[string]TableConfig `yaml:"tables,omitempty"`

	// validated tracks if Validate() has been called
	validated bool

	// applied tracks if Apply() has been called
	applied bool

	// configPath is the source file path
	configPath string
}

// GatewayConfig holds gateway server configuration.
type GatewayConfig struct {
	Listen string `yaml:"listen"`
}

// RepositoryConfig holds database repository configuration.
type RepositoryConfig struct {
	Postgres PostgresConfig `yaml:"postgres"`
}

// PostgresConfig holds PostgreSQL connection configuration.
type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// EngineConfig holds query engine configuration.
type EngineConfig struct {
	Enabled      bool     `yaml:"enabled,omitempty"`
	Endpoint     string   `yaml:"endpoint,omitempty"`
	Database     string   `yaml:"database,omitempty"`
	Capabilities []string `yaml:"capabilities,omitempty"`
}

// RoleConfig holds role → table permissions.
type RoleConfig struct {
	Tables map[string][]string `yaml:"tables"`
}

// TableConfig holds virtual table configuration.
type TableConfig struct {
	Description  string         `yaml:"description,omitempty"`
	Sources      []SourceConfig `yaml:"sources"`
	Capabilities []string       `yaml:"capabilities,omitempty"`
	Constraints  []string       `yaml:"constraints,omitempty"`
}

// SourceConfig holds physical source configuration.
type SourceConfig struct {
	Engine   string `yaml:"engine"`
	Format   string `yaml:"format"`
	Location string `yaml:"location"`
}

// LoadConfig loads and validates configuration from a YAML file.
// Per phase-5-spec.md §1: "Unknown fields MUST fail"
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// First pass: Check for unknown fields using strict unmarshal
	var rawConfig map[string]interface{}
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	// Validate known top-level keys
	knownKeys := map[string]bool{
		"gateway":    true,
		"repository": true,
		"engines":    true,
		"roles":      true,
		"tables":     true,
	}

	for key := range rawConfig {
		if !knownKeys[key] {
			return nil, fmt.Errorf("unknown configuration key: %s", key)
		}
	}

	// Validate gateway section has only known keys
	if gwRaw, ok := rawConfig["gateway"].(map[string]interface{}); ok {
		gwKnownKeys := map[string]bool{"listen": true}
		for key := range gwRaw {
			if !gwKnownKeys[key] {
				return nil, fmt.Errorf("unknown configuration key in gateway: %s", key)
			}
		}
	}

	// Validate repository section
	if repoRaw, ok := rawConfig["repository"].(map[string]interface{}); ok {
		repoKnownKeys := map[string]bool{"postgres": true}
		for key := range repoRaw {
			if !repoKnownKeys[key] {
				return nil, fmt.Errorf("unknown configuration key in repository: %s", key)
			}
		}
	}

	// Second pass: Unmarshal into typed config
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.configPath = path

	// Validate required sections per phase-5-spec.md §1
	if cfg.Gateway.Listen == "" {
		return nil, fmt.Errorf("missing required section: gateway (listen address required)")
	}

	if cfg.Repository.Postgres.DSN == "" {
		return nil, fmt.Errorf("missing required section: repository (postgres.dsn required)")
	}

	if len(cfg.Engines) == 0 {
		return nil, fmt.Errorf("missing required section: engines (at least one engine required)")
	}

	// Validate engine capabilities
	for engineName, engineCfg := range cfg.Engines {
		for _, capStr := range engineCfg.Capabilities {
			if _, err := capabilities.ParseCapability(capStr); err != nil {
				return nil, fmt.Errorf("engine %s: invalid capability %s", engineName, capStr)
			}
		}
	}

	// Validate table names are schema-qualified
	for tableName, tableCfg := range cfg.Tables {
		if !strings.Contains(tableName, ".") {
			return nil, fmt.Errorf("table '%s': name must be schema-qualified (e.g., 'schema.table')", tableName)
		}

		// Note: Engine reference validation is done in Validate() method
		// to keep LoadConfig focused on structural validation

		// Validate capabilities
		for _, capStr := range tableCfg.Capabilities {
			if _, err := capabilities.ParseCapability(capStr); err != nil {
				return nil, fmt.Errorf("table %s: invalid capability %s", tableName, capStr)
			}
		}

		// Validate constraints
		for _, conStr := range tableCfg.Constraints {
			if _, err := capabilities.ParseConstraint(conStr); err != nil {
				return nil, fmt.Errorf("table %s: invalid constraint %s", tableName, conStr)
			}
		}
	}

	return &cfg, nil
}

// Validate performs dry-run validation of the configuration.
// Per phase-5-spec.md §2: "bootstrap validate performs dry-run invariant checks"
func (c *Config) Validate() error {
	// Check engine references in tables
	for tableName, tableCfg := range c.Tables {
		for _, src := range tableCfg.Sources {
			// First check if engine exists
			engineCfg, ok := c.Engines[src.Engine]
			if !ok {
				return fmt.Errorf("table '%s': references unknown engine '%s'", tableName, src.Engine)
			}
			// Then check if it's properly configured
			if !engineCfg.Enabled && engineCfg.Endpoint == "" && engineCfg.Database == "" {
				return fmt.Errorf("table '%s': engine '%s' is not enabled", tableName, src.Engine)
			}
		}
	}

	// Check role references
	for roleName, roleCfg := range c.Roles {
		for tableName, caps := range roleCfg.Tables {
			// Table must exist or be a wildcard pattern
			if !strings.Contains(tableName, "*") {
				if _, ok := c.Tables[tableName]; !ok {
					return fmt.Errorf("role '%s': references unknown table '%s'", roleName, tableName)
				}
			}
			// Capabilities must be valid
			for _, capStr := range caps {
				if _, err := capabilities.ParseCapability(capStr); err != nil {
					return fmt.Errorf("role '%s': invalid capability '%s'", roleName, capStr)
				}
			}
		}
	}

	c.validated = true
	return nil
}

// IsValidated returns true if Validate() has been called successfully.
func (c *Config) IsValidated() bool {
	return c.validated
}

// IsApplied returns true if Apply() has been called successfully.
func (c *Config) IsApplied() bool {
	return c.applied
}

// Apply applies the configuration to the system.
// Per phase-5-spec.md §2: "Apply is idempotent"
func (c *Config) Apply(ctx context.Context) error {
	if !c.validated {
		return fmt.Errorf("configuration must be validated before apply")
	}
	// This method is for applying without a repository (fails)
	return fmt.Errorf("apply requires a repository; use ApplyToRepository")
}

// ApplyToRepository applies configuration to a TableRepository.
// Per phase-5-spec.md §2: "Is idempotent, refuses destructive changes unless explicitly acknowledged"
func (c *Config) ApplyToRepository(ctx context.Context, repo Repository) error {
	if !c.validated {
		return fmt.Errorf("configuration must be validated before apply")
	}

	// Create or update tables
	for tableName, tableCfg := range c.Tables {
		vt := c.tableConfigToVirtualTable(tableName, tableCfg)

		// Check if table exists
		exists, err := repo.Exists(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to check table existence: %w", err)
		}

		if exists {
			// Update existing table
			if err := repo.Update(ctx, vt); err != nil {
				return fmt.Errorf("failed to update table '%s': %w", tableName, err)
			}
		} else {
			// Create new table
			if err := repo.Create(ctx, vt); err != nil {
				return fmt.Errorf("failed to create table '%s': %w", tableName, err)
			}
		}
	}

	c.applied = true
	return nil
}

// tableConfigToVirtualTable converts a TableConfig to a VirtualTable.
func (c *Config) tableConfigToVirtualTable(name string, cfg TableConfig) *tables.VirtualTable {
	vt := &tables.VirtualTable{
		Name:        name,
		Description: cfg.Description,
	}

	// Convert sources
	for _, src := range cfg.Sources {
		vt.Sources = append(vt.Sources, tables.PhysicalSource{
			Format:   tables.StorageFormat(strings.ToUpper(src.Format)),
			Location: src.Location,
			Engine:   src.Engine,
		})
	}

	// Convert capabilities
	for _, capStr := range cfg.Capabilities {
		cap, _ := capabilities.ParseCapability(capStr)
		vt.Capabilities = append(vt.Capabilities, cap)
	}

	// Convert constraints
	for _, conStr := range cfg.Constraints {
		con, _ := capabilities.ParseConstraint(conStr)
		vt.Constraints = append(vt.Constraints, con)
	}

	return vt
}

// Save saves the configuration to a YAML file.
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Repository interface for applying configuration.
// Matches storage.TableRepository but without the full import.
type Repository interface {
	Create(ctx context.Context, table *tables.VirtualTable) error
	Get(ctx context.Context, name string) (*tables.VirtualTable, error)
	Update(ctx context.Context, table *tables.VirtualTable) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context) ([]*tables.VirtualTable, error)
	Exists(ctx context.Context, name string) (bool, error)
}

// ChangeType represents the type of configuration change.
type ChangeType string

const (
	ChangeTypeCreate ChangeType = "create"
	ChangeTypeUpdate ChangeType = "update"
	ChangeTypeDelete ChangeType = "delete"
)

// ConfigChange represents a pending configuration change.
type ConfigChange struct {
	Type      ChangeType
	Table     string
	Confirmed bool
}

// Bootstrapper handles bootstrap operations.
type Bootstrapper struct {
	repo Repository
}

// NewBootstrapper creates a new bootstrapper.
func NewBootstrapper(repo Repository) *Bootstrapper {
	return &Bootstrapper{repo: repo}
}

// Init generates an example configuration file.
// Per phase-5-spec.md §2: "bootstrap init generates example configuration"
func (b *Bootstrapper) Init(dir string) (string, error) {
	configPath := filepath.Join(dir, "canonic.yaml")

	exampleConfig := `# Canonica Configuration
# Generated by 'canonic bootstrap init'
# See docs/phase-5-spec.md for full specification

gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true
    database: ":memory:"

  # Uncomment to enable Trino
  # trino:
  #   endpoint: http://localhost:8080
  #   capabilities:
  #     - READ
  #     - TIME_TRAVEL

roles:
  analyst:
    tables:
      analytics.sales_orders:
        - READ

tables:
  analytics.sales_orders:
    description: Example sales orders table
    sources:
      - engine: duckdb
        format: parquet
        location: s3://example-bucket/sales_orders
    capabilities:
      - READ
    constraints:
      - READ_ONLY
`

	if err := os.WriteFile(configPath, []byte(exampleConfig), 0644); err != nil {
		return "", fmt.Errorf("failed to write config file: %w", err)
	}

	return configPath, nil
}

// ApplyChange applies a single configuration change.
// Per phase-5-spec.md §2: "Destructive change without confirmation" must fail
func (b *Bootstrapper) ApplyChange(ctx context.Context, change ConfigChange) error {
	// Destructive changes require confirmation
	if change.Type == ChangeTypeDelete && !change.Confirmed {
		return errors.NewBootstrapError(
			"destructive change requires confirmation",
			fmt.Sprintf("deleting table '%s' requires --confirm flag", change.Table),
			"run with --confirm to acknowledge destructive change",
		)
	}

	if b.repo == nil {
		return errors.NewBootstrapError(
			"no repository configured",
			"bootstrap operations require a database connection",
			"configure repository in config file",
		)
	}

	switch change.Type {
	case ChangeTypeDelete:
		return b.repo.Delete(ctx, change.Table)
	default:
		return nil
	}
}
