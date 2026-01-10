// Package greenflag contains tests that prove allowed behavior works correctly.
// Green-Flag tests validate happy paths and deterministic behavior.
//
// This file tests Phase 5 bootstrap and configuration requirements.
// Per phase-5-spec.md: "Valid full configuration loads successfully"
package greenflag

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/bootstrap"
)

// TestBootstrap_ValidConfigurationLoads verifies that a valid full configuration
// loads successfully.
// Per phase-5-spec.md §1 Green-Flag: "Valid full configuration loads successfully"
func TestBootstrap_ValidConfigurationLoads(t *testing.T) {
	config := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  trino:
    endpoint: http://localhost:8080
    capabilities:
      - READ
      - TIME_TRAVEL
  duckdb:
    enabled: true
    database: ":memory:"

roles:
  analyst:
    tables:
      analytics.sales_orders:
        - READ
        - TIME_TRAVEL

tables:
  analytics.sales_orders:
    sources:
      - engine: trino
        format: iceberg
        location: s3://warehouse/sales_orders
    constraints:
      - SNAPSHOT_CONSISTENT
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("expected valid config to load, got error: %v", err)
	}

	// Verify key fields are populated
	if cfg.Gateway.Listen != ":8080" {
		t.Errorf("expected gateway.listen=:8080, got %s", cfg.Gateway.Listen)
	}

	if cfg.Repository.Postgres.DSN != "postgres://canonic:canonic@localhost:5432/canonic" {
		t.Errorf("unexpected repository DSN: %s", cfg.Repository.Postgres.DSN)
	}

	if len(cfg.Engines) == 0 {
		t.Error("expected engines to be configured")
	}

	if len(cfg.Tables) == 0 {
		t.Error("expected tables to be configured")
	}

	if len(cfg.Roles) == 0 {
		t.Error("expected roles to be configured")
	}
}

// TestBootstrap_ConfigurationRoundTrips verifies that configuration
// can be saved and reloaded without loss.
// Per phase-5-spec.md §1 Green-Flag: "Configuration round-trips without loss"
func TestBootstrap_ConfigurationRoundTrips(t *testing.T) {
	config := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true
    database: /tmp/test.duckdb

tables:
  analytics.sales_orders:
    description: Sales order data
    sources:
      - engine: duckdb
        format: parquet
        location: s3://bucket/sales_orders
    capabilities:
      - READ
    constraints:
      - READ_ONLY
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	// Load original
	original, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Save to new file
	savePath := filepath.Join(dir, "saved.yaml")
	if err := original.Save(savePath); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	// Reload saved file
	reloaded, err := bootstrap.LoadConfig(savePath)
	if err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}

	// Compare key fields
	if original.Gateway.Listen != reloaded.Gateway.Listen {
		t.Errorf("gateway.listen changed: %s -> %s",
			original.Gateway.Listen, reloaded.Gateway.Listen)
	}

	if original.Repository.Postgres.DSN != reloaded.Repository.Postgres.DSN {
		t.Errorf("repository DSN changed: %s -> %s",
			original.Repository.Postgres.DSN, reloaded.Repository.Postgres.DSN)
	}

	if len(original.Tables) != len(reloaded.Tables) {
		t.Errorf("table count changed: %d -> %d",
			len(original.Tables), len(reloaded.Tables))
	}
}

// TestBootstrap_InitGeneratesExample verifies that bootstrap init
// generates a valid example configuration.
// Per phase-5-spec.md §2: "bootstrap init generates example configuration"
func TestBootstrap_InitGeneratesExample(t *testing.T) {
	dir := t.TempDir()
	
	bootstrapper := bootstrap.NewBootstrapper(nil)
	
	configPath, err := bootstrapper.Init(dir)
	if err != nil {
		t.Fatalf("bootstrap init failed: %v", err)
	}

	// File should exist
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Fatalf("config file not created at %s", configPath)
	}

	// Should be loadable and valid
	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("generated config not loadable: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("generated config not valid: %v", err)
	}
}

// TestBootstrap_ValidatePerformsDryRun verifies that bootstrap validate
// performs dry-run invariant checks without modifying state.
// Per phase-5-spec.md §2: "Performs dry-run invariant checks"
func TestBootstrap_ValidatePerformsDryRun(t *testing.T) {
	config := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true

tables:
  analytics.sales_orders:
    sources:
      - engine: duckdb
        format: parquet
        location: s3://bucket/path
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Validate should succeed without side effects
	if err := cfg.Validate(); err != nil {
		t.Errorf("validation failed: %v", err)
	}

	// Config should not be marked as applied
	if cfg.IsApplied() {
		t.Error("config should not be marked as applied after validate")
	}
}

// TestBootstrap_CleanInstallSucceeds verifies that a clean install
// from scratch succeeds.
// Per phase-5-spec.md §2 Green-Flag: "Clean install succeeds"
func TestBootstrap_CleanInstallSucceeds(t *testing.T) {
	config := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Validate first (required before apply)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	// Create mock repository for clean install test
	mockRepo := bootstrap.NewMockRepository()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Apply should succeed on clean state
	err = cfg.ApplyToRepository(ctx, mockRepo)
	if err != nil {
		t.Fatalf("clean install failed: %v", err)
	}
}

// TestBootstrap_ReApplyIsNoOp verifies that re-applying the same
// configuration is idempotent (no-op).
// Per phase-5-spec.md §2 Green-Flag: "Re-apply is no-op"
func TestBootstrap_ReApplyIsNoOp(t *testing.T) {
	config := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true

tables:
  analytics.sales_orders:
    sources:
      - engine: duckdb
        format: parquet
        location: s3://bucket/path
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	mockRepo := bootstrap.NewMockRepository()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First apply
	if err := cfg.ApplyToRepository(ctx, mockRepo); err != nil {
		t.Fatalf("first apply failed: %v", err)
	}

	tableCount1 := mockRepo.TableCount()

	// Second apply (same config)
	if err := cfg.ApplyToRepository(ctx, mockRepo); err != nil {
		t.Fatalf("re-apply failed: %v", err)
	}

	tableCount2 := mockRepo.TableCount()

	// Table count should be the same (idempotent)
	if tableCount1 != tableCount2 {
		t.Errorf("re-apply changed state: %d tables -> %d tables",
			tableCount1, tableCount2)
	}
}

// TestBootstrap_PartialUpdatesApplySafely verifies that partial updates
// to configuration apply safely without breaking existing state.
// Per phase-5-spec.md §2 Green-Flag: "Partial updates apply safely"
func TestBootstrap_PartialUpdatesApplySafely(t *testing.T) {
	// Initial config with one table
	config1 := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true

tables:
  analytics.sales_orders:
    sources:
      - engine: duckdb
        format: parquet
        location: s3://bucket/sales
`
	// Updated config with additional table
	config2 := `
gateway:
  listen: :8080

repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic

engines:
  duckdb:
    enabled: true

tables:
  analytics.sales_orders:
    sources:
      - engine: duckdb
        format: parquet
        location: s3://bucket/sales
  analytics.customers:
    sources:
      - engine: duckdb
        format: parquet
        location: s3://bucket/customers
`
	dir := t.TempDir()
	mockRepo := bootstrap.NewMockRepository()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Apply first config
	configPath1 := filepath.Join(dir, "config1.yaml")
	if err := os.WriteFile(configPath1, []byte(config1), 0644); err != nil {
		t.Fatalf("failed to write config1: %v", err)
	}

	cfg1, err := bootstrap.LoadConfig(configPath1)
	if err != nil {
		t.Fatalf("failed to load config1: %v", err)
	}
	if err := cfg1.Validate(); err != nil {
		t.Fatalf("config1 validation failed: %v", err)
	}
	if err := cfg1.ApplyToRepository(ctx, mockRepo); err != nil {
		t.Fatalf("config1 apply failed: %v", err)
	}

	// Verify first table exists
	if !mockRepo.HasTable("analytics.sales_orders") {
		t.Error("sales_orders table should exist after first apply")
	}

	// Apply second config (partial update)
	configPath2 := filepath.Join(dir, "config2.yaml")
	if err := os.WriteFile(configPath2, []byte(config2), 0644); err != nil {
		t.Fatalf("failed to write config2: %v", err)
	}

	cfg2, err := bootstrap.LoadConfig(configPath2)
	if err != nil {
		t.Fatalf("failed to load config2: %v", err)
	}
	if err := cfg2.Validate(); err != nil {
		t.Fatalf("config2 validation failed: %v", err)
	}
	if err := cfg2.ApplyToRepository(ctx, mockRepo); err != nil {
		t.Fatalf("config2 apply failed: %v", err)
	}

	// Verify both tables exist
	if !mockRepo.HasTable("analytics.sales_orders") {
		t.Error("sales_orders table should still exist after partial update")
	}
	if !mockRepo.HasTable("analytics.customers") {
		t.Error("customers table should exist after partial update")
	}
}
