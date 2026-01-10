// Package redflag contains tests that prove unsafe behavior is blocked.
// Red-Flag tests MUST fail before implementation and pass after.
//
// This file tests Phase 5 bootstrap and configuration requirements.
// Per phase-5-spec.md: "Invalid configuration MUST fail validation"
package redflag

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/bootstrap"
)

// TestBootstrap_RejectsMissingRequiredSection verifies that configuration
// without required sections is rejected.
// Per phase-5-spec.md §1: "Partial configuration MUST fail"
func TestBootstrap_RejectsMissingRequiredSection(t *testing.T) {
	testCases := []struct {
		name     string
		config   string
		wantErr  string
	}{
		{
			name: "missing gateway section",
			config: `
repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic
engines:
  duckdb:
    enabled: true
`,
			wantErr: "gateway",
		},
		{
			name: "missing repository section",
			config: `
gateway:
  listen: :8080
engines:
  duckdb:
    enabled: true
`,
			wantErr: "repository",
		},
		{
			name: "missing engines section",
			config: `
gateway:
  listen: :8080
repository:
  postgres:
    dsn: postgres://canonic:canonic@localhost:5432/canonic
`,
			wantErr: "engines",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temp file with config
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(configPath, []byte(tc.config), 0644); err != nil {
				t.Fatalf("failed to write config: %v", err)
			}

			// Attempt to load configuration
			_, err := bootstrap.LoadConfig(configPath)
			if err == nil {
				t.Errorf("expected error for missing %s section, got nil", tc.wantErr)
			}

			// Error should mention the missing section
			if err != nil && !containsString(err.Error(), tc.wantErr) {
				t.Errorf("error should mention '%s', got: %v", tc.wantErr, err)
			}
		})
	}
}

// TestBootstrap_RejectsUnknownConfigKeys verifies that unknown configuration
// keys are rejected (no permissive mode).
// Per phase-5-spec.md §1: "Unknown fields MUST fail"
func TestBootstrap_RejectsUnknownConfigKeys(t *testing.T) {
	config := `
gateway:
  listen: :8080
  unknown_field: "should fail"
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

	_, err := bootstrap.LoadConfig(configPath)
	if err == nil {
		t.Error("expected error for unknown field, got nil")
	}

	if err != nil && !containsString(err.Error(), "unknown") {
		t.Errorf("error should mention 'unknown', got: %v", err)
	}
}

// TestBootstrap_RejectsInvalidCapabilityNames verifies that invalid
// capability names are rejected during configuration validation.
// Per phase-5-spec.md §1: "Invalid capability names" must fail
func TestBootstrap_RejectsInvalidCapabilityNames(t *testing.T) {
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
      - INVALID_CAPABILITY
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	_, err := bootstrap.LoadConfig(configPath)
	if err == nil {
		t.Error("expected error for invalid capability, got nil")
	}

	if err != nil && !containsString(err.Error(), "INVALID_CAPABILITY") {
		t.Errorf("error should mention 'INVALID_CAPABILITY', got: %v", err)
	}
}

// TestBootstrap_RejectsTableWithoutSchemaQualifiedName verifies that tables
// without schema-qualified names are rejected.
// Per phase-5-spec.md §1: "Table without schema-qualified name" must fail
func TestBootstrap_RejectsTableWithoutSchemaQualifiedName(t *testing.T) {
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
  sales_orders:
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

	_, err := bootstrap.LoadConfig(configPath)
	if err == nil {
		t.Error("expected error for unqualified table name, got nil")
	}

	if err != nil && !containsString(err.Error(), "schema") {
		t.Errorf("error should mention 'schema', got: %v", err)
	}
}

// TestBootstrap_ApplyWithoutValidate verifies that applying configuration
// without prior validation fails.
// Per phase-5-spec.md §2: "Apply without validate" must fail
func TestBootstrap_ApplyWithoutValidate(t *testing.T) {
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

	// Load but don't validate
	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load should succeed: %v", err)
	}

	// Apply without validate should fail
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cfg.Apply(ctx)
	if err == nil {
		t.Error("expected error for apply without validate, got nil")
	}

	if err != nil && !containsString(err.Error(), "validate") {
		t.Errorf("error should mention 'validate', got: %v", err)
	}
}

// TestBootstrap_ApplyInvalidConfiguration verifies that applying invalid
// configuration fails with clear error message.
// Per phase-5-spec.md §2: "Apply invalid configuration" must fail
func TestBootstrap_ApplyInvalidConfiguration(t *testing.T) {
	// Create minimal config but with invalid engine reference in table
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
      - engine: nonexistent_engine
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
		t.Fatalf("load should succeed: %v", err)
	}

	// Validation should fail due to invalid engine reference
	err = cfg.Validate()
	if err == nil {
		t.Error("expected validation error for invalid engine reference")
	}

	if err != nil && !containsString(err.Error(), "engine") {
		t.Errorf("error should mention 'engine', got: %v", err)
	}
}

// TestBootstrap_DestructiveChangeWithoutConfirmation verifies that destructive
// changes require explicit acknowledgment.
// Per phase-5-spec.md §2: "Destructive change without confirmation" must fail
func TestBootstrap_DestructiveChangeWithoutConfirmation(t *testing.T) {
	// This test requires a pre-existing state to detect destructive changes
	// For now, we test that removing a table requires confirmation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bootstrapper := bootstrap.NewBootstrapper(nil) // nil repository for test

	// Simulate a destructive change (removing a table)
	change := bootstrap.ConfigChange{
		Type:       bootstrap.ChangeTypeDelete,
		Table:      "analytics.sales_orders",
		Confirmed:  false,
	}

	err := bootstrapper.ApplyChange(ctx, change)
	if err == nil {
		t.Error("expected error for unconfirmed destructive change")
	}

	if err != nil && !containsString(err.Error(), "confirm") {
		t.Errorf("error should mention 'confirm', got: %v", err)
	}
}

// containsString checks if s contains substr (case-insensitive).
func containsString(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr || containsString(s[1:], substr)))
}
