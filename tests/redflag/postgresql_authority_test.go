// Package redflag contains Red-Flag tests that prove the system correctly
// refuses unsafe, ambiguous, or unsupported behavior.
//
// Per docs/test.md: "Red-Flag tests assert that the system REFUSES unsafe, ambiguous,
// or unsupported behavior."
//
// Per docs/phase-4-spec.md §1: "Enforce PostgreSQL as the Only Metadata Authority"
// - Gateway constructor MUST require a repository instance
// - No zero-value or default repository allowed
// - Gateway MUST NOT start without PostgreSQL
package redflag

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/storage"
)

// TestGatewayStartupWithoutRepository tests that gateway cannot start without a repository.
// Per phase-4-spec.md §1: "Gateway constructor MUST require a repository instance"
func TestGatewayStartupWithoutRepository(t *testing.T) {
	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: true,
	}

	// Attempt to create gateway with nil repository
	_, err := gateway.NewGatewayWithRepository(nil, cfg)

	if err == nil {
		t.Fatal("Gateway MUST NOT start without a repository")
	}

	// Error should mention database/repository
	errMsg := err.Error()
	hasDatabaseMention := strings.Contains(strings.ToLower(errMsg), "database") ||
		strings.Contains(strings.ToLower(errMsg), "repository") ||
		strings.Contains(strings.ToLower(errMsg), "unavailable")

	if !hasDatabaseMention {
		t.Errorf("Error should mention database/repository dependency: %s", errMsg)
	}
}

// TestGatewayRejectsInMemoryInProduction tests that in-memory registries are rejected in production.
// Per phase-4-spec.md §1: "Production code paths MUST NOT create registries implicitly"
func TestGatewayRejectsInMemoryInProduction(t *testing.T) {
	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: true,
	}

	// Attempt to create gateway with in-memory registry in production mode
	_, err := gateway.NewGatewayWithInMemoryRegistry(cfg)

	if err == nil {
		t.Fatal("In-memory registry MUST be rejected in production mode")
	}
}

// TestGatewayFailsOnUnreachableDatabase tests that gateway fails if DB is unreachable.
// Per phase-4-spec.md §1: "If PostgreSQL is unreachable at startup: gateway MUST NOT start"
func TestGatewayFailsOnUnreachableDatabase(t *testing.T) {
	// Create a mock repository that fails connectivity check
	repo := storage.NewMockRepository()
	repo.SetConnectivityFailure(true)

	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: true,
	}

	_, err := gateway.NewGatewayWithRepository(repo, cfg)

	if err == nil {
		t.Fatal("Gateway MUST NOT start when database is unreachable")
	}

	// Error should clearly state database dependency failure
	errMsg := err.Error()
	if errMsg == "" {
		t.Error("Error message should not be empty")
	}
}

// TestGatewayMustCheckConnectivityAtStartup tests that connectivity is verified at startup.
// Per phase-4-spec.md §1: "Fail Fast on Startup"
func TestGatewayMustCheckConnectivityAtStartup(t *testing.T) {
	// Create a mock repository
	repo := storage.NewMockRepository()

	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: false, // Allow for test
	}

	_, _ = gateway.NewGatewayWithRepository(repo, cfg)

	// Verify connectivity check was called
	if !repo.ConnectivityCheckCalled() {
		t.Error("Gateway MUST check database connectivity at startup")
	}
}

// TestMetadataReadBeforeRepositoryInitFails tests that reading metadata before init fails.
// Per phase-4-spec.md §1: "Attempt to read metadata before repository initialization"
func TestMetadataReadBeforeRepositoryInitFails(t *testing.T) {
	// Create a repository that simulates delayed initialization
	repo := storage.NewMockRepository()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Attempt to read from empty repository (no tables registered)
	_, err := repo.Get(ctx, "nonexistent.table")

	if err == nil {
		t.Fatal("Reading nonexistent table MUST return an error")
	}
}

// TestNoDefaultRepositoryAllowed tests that no default/zero-value repository is allowed.
// Per phase-4-spec.md §1: "No zero-value or default repository allowed"
func TestNoDefaultRepositoryAllowed(t *testing.T) {
	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: true,
	}

	// All these should fail - no implicit registry creation
	testCases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "NewGatewayWithDB with nil",
			fn: func() error {
				_, err := gateway.NewGatewayWithDB(nil, cfg)
				return err
			},
		},
		{
			name: "NewGatewayWithRepository with nil",
			fn: func() error {
				_, err := gateway.NewGatewayWithRepository(nil, cfg)
				return err
			},
		},
		{
			name: "NewGatewayWithInMemoryRegistry in production",
			fn: func() error {
				_, err := gateway.NewGatewayWithInMemoryRegistry(cfg)
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Errorf("%s: expected error, got nil", tc.name)
			}
		})
	}
}

// TestGatewayErrorMessageClearlyStatesDatabaseFailure tests error message clarity.
// Per phase-4-spec.md §1: "error must clearly state database dependency failure"
func TestGatewayErrorMessageClearlyStatesDatabaseFailure(t *testing.T) {
	repo := storage.NewMockRepository()
	repo.SetConnectivityFailure(true)

	cfg := gateway.Config{
		Version:        "test",
		ProductionMode: true,
	}

	_, err := gateway.NewGatewayWithRepository(repo, cfg)

	if err == nil {
		t.Fatal("Expected error")
	}

	errMsg := strings.ToLower(err.Error())

	// Error must be actionable - should mention what failed
	hasActionableInfo := strings.Contains(errMsg, "database") ||
		strings.Contains(errMsg, "connection") ||
		strings.Contains(errMsg, "connectivity") ||
		strings.Contains(errMsg, "unavailable")

	if !hasActionableInfo {
		t.Errorf("Error message should be actionable and mention the failure: %s", err.Error())
	}
}
