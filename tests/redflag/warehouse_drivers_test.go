// Package redflag contains tests that MUST fail if invariants are violated.
// Per docs/test.md: "Red-Flag tests are mandatory for all new features."
//
// This file tests T058/T059: Warehouse Driver Integration.
package redflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/adapters/bigquery"
	"github.com/canonica-labs/canonica/internal/adapters/snowflake"
)

// TestSnowflakeAdapter_RequiresAccount verifies Snowflake adapter
// rejects missing account configuration.
// Per phase-8-spec.md §4.1: Account is required
func TestSnowflakeAdapter_RequiresAccount(t *testing.T) {
	ctx := context.Background()

	_, err := snowflake.NewAdapter(ctx, snowflake.Config{
		User:      "test",
		Password:  "test",
		Warehouse: "test",
		// Account is missing
	})
	if err == nil {
		t.Error("Expected error for missing account")
	}
}

// TestSnowflakeAdapter_RequiresCredentials verifies Snowflake adapter
// rejects missing authentication.
// Per phase-8-spec.md §4.1: Password or PrivateKey required
func TestSnowflakeAdapter_RequiresCredentials(t *testing.T) {
	ctx := context.Background()

	_, err := snowflake.NewAdapter(ctx, snowflake.Config{
		Account:   "test.us-east-1",
		User:      "test",
		Warehouse: "test",
		// No password or private key
	})
	if err == nil {
		t.Error("Expected error for missing credentials")
	}
}

// TestSnowflakeAdapter_RequiresWarehouse verifies Snowflake adapter
// rejects missing warehouse configuration.
// Per phase-8-spec.md §4.1: Warehouse required for query execution
func TestSnowflakeAdapter_RequiresWarehouse(t *testing.T) {
	ctx := context.Background()

	_, err := snowflake.NewAdapter(ctx, snowflake.Config{
		Account:  "test.us-east-1",
		User:     "test",
		Password: "test",
		// Warehouse is missing
	})
	if err == nil {
		t.Error("Expected error for missing warehouse")
	}
}

// TestSnowflakeAdapter_HasCorrectName verifies adapter name.
// Note: Uses NewAdapterWithoutConnect to avoid network dependency.
func TestSnowflakeAdapter_HasCorrectName(t *testing.T) {
	adapter := snowflake.NewAdapterWithoutConnect(snowflake.Config{
		Account:   "test.us-east-1",
		User:      "test",
		Password:  "test",
		Warehouse: "test",
	})

	if adapter.Name() != "snowflake" {
		t.Errorf("Expected name 'snowflake', got %q", adapter.Name())
	}
}

// TestBigQueryAdapter_RequiresProject verifies BigQuery adapter
// rejects missing project configuration.
// Per phase-8-spec.md §5.1: ProjectID is required
func TestBigQueryAdapter_RequiresProject(t *testing.T) {
	ctx := context.Background()

	_, err := bigquery.NewAdapter(ctx, bigquery.Config{
		// ProjectID is missing
	})
	if err == nil {
		t.Error("Expected error for missing project_id")
	}
}

// TestBigQueryAdapter_HasCorrectName verifies adapter name.
// Note: Uses NewAdapterWithoutConnect to avoid network dependency.
func TestBigQueryAdapter_HasCorrectName(t *testing.T) {
	adapter := bigquery.NewAdapterWithoutConnect(bigquery.Config{
		ProjectID: "test-project",
	})

	if adapter.Name() != "bigquery" {
		t.Errorf("Expected name 'bigquery', got %q", adapter.Name())
	}
}

// TestBigQueryAdapter_HasTimeTravelCapability verifies BigQuery
// reports time-travel capability.
// Per phase-8-spec.md §5: BigQuery supports 7-day time-travel
func TestBigQueryAdapter_HasTimeTravelCapability(t *testing.T) {
	adapter := bigquery.NewAdapterWithoutConnect(bigquery.Config{
		ProjectID: "test-project",
	})

	caps := adapter.Capabilities()
	hasTimeTravel := false
	for _, cap := range caps {
		if cap.String() == "TIME_TRAVEL" {
			hasTimeTravel = true
			break
		}
	}

	if !hasTimeTravel {
		t.Error("BigQuery adapter should report TIME_TRAVEL capability")
	}
}

// TestSnowflakeAdapter_HasTimeTravelCapability verifies Snowflake
// reports time-travel capability.
// Per phase-8-spec.md §4: Snowflake supports 90-day time-travel
func TestSnowflakeAdapter_HasTimeTravelCapability(t *testing.T) {
	adapter := snowflake.NewAdapterWithoutConnect(snowflake.Config{
		Account:   "test.us-east-1",
		User:      "test",
		Password:  "test",
		Warehouse: "test",
	})

	caps := adapter.Capabilities()
	hasTimeTravel := false
	for _, cap := range caps {
		if cap.String() == "TIME_TRAVEL" {
			hasTimeTravel = true
			break
		}
	}

	if !hasTimeTravel {
		t.Error("Snowflake adapter should report TIME_TRAVEL capability")
	}
}
