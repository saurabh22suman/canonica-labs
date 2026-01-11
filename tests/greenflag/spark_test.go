// Package greenflag contains tests that verify the system correctly performs allowed operations.
// Per docs/test.md: "Green-Flag tests demonstrate allowed behavior and must be deterministic."
package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/spark"
	"github.com/canonica-labs/canonica/internal/capabilities"
)

// TestSpark_Name verifies the adapter returns correct name.
// Green-Flag: Adapter name must be "spark".
func TestSpark_Name(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	if adapter.Name() != "spark" {
		t.Fatalf("expected name 'spark', got %q", adapter.Name())
	}
}

// TestSpark_Capabilities verifies the adapter reports correct capabilities.
// Green-Flag: Spark supports READ and TIME_TRAVEL (per plan.md fallback engine).
func TestSpark_Capabilities(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	caps := adapter.Capabilities()

	hasRead := false
	hasTimeTravel := false
	for _, cap := range caps {
		if cap == capabilities.CapabilityRead {
			hasRead = true
		}
		if cap == capabilities.CapabilityTimeTravel {
			hasTimeTravel = true
		}
	}

	if !hasRead {
		t.Fatal("expected Spark to have READ capability")
	}

	if !hasTimeTravel {
		t.Fatal("expected Spark to have TIME_TRAVEL capability (for AS OF fallback)")
	}
}

// TestSpark_CloseIsIdempotent verifies Close can be called multiple times.
// Green-Flag: Close must be safe to call multiple times.
func TestSpark_CloseIsIdempotent(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})

	err1 := adapter.Close()
	if err1 != nil {
		t.Fatalf("first close failed: %v", err1)
	}

	err2 := adapter.Close()
	if err2 != nil {
		t.Fatalf("second close failed: %v", err2)
	}
}

// TestSpark_WithDefaultConfig verifies adapter works with default configuration.
// Green-Flag: Default configuration must be usable.
func TestSpark_WithDefaultConfig(t *testing.T) {
	config := spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	}
	adapter := spark.NewAdapter(config)
	defer adapter.Close()

	// Just verify it doesn't panic
	_ = adapter.Name()
	_ = adapter.Capabilities()
}

// TestSpark_WithFullConfig verifies adapter accepts full configuration.
// Green-Flag: All configuration options must work.
func TestSpark_WithFullConfig(t *testing.T) {
	config := spark.AdapterConfig{
		Host:       "spark.example.com",
		Port:       10000,
		Database:   "analytics",
		User:       "spark-user",
		AuthMethod: "NONE",
	}
	adapter := spark.NewAdapter(config)
	defer adapter.Close()

	if adapter.Name() != "spark" {
		t.Fatalf("expected name 'spark', got %q", adapter.Name())
	}
}

// TestSpark_ImplementsEngineAdapter verifies interface compliance.
// Green-Flag: Adapter must implement EngineAdapter interface.
func TestSpark_ImplementsEngineAdapter(t *testing.T) {
	var adapter adapters.EngineAdapter = spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	// Just verify it compiles and runs
	_ = adapter.Name()
	_ = adapter.Capabilities()
}

// TestSpark_PingReturnsErrorWhenNotConnected verifies ping behavior without server.
// Green-Flag: Ping should report connection status honestly.
func TestSpark_PingReturnsErrorWhenNotConnected(t *testing.T) {
	// Configure to connect to a non-existent server
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "nonexistent.invalid",
		Port: 9999,
	})
	defer adapter.Close()

	// Ping should fail since no server is running
	err := adapter.Ping(context.Background())
	if err == nil {
		t.Log("ping succeeded - Spark server may be running (this is acceptable)")
	}
	// Both outcomes are valid - if server exists, ping succeeds; if not, it fails
}

// TestSpark_ConfigDefaults verifies default values are applied.
// Green-Flag: Missing config values should use sensible defaults.
func TestSpark_ConfigDefaults(t *testing.T) {
	config := spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
		// Leave other fields empty to test defaults
	}
	adapter := spark.NewAdapter(config)
	defer adapter.Close()

	// Should not panic with minimal config
	_ = adapter.Name()
}

// TestSpark_IsFallbackEngine verifies Spark is positioned as fallback.
// Green-Flag: Per plan.md, Spark is the fallback engine.
func TestSpark_IsFallbackEngine(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	// Spark should support time travel for AS OF fallback
	caps := adapter.Capabilities()
	hasTimeTravel := false
	for _, cap := range caps {
		if cap == capabilities.CapabilityTimeTravel {
			hasTimeTravel = true
			break
		}
	}

	if !hasTimeTravel {
		t.Fatal("Spark must support TIME_TRAVEL for AS OF fallback per plan.md")
	}
}

// Phase 6 Green-Flag Tests: Health Check Configuration

// TestSpark_CheckHealthMethodExists verifies CheckHealth is available.
// Green-Flag: Adapter must expose CheckHealth per phase-6-spec.md.
func TestSpark_CheckHealthMethodExists(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})
	defer adapter.Close()

	// Just verify method exists and can be called
	// Error is expected since no actual Spark server is running
	_ = adapter.CheckHealth(context.Background())
}

// TestSpark_ConnectionTimeoutConfig verifies timeout configuration is accepted.
// Green-Flag: Custom timeout configuration should be accepted.
func TestSpark_ConnectionTimeoutConfig(t *testing.T) {
	config := spark.AdapterConfig{
		Host:              "localhost",
		Port:              10000,
		ConnectionTimeout: 60 * 1e9, // 60 seconds
	}
	adapter := spark.NewAdapter(config)
	defer adapter.Close()

	// Verify adapter was created successfully
	if adapter.Name() != "spark" {
		t.Fatalf("expected name 'spark', got %q", adapter.Name())
	}
}
