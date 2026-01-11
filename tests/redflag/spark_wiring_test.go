// Package redflag contains tests that MUST fail if invariants are violated.
// Per docs/test.md: "Red-Flag tests are mandatory for all new features."
//
// This file tests T003: Spark Adapter Wiring.
package redflag

import (
	"testing"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/spark"
)

// TestSparkAdapter_Registrable verifies that Spark adapter can be registered
// in the adapter registry.
// Per plan.md: "Fallback → Spark"
func TestSparkAdapter_Registrable(t *testing.T) {
	// Spark adapter must implement EngineAdapter interface
	registry := adapters.NewAdapterRegistry()

	sparkAdapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})

	// Register (no error return)
	registry.Register(sparkAdapter)

	// Should be retrievable by name
	adapter, ok := registry.Get("spark")
	if !ok {
		t.Fatal("Spark adapter not found after registration")
	}
	if adapter.Name() != "spark" {
		t.Errorf("Expected adapter name 'spark', got %q", adapter.Name())
	}
}

// TestSparkAdapter_HasCorrectCapabilities verifies Spark reports correct capabilities.
// Per phase-8-spec.md: Spark supports time-travel for Delta/Iceberg.
func TestSparkAdapter_HasCorrectCapabilities(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})

	caps := adapter.Capabilities()
	if len(caps) == 0 {
		t.Fatal("Spark adapter reports no capabilities")
	}

	// Spark must report TIME_TRAVEL capability
	hasTimeTravel := false
	for _, cap := range caps {
		if cap.String() == "TIME_TRAVEL" {
			hasTimeTravel = true
			break
		}
	}
	if !hasTimeTravel {
		t.Error("Spark adapter does not report TIME_TRAVEL capability")
	}
}

// TestSparkAdapter_RejectsNilPlan verifies Spark rejects nil execution plans.
// Per copilot-instructions.md: "If unsure, code must fail"
func TestSparkAdapter_RejectsNilPlan(t *testing.T) {
	adapter := spark.NewAdapter(spark.AdapterConfig{
		Host: "localhost",
		Port: 10000,
	})

	ctx := t.Context()
	_, err := adapter.Execute(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil execution plan, got nil")
	}
}
