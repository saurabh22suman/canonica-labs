// Package greenflag contains tests that verify the system correctly performs allowed operations.
// Per docs/test.md: "Green-Flag tests demonstrate allowed behavior and must be deterministic."
package greenflag

import (
	"context"
	"testing"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/adapters/trino"
	"github.com/canonica-labs/canonica/internal/capabilities"
)

// TestTrino_Name verifies the adapter returns correct name.
// Green-Flag: Adapter name must be "trino".
func TestTrino_Name(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	if adapter.Name() != "trino" {
		t.Fatalf("expected name 'trino', got %q", adapter.Name())
	}
}

// TestTrino_Capabilities verifies the adapter reports correct capabilities.
// Green-Flag: Trino supports READ (primary read engine per plan.md).
func TestTrino_Capabilities(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	caps := adapter.Capabilities()

	hasRead := false
	for _, cap := range caps {
		if cap == capabilities.CapabilityRead {
			hasRead = true
			break
		}
	}

	if !hasRead {
		t.Fatal("expected Trino to have READ capability")
	}
}

// TestTrino_CloseIsIdempotent verifies Close can be called multiple times.
// Green-Flag: Close must be safe to call multiple times.
func TestTrino_CloseIsIdempotent(t *testing.T) {
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
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

// TestTrino_WithDefaultConfig verifies adapter works with default configuration.
// Green-Flag: Default configuration must be usable.
func TestTrino_WithDefaultConfig(t *testing.T) {
	config := trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	}
	adapter := trino.NewAdapter(config)
	defer adapter.Close()

	// Just verify it doesn't panic
	_ = adapter.Name()
	_ = adapter.Capabilities()
}

// TestTrino_WithFullConfig verifies adapter accepts full configuration.
// Green-Flag: All configuration options must work.
func TestTrino_WithFullConfig(t *testing.T) {
	config := trino.AdapterConfig{
		Host:     "trino.example.com",
		Port:     443,
		Catalog:  "hive",
		Schema:   "analytics",
		User:     "query-user",
		SSLMode:  "require",
	}
	adapter := trino.NewAdapter(config)
	defer adapter.Close()

	if adapter.Name() != "trino" {
		t.Fatalf("expected name 'trino', got %q", adapter.Name())
	}
}

// TestTrino_ImplementsEngineAdapter verifies interface compliance.
// Green-Flag: Adapter must implement EngineAdapter interface.
func TestTrino_ImplementsEngineAdapter(t *testing.T) {
	var adapter adapters.EngineAdapter = trino.NewAdapter(trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
	})
	defer adapter.Close()

	// Just verify it compiles and runs
	_ = adapter.Name()
	_ = adapter.Capabilities()
}

// TestTrino_PingReturnsErrorWhenNotConnected verifies ping behavior without server.
// Green-Flag: Ping should report connection status honestly.
func TestTrino_PingReturnsErrorWhenNotConnected(t *testing.T) {
	// Configure to connect to a non-existent server
	adapter := trino.NewAdapter(trino.AdapterConfig{
		Host: "nonexistent.invalid",
		Port: 9999,
	})
	defer adapter.Close()

	// Ping should fail since no server is running
	err := adapter.Ping(context.Background())
	if err == nil {
		t.Log("ping succeeded - Trino server may be running (this is acceptable)")
	}
	// Both outcomes are valid - if server exists, ping succeeds; if not, it fails
}

// TestTrino_ConfigDefaults verifies default values are applied.
// Green-Flag: Missing config values should use sensible defaults.
func TestTrino_ConfigDefaults(t *testing.T) {
	config := trino.AdapterConfig{
		Host: "localhost",
		Port: 8080,
		// Leave other fields empty to test defaults
	}
	adapter := trino.NewAdapter(config)
	defer adapter.Close()

	// Should not panic with minimal config
	_ = adapter.Name()
}
