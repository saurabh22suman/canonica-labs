// Package greenflag contains Green-Flag tests that prove the system correctly
// executes behavior that is explicitly declared safe.
//
// Per docs/test.md: "Green-Flag tests assert that the system SUCCESSFULLY EXECUTES
// behavior that is explicitly declared safe."
//
// Per docs/phase-4-spec.md §4: "Add Explicit Readiness and Liveness Checks"
package greenflag

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/router"
)

// TestHealthzReturnsOK tests that /healthz always returns 200.
// Per phase-4-spec.md §4: "/healthz reports process health only"
func TestHealthzReturnsOK(t *testing.T) {
	cfg := gateway.Config{Version: "test"}
	gw, err := gateway.NewGatewayWithInMemoryRegistry(cfg)
	if err != nil {
		t.Fatalf("Failed to create gateway: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	gw.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("/healthz should return 200, got %d", w.Code)
	}

	var resp struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "alive" {
		t.Errorf("Expected status 'alive', got '%s'", resp.Status)
	}
}

// TestReadyzReturnsOKWhenAllReady tests that /readyz returns 200 when all components are ready.
// Per phase-4-spec.md §4: "/readyz reports: PostgreSQL connectivity, engine availability, metadata load"
func TestReadyzReturnsOKWhenAllReady(t *testing.T) {
	// Create gateway with all components configured
	tableRegistry := gateway.NewInMemoryTableRegistry()
	authenticator := auth.NewStaticTokenAuthenticator()
	engineRouter := router.DefaultRouter()
	adapterRegistry := adapters.NewAdapterRegistry()

	// Register a mock adapter to make engines available
	adapterRegistry.Register(&mockAdapter{name: "test-engine"})

	cfg := gateway.Config{Version: "test"}
	gw := gateway.NewGateway(authenticator, tableRegistry, engineRouter, adapterRegistry, cfg)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	gw.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("/readyz should return 200 when all components ready, got %d", w.Code)
		t.Logf("Body: %s", w.Body.String())
	}

	var resp struct {
		Status     string `json:"status"`
		Components struct {
			Database struct {
				Ready bool `json:"ready"`
			} `json:"database"`
			Engines struct {
				Ready bool `json:"ready"`
			} `json:"engines"`
			Metadata struct {
				Ready bool `json:"ready"`
			} `json:"metadata"`
		} `json:"components"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "ready" {
		t.Errorf("Expected status 'ready', got '%s'", resp.Status)
	}
}

// TestReadyzReportsAllComponents tests that /readyz reports all required components.
// Per phase-4-spec.md §4: "/readyz reports: PostgreSQL connectivity, engine availability, metadata load"
func TestReadyzReportsAllComponents(t *testing.T) {
	cfg := gateway.Config{Version: "test"}
	gw, err := gateway.NewGatewayWithInMemoryRegistry(cfg)
	if err != nil {
		t.Fatalf("Failed to create gateway: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	gw.ServeHTTP(w, req)

	var resp struct {
		Components struct {
			Database struct {
				Ready   bool   `json:"ready"`
				Message string `json:"message"`
			} `json:"database"`
			Engines struct {
				Ready   bool   `json:"ready"`
				Message string `json:"message"`
			} `json:"engines"`
			Metadata struct {
				Ready   bool   `json:"ready"`
				Message string `json:"message"`
			} `json:"metadata"`
		} `json:"components"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Verify all components are reported
	if resp.Components.Database.Message == "" {
		t.Error("Database component should have a message")
	}
	if resp.Components.Engines.Message == "" {
		t.Error("Engines component should have a message")
	}
	if resp.Components.Metadata.Message == "" {
		t.Error("Metadata component should have a message")
	}
}

// mockAdapter implements adapters.EngineAdapter for testing
type mockAdapter struct {
	name string
}

func (m *mockAdapter) Name() string { return m.name }
func (m *mockAdapter) Capabilities() []capabilities.Capability {
	return []capabilities.Capability{capabilities.CapabilityRead}
}
func (m *mockAdapter) Execute(ctx context.Context, plan *planner.ExecutionPlan) (*adapters.QueryResult, error) {
	return nil, nil
}
func (m *mockAdapter) Ping(ctx context.Context) error { return nil }
func (m *mockAdapter) Close() error                   { return nil }
