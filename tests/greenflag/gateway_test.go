// Package greenflag contains tests that verify the system correctly ALLOWS safe behavior.
// These tests prove that valid operations succeed.
//
// Per docs/test.md: "Green-Flag tests must pass after implementation."
package greenflag

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/canonica-labs/canonica/internal/gateway"
)

// TestGateway_AcceptsValidToken verifies that requests with valid tokens are authenticated.
//
// Green-Flag: Valid tokens must be accepted.
func TestGateway_AcceptsValidToken(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code == http.StatusUnauthorized {
		t.Error("valid token should be accepted, got 401")
	}
}

// TestGateway_HealthEndpoint verifies the health check endpoint works without auth.
//
// Green-Flag: Health endpoint must be publicly accessible.
func TestGateway_HealthEndpoint(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	// No Authorization header - health should be public
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK for health check, got %d", rec.Code)
	}

	var resp gateway.HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("expected status 'healthy', got '%s'", resp.Status)
	}
}

// TestGateway_ListTables verifies the tables endpoint returns registered tables.
//
// Green-Flag: Tables endpoint must list registered tables.
func TestGateway_ListTables(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.orders", []string{"READ"}, nil)

	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}

	var resp gateway.TablesResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode tables response: %v", err)
	}
	if len(resp.Tables) == 0 {
		t.Error("expected at least one table in response")
	}

	found := false
	for _, table := range resp.Tables {
		if table.Name == "test.orders" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected test.orders table in response")
	}
}

// TestGateway_ListEngines verifies the engines endpoint returns available engines.
//
// Green-Flag: Engines endpoint must list available engines.
func TestGateway_ListEngines(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	req := httptest.NewRequest(http.MethodGet, "/engines", nil)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}

	var resp gateway.EnginesResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode engines response: %v", err)
	}
	if len(resp.Engines) == 0 {
		t.Error("expected at least one engine in response")
	}

	// Check that DuckDB is available (MVP engine)
	found := false
	for _, engine := range resp.Engines {
		if engine.Name == "duckdb" && engine.Available {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected duckdb engine to be available")
	}
}

// TestGateway_ExplainQuery verifies the explain endpoint returns query plan.
//
// Green-Flag: Valid query explain must return plan details.
func TestGateway_ExplainQuery(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.orders", []string{"READ"}, nil)

	body, _ := json.Marshal(gateway.QueryRequest{SQL: "SELECT * FROM test.orders"})
	req := httptest.NewRequest(http.MethodPost, "/query/explain", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp gateway.ExplainResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode explain response: %v", err)
	}
	if resp.Engine == "" {
		t.Error("explain response must include engine")
	}
	if resp.Engine != "duckdb" {
		t.Errorf("expected duckdb engine, got '%s'", resp.Engine)
	}
}

// TestGateway_ValidateQuery verifies the validate endpoint checks query validity.
//
// Green-Flag: Valid query validation must return valid=true.
func TestGateway_ValidateQuery(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.orders", []string{"READ"}, nil)

	body, _ := json.Marshal(gateway.QueryRequest{SQL: "SELECT * FROM test.orders"})
	req := httptest.NewRequest(http.MethodPost, "/query/validate", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp gateway.ValidateResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode validate response: %v", err)
	}
	if !resp.Valid {
		t.Errorf("expected valid=true, got valid=false with error: %s", resp.Error)
	}
}

// TestGateway_ExecuteValidQuery verifies that valid SELECT queries execute successfully.
//
// Green-Flag: Valid SELECT queries must execute.
func TestGateway_ExecuteValidQuery(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.orders", []string{"READ"}, nil)

	body, _ := json.Marshal(gateway.QueryRequest{SQL: "SELECT * FROM test.orders"})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp gateway.QueryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode query response: %v", err)
	}
	if resp.QueryID == "" {
		t.Error("query response must include query_id")
	}
}

// TestGateway_DescribeTable verifies the table describe endpoint.
//
// Green-Flag: Valid table describe must return table details.
func TestGateway_DescribeTable(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.orders", []string{"READ", "TIME_TRAVEL"}, nil)

	req := httptest.NewRequest(http.MethodGet, "/tables/test.orders", nil)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp gateway.TableDescribeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode table describe response: %v", err)
	}
	if resp.Name != "test.orders" {
		t.Errorf("expected table name 'orders', got '%s'", resp.Name)
	}
	if len(resp.Capabilities) != 2 {
		t.Errorf("expected 2 capabilities, got %d", len(resp.Capabilities))
	}
}

// TestGateway_DescribeEngine verifies the engine describe endpoint.
//
// Green-Flag: Valid engine describe must return engine details.
func TestGateway_DescribeEngine(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	req := httptest.NewRequest(http.MethodGet, "/engines/duckdb", nil)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp gateway.EngineDescribeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode engine describe response: %v", err)
	}
	if resp.Name != "duckdb" {
		t.Errorf("expected engine name 'duckdb', got '%s'", resp.Name)
	}
	if !resp.Available {
		t.Error("expected duckdb to be available")
	}
}

// TestGateway_ResponseIncludesQueryID verifies observability requirements.
// Per docs/plan.md: "Every query must emit: query_id"
//
// Green-Flag: All query responses must include query_id.
func TestGateway_ResponseIncludesQueryID(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.orders", []string{"READ"}, nil)

	body, _ := json.Marshal(gateway.QueryRequest{SQL: "SELECT * FROM test.orders"})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	var resp gateway.QueryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode query response: %v", err)
	}
	if resp.QueryID == "" {
		t.Error("query response must include query_id for observability")
	}
}

// TestGateway_JSONContentType verifies responses have correct content type.
//
// Green-Flag: All responses must have application/json content type.
func TestGateway_JSONContentType(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	endpoints := []struct {
		method   string
		path     string
		needAuth bool
	}{
		{http.MethodGet, "/health", false},
		{http.MethodGet, "/tables", true},
		{http.MethodGet, "/engines", true},
	}

	for _, ep := range endpoints {
		t.Run(ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			if ep.needAuth {
				req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
			}
			rec := httptest.NewRecorder()

			gw.ServeHTTP(rec, req)

			contentType := rec.Header().Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("expected Content-Type application/json, got '%s'", contentType)
			}
		})
	}
}
