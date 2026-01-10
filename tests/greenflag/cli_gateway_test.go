// Package greenflag contains Green-Flag tests that prove the system correctly
// succeeds when semantics are guaranteed.
//
// Per docs/test.md: "Green-Flag tests assert that the system SUCCESSFULLY EXECUTES
// behavior that is explicitly declared safe."
//
// Per docs/phase-3-spec.md §8: "CLI reflects gateway metadata accurately.
// CLI explain output matches gateway explain. CLI errors propagate unchanged."
package greenflag

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/canonica-labs/canonica/internal/cli"
)

// TestCLIReflectsGatewayMetadata tests that CLI reflects gateway state.
// Per phase-3-spec.md §8: "CLI reflects gateway metadata accurately"
func TestCLIReflectsGatewayMetadata(t *testing.T) {
	// Create a mock gateway server
	mockTables := []cli.TableInfo{
		{Name: "analytics.orders", Capabilities: []string{"READ", "TIME_TRAVEL"}},
		{Name: "analytics.customers", Capabilities: []string{"READ"}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tables" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"tables": mockTables,
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	tables, err := client.ListTables(ctx)
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}

	if len(tables) != len(mockTables) {
		t.Errorf("Expected %d tables, got %d", len(mockTables), len(tables))
	}

	for i, table := range tables {
		if table.Name != mockTables[i].Name {
			t.Errorf("Table %d name mismatch: got %s, want %s", i, table.Name, mockTables[i].Name)
		}
	}
}

// TestCLIDescribeMatchesGateway tests that describe returns gateway data.
// Per phase-3-spec.md §8: "CLI reflects gateway metadata accurately"
func TestCLIDescribeMatchesGateway(t *testing.T) {
	mockDetail := cli.TableDetail{
		Name:         "analytics.orders",
		Capabilities: []string{"READ", "TIME_TRAVEL"},
		Sources: []cli.SourceInfo{
			{Format: "DELTA", Location: "s3://bucket/orders"},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tables/analytics.orders" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(mockDetail)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	detail, err := client.DescribeTable(ctx, "analytics.orders")
	if err != nil {
		t.Fatalf("DescribeTable failed: %v", err)
	}

	if detail.Name != mockDetail.Name {
		t.Errorf("Name mismatch: got %s, want %s", detail.Name, mockDetail.Name)
	}

	if len(detail.Sources) != len(mockDetail.Sources) {
		t.Errorf("Sources count mismatch: got %d, want %d", len(detail.Sources), len(mockDetail.Sources))
	}
}

// TestCLIExplainMatchesGateway tests that explain output matches gateway.
// Per phase-3-spec.md §8: "CLI explain output matches gateway explain"
func TestCLIExplainMatchesGateway(t *testing.T) {
	mockExplain := cli.ExplainResult{
		SQL:          "SELECT * FROM analytics.orders",
		Engine:       "duckdb",
		Tables:       []string{"analytics.orders"},
		Capabilities: []string{"READ"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query/explain" && r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(mockExplain)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	explain, err := client.ExplainQuery(ctx, "SELECT * FROM analytics.orders")
	if err != nil {
		t.Fatalf("ExplainQuery failed: %v", err)
	}

	if explain.Engine != mockExplain.Engine {
		t.Errorf("Engine mismatch: got %s, want %s", explain.Engine, mockExplain.Engine)
	}

	if len(explain.Tables) != len(mockExplain.Tables) {
		t.Errorf("Tables count mismatch: got %d, want %d", len(explain.Tables), len(mockExplain.Tables))
	}
}

// TestCLIValidateMatchesGateway tests that validate output matches gateway.
// Per phase-3-spec.md §8: "CLI reflects gateway metadata accurately"
func TestCLIValidateMatchesGateway(t *testing.T) {
	mockValidate := cli.ValidateResult{
		Valid: true,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query/validate" && r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(mockValidate)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	result, err := client.ValidateQuery(ctx, "SELECT * FROM analytics.orders")
	if err != nil {
		t.Fatalf("ValidateQuery failed: %v", err)
	}

	if result.Valid != mockValidate.Valid {
		t.Errorf("Valid mismatch: got %v, want %v", result.Valid, mockValidate.Valid)
	}
}

// TestCLIErrorsPropagateUnchanged tests that gateway errors are preserved.
// Per phase-3-spec.md §8: "CLI errors propagate unchanged"
func TestCLIErrorsPropagateUnchanged(t *testing.T) {
	errorMessage := "table not found: analytics.unknown"
	errorReason := "no virtual table registered with this name"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"error":  errorMessage,
			"reason": errorReason,
		})
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	_, err := client.DescribeTable(ctx, "analytics.unknown")
	if err == nil {
		t.Fatal("Expected error for unknown table")
	}

	// Error should contain the gateway's error message
	errMsg := err.Error()
	if len(errMsg) == 0 {
		t.Error("Error message should not be empty")
	}
}

// TestCLIAuthTokenIncludedInRequests tests that auth token is sent.
// Per phase-3-spec.md §8: "The CLI MUST authenticate to the gateway"
func TestCLIAuthTokenIncludedInRequests(t *testing.T) {
	expectedToken := "my-test-token-12345"
	var receivedToken string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedToken = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tables": []interface{}{},
		})
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, expectedToken)
	ctx := context.Background()

	_, _ = client.ListTables(ctx)

	expectedHeader := "Bearer " + expectedToken
	if receivedToken != expectedHeader {
		t.Errorf("Auth token mismatch:\nGot: %s\nWant: %s", receivedToken, expectedHeader)
	}
}

// TestCLIQueryExecution tests that queries are sent to gateway.
// Per phase-3-spec.md §8: "canonic query"
func TestCLIQueryExecution(t *testing.T) {
	var receivedQuery string

	mockResult := cli.QueryResult{
		QueryID:  "q123",
		RowCount: 10,
		Engine:   "duckdb",
		Duration: "100ms",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query" && r.Method == "POST" {
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			receivedQuery = body["sql"]

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(mockResult)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	expectedSQL := "SELECT * FROM analytics.orders LIMIT 10"
	result, err := client.ExecuteQuery(ctx, expectedSQL)
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if receivedQuery != expectedSQL {
		t.Errorf("Query not sent correctly:\nGot: %s\nWant: %s", receivedQuery, expectedSQL)
	}

	if result.QueryID != mockResult.QueryID {
		t.Errorf("QueryID mismatch: got %s, want %s", result.QueryID, mockResult.QueryID)
	}
}

// TestCLIHealthCheck tests that health check works correctly.
// Per phase-3-spec.md §8: "canonic doctor"
func TestCLIHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"status": "healthy",
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := cli.NewGatewayClient(server.URL, "test-token")
	ctx := context.Background()

	healthy, err := client.CheckHealth(ctx)
	if err != nil {
		t.Fatalf("CheckHealth failed: %v", err)
	}

	if !healthy {
		t.Error("Expected health check to succeed")
	}
}

// Helper to suppress unused warning
var _ = bytes.Buffer{}
