// Package redflag contains tests that verify the system correctly REJECTS unsafe behavior.
// These tests MUST fail before implementation (Red-Flag TDD).
//
// Per docs/test.md: "If it doesn't fail first, it doesn't prove safety."
package redflag

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/canonica-labs/canonica/internal/gateway"
)

// TestGateway_RejectsUnauthenticatedRequest verifies that requests without
// a valid authentication token are rejected with 401.
//
// Red-Flag: Unauthenticated access must be blocked.
func TestGateway_RejectsUnauthenticatedRequest(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	tests := []struct {
		name     string
		endpoint string
		method   string
	}{
		{"query without token", "/query", http.MethodPost},
		{"tables without token", "/tables", http.MethodGet},
		{"engines without token", "/engines", http.MethodGet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.endpoint, nil)
			// No Authorization header
			rec := httptest.NewRecorder()

			gw.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401 Unauthorized, got %d", rec.Code)
			}

			// Response must include reason
			var resp gateway.ErrorResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if resp.Reason == "" {
				t.Error("error response must include reason")
			}
		})
	}
}

// TestGateway_RejectsInvalidToken verifies that requests with invalid
// authentication tokens are rejected with 401.
//
// Red-Flag: Invalid tokens must be rejected.
func TestGateway_RejectsInvalidToken(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	req.Header.Set("Authorization", "Bearer invalid-token-12345")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", rec.Code)
	}
}

// TestGateway_RejectsEmptyQuery verifies that empty SQL queries are rejected.
//
// Red-Flag: Empty queries must be rejected with clear error.
func TestGateway_RejectsEmptyQuery(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	body := bytes.NewBufferString(`{"sql": ""}`)
	req := httptest.NewRequest(http.MethodPost, "/query", body)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rec.Code)
	}

	var resp gateway.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Reason == "" {
		t.Error("empty query rejection must include reason")
	}
}

// TestGateway_RejectsWriteOperations verifies that write operations are blocked.
// MVP is read-only per docs/plan.md.
//
// Red-Flag: Writes must be blocked with clear error.
func TestGateway_RejectsWriteOperations(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	writeQueries := []string{
		"DELETE FROM sales_orders WHERE id = 1",
		"INSERT INTO sales_orders VALUES (1, 'test')",
		"UPDATE sales_orders SET name = 'test' WHERE id = 1",
	}

	for _, sql := range writeQueries {
		t.Run(sql, func(t *testing.T) {
			body, _ := json.Marshal(gateway.QueryRequest{SQL: sql})
			req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
			req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			gw.ServeHTTP(rec, req)

			// Write operations should return 400 Bad Request
			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 Bad Request for write operation, got %d", rec.Code)
			}

			var resp gateway.ErrorResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if resp.Reason == "" {
				t.Error("write rejection must include reason")
			}
		})
	}
}

// TestGateway_RejectsInvalidJSON verifies that malformed JSON is rejected.
//
// Red-Flag: Invalid request bodies must be rejected with clear error.
func TestGateway_RejectsInvalidJSON(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	body := bytes.NewBufferString(`{not valid json}`)
	req := httptest.NewRequest(http.MethodPost, "/query", body)
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rec.Code)
	}
}

// TestGateway_RejectsUnknownTable verifies that queries on non-existent tables are rejected.
// Per phase-2-spec.md: Authorization is checked BEFORE table existence.
// This prevents information leakage about table existence.
//
// Red-Flag: Unknown tables must be rejected (403 if unauthorized, 404 if authorized but not found).
func TestGateway_RejectsUnknownTable(t *testing.T) {
	gw := gateway.NewTestGateway(t)

	body, _ := json.Marshal(gateway.QueryRequest{SQL: "SELECT * FROM test.nonexistent_table"})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	// Per phase-2-spec.md §4: Authorization is checked before table existence.
	// Since user has no permission on this table, they get 403 (deny-by-default)
	// This is more secure: doesn't reveal if table exists or not.
	if rec.Code != http.StatusForbidden && rec.Code != http.StatusNotFound {
		t.Errorf("expected 403 Forbidden or 404 Not Found, got %d", rec.Code)
	}

	var resp gateway.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Reason == "" {
		t.Error("table rejection must include reason")
	}
}

// TestGateway_RejectsQueryWithoutCapability verifies that queries requiring
// capabilities the table doesn't have are rejected.
//
// Red-Flag: Capability violations must be blocked.
func TestGateway_RejectsQueryWithoutCapability(t *testing.T) {
	gw := gateway.NewTestGatewayWithTable(t, "test.limited_table", []string{"READ"}, nil)

	// TIME_TRAVEL query on table without TIME_TRAVEL capability
	body, _ := json.Marshal(gateway.QueryRequest{SQL: "SELECT * FROM test.limited_table AS OF '2024-01-01'"})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+gateway.TestToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.ServeHTTP(rec, req)

	// Capability violations should return 400
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for capability violation, got %d", rec.Code)
	}

	var resp gateway.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Reason == "" {
		t.Error("capability violation must include reason")
	}
}
