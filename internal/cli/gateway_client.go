// Package cli provides the command-line interface for canonica.
//
// Per phase-3-spec.md §8: "The CLI must reflect REAL system behavior, not local simulation.
// The CLI becomes a CLIENT, not an emulator."
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/canonica-labs/canonica/internal/errors"
)

// GatewayClient is the HTTP client for communicating with the canonica gateway.
// Per phase-3-spec.md §8: "The CLI MUST authenticate to the gateway,
// issue requests via HTTP / API, display real responses."
type GatewayClient struct {
	endpoint   string
	token      string
	httpClient *http.Client
}

// NewGatewayClient creates a new gateway client.
// Per phase-3-spec.md §8: "The CLI becomes a CLIENT, not an emulator."
func NewGatewayClient(endpoint, token string) *GatewayClient {
	return &GatewayClient{
		endpoint: endpoint,
		token:    token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Endpoint returns the configured gateway endpoint.
func (c *GatewayClient) Endpoint() string {
	return c.endpoint
}

// Token returns the configured authentication token.
func (c *GatewayClient) Token() string {
	return c.token
}

// TableInfo represents a table in the list response.
type TableInfo struct {
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities"`
	Constraints  []string `json:"constraints,omitempty"`
}

// TableDetail represents detailed table information.
type TableDetail struct {
	Name         string          `json:"name"`
	Capabilities []string        `json:"capabilities"`
	Constraints  []string        `json:"constraints,omitempty"`
	Sources      []SourceInfo    `json:"sources"`
}

// SourceInfo represents a physical source.
type SourceInfo struct {
	Format   string `json:"format"`
	Location string `json:"location"`
}

// ExplainResult represents query explanation from the gateway.
type ExplainResult struct {
	SQL          string   `json:"sql"`
	Engine       string   `json:"engine"`
	Tables       []string `json:"tables"`
	Capabilities []string `json:"capabilities"`
	Plan         string   `json:"plan,omitempty"`
}

// ValidateResult represents query validation from the gateway.
type ValidateResult struct {
	Valid bool   `json:"valid"`
	Error string `json:"error,omitempty"`
}

// QueryResult represents a query execution result.
type QueryResult struct {
	QueryID  string                   `json:"query_id"`
	Columns  []string                 `json:"columns,omitempty"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	RowCount int                      `json:"row_count"`
	Engine   string                   `json:"engine"`
	Duration string                   `json:"duration"`
}

// ListTables retrieves all registered tables from the gateway.
// Per phase-3-spec.md §8: "canonic table list"
func (c *GatewayClient) ListTables(ctx context.Context) ([]TableInfo, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	resp, err := c.doRequest(ctx, "GET", "/tables", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var result struct {
		Tables []TableInfo `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Tables, nil
}

// DescribeTable retrieves detailed information about a table.
// Per phase-3-spec.md §8: "canonic table describe"
func (c *GatewayClient) DescribeTable(ctx context.Context, tableName string) (*TableDetail, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	resp, err := c.doRequest(ctx, "GET", "/tables/"+tableName, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var result TableDetail
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// ExplainQuery gets the execution plan for a query from the gateway.
// Per phase-3-spec.md §8: "canonic query explain"
func (c *GatewayClient) ExplainQuery(ctx context.Context, sql string) (*ExplainResult, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := c.doRequest(ctx, "POST", "/query/explain", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var result ExplainResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// ValidateQuery validates a query without executing it.
// Per phase-3-spec.md §8: "canonic query validate"
func (c *GatewayClient) ValidateQuery(ctx context.Context, sql string) (*ValidateResult, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := c.doRequest(ctx, "POST", "/query/validate", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var result ValidateResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// ExecuteQuery executes a query and returns the result.
// Per phase-3-spec.md §8: "canonic query"
func (c *GatewayClient) ExecuteQuery(ctx context.Context, sql string) (*QueryResult, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := c.doRequest(ctx, "POST", "/query", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// CheckHealth verifies gateway connectivity.
// Per phase-3-spec.md §8: "canonic doctor"
func (c *GatewayClient) CheckHealth(ctx context.Context) (bool, error) {
	if c.endpoint == "" {
		return false, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	resp, err := c.doRequest(ctx, "GET", "/health", nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

// doRequest performs an HTTP request to the gateway.
func (c *GatewayClient) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	url := c.endpoint + path
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.NewGatewayUnavailable(c.endpoint, err.Error())
	}

	return resp, nil
}

// parseErrorResponse parses an error response from the gateway.
func (c *GatewayClient) parseErrorResponse(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)

	var errResp struct {
		Error      string `json:"error"`
		Reason     string `json:"reason"`
		Suggestion string `json:"suggestion"`
	}
	if err := json.Unmarshal(body, &errResp); err != nil {
		return fmt.Errorf("gateway error: %d - %s", resp.StatusCode, string(body))
	}

	if errResp.Reason != "" {
		return fmt.Errorf("%s: %s", errResp.Error, errResp.Reason)
	}
	return fmt.Errorf("%s", errResp.Error)
}

// AuditSummary represents aggregated audit statistics.
// Per phase-5-spec.md §4: "No raw data exposure"
type AuditSummary struct {
	AcceptedCount       int                   `json:"accepted_count"`
	RejectedCount       int                   `json:"rejected_count"`
	TopRejectionReasons []RejectionReasonStat `json:"top_rejection_reasons"`
	TopQueriedTables    []TableQueryStat      `json:"top_queried_tables"`
}

// RejectionReasonStat represents rejection reason statistics.
type RejectionReasonStat struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

// TableQueryStat represents table query statistics.
type TableQueryStat struct {
	Table string `json:"table"`
	Count int    `json:"count"`
}

// GetAuditSummary retrieves audit summary from the gateway.
// Per phase-5-spec.md §4: "canonic audit summary"
func (c *GatewayClient) GetAuditSummary(ctx context.Context) (*AuditSummary, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	resp, err := c.doRequest(ctx, "GET", "/audit/summary", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var result AuditSummary
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetStatus retrieves system status from the gateway.
// Per phase-5-spec.md §4: "canonic status"
func (c *GatewayClient) GetStatus(ctx context.Context) (*StatusResult, error) {
	if c.endpoint == "" {
		return nil, errors.NewGatewayUnavailable("", "no gateway endpoint configured")
	}

	resp, err := c.doRequest(ctx, "GET", "/readyz", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Status     string `json:"status"`
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
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &StatusResult{
		Ready:            result.Status == "ready",
		GatewayReady:     resp.StatusCode == http.StatusOK,
		RepositoryHealth: result.Components.Database.Message,
		EnginesMessage:   result.Components.Engines.Message,
	}, nil
}

// StatusResult represents system status.
type StatusResult struct {
	Ready            bool   `json:"ready"`
	GatewayReady     bool   `json:"gateway_ready"`
	RepositoryHealth string `json:"repository_health"`
	EnginesMessage   string `json:"engines_message"`
	ConfigVersion    string `json:"config_version"`
}
