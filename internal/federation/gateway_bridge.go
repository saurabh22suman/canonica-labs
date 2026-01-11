// Package federation provides cross-engine query federation.
package federation

import (
	"context"
	"fmt"

	"github.com/canonica-labs/canonica/internal/adapters"
	"github.com/canonica-labs/canonica/internal/planner"
	"github.com/canonica-labs/canonica/internal/sql"
)

// GatewayAdapterBridge wraps a gateway adapter to work with federation.
// This bridges the different adapter interfaces:
//   - Gateway: Execute(ctx, *planner.ExecutionPlan) (*QueryResult, error)
//   - Federation: Execute(ctx, query string) (ResultStream, error)
type GatewayAdapterBridge struct {
	adapter adapters.EngineAdapter
}

// NewGatewayAdapterBridge creates a bridge for a gateway adapter.
func NewGatewayAdapterBridge(adapter adapters.EngineAdapter) *GatewayAdapterBridge {
	return &GatewayAdapterBridge{adapter: adapter}
}

// Name returns the engine name.
func (b *GatewayAdapterBridge) Name() string {
	return b.adapter.Name()
}

// Execute runs a query and returns a result stream.
// Per phase-9-spec.md: Federation uses raw SQL for sub-queries.
func (b *GatewayAdapterBridge) Execute(ctx context.Context, query string) (ResultStream, error) {
	// Create a minimal execution plan from the raw SQL
	parser := sql.NewParser()
	logicalPlan, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("bridge: parse error: %w", err)
	}

	execPlan := &planner.ExecutionPlan{
		LogicalPlan: logicalPlan,
		Engine:      b.adapter.Name(),
	}

	result, err := b.adapter.Execute(ctx, execPlan)
	if err != nil {
		return nil, fmt.Errorf("bridge: execution error: %w", err)
	}

	// Convert QueryResult to ResultStream
	return NewQueryResultStream(result), nil
}

// TableStats returns statistics for a table.
func (b *GatewayAdapterBridge) TableStats(ctx context.Context, table string) (*TableStats, error) {
	// Gateway adapters don't provide stats - return unknown
	return &TableStats{
		RowCount: -1,
	}, nil
}

// HealthCheck returns true if the engine is available.
func (b *GatewayAdapterBridge) HealthCheck(ctx context.Context) bool {
	return b.adapter.CheckHealth(ctx) == nil
}

// QueryResultStream adapts adapters.QueryResult to ResultStream interface.
type QueryResultStream struct {
	result  *adapters.QueryResult
	schema  *ResultSchema
	idx     int
	closed  bool
}

// NewQueryResultStream creates a ResultStream from a QueryResult.
func NewQueryResultStream(result *adapters.QueryResult) *QueryResultStream {
	// Build schema from columns
	columns := make([]ColumnDef, len(result.Columns))
	for i, col := range result.Columns {
		columns[i] = ColumnDef{
			Name: col,
			Type: "unknown", // Gateway doesn't provide type info
		}
	}

	return &QueryResultStream{
		result: result,
		schema: &ResultSchema{Columns: columns},
		idx:    0,
	}
}

// Schema returns column names and types.
func (s *QueryResultStream) Schema() *ResultSchema {
	return s.schema
}

// Next returns the next row, or nil if exhausted.
func (s *QueryResultStream) Next(ctx context.Context) (Row, error) {
	if s.closed || s.idx >= len(s.result.Rows) {
		return nil, nil
	}

	// Convert []interface{} to Row (map)
	rowData := s.result.Rows[s.idx]
	row := make(Row)
	for i, col := range s.result.Columns {
		if i < len(rowData) {
			row[col] = rowData[i]
		}
	}

	s.idx++
	return row, nil
}

// Close releases resources.
func (s *QueryResultStream) Close() error {
	s.closed = true
	return nil
}

// EstimatedRows returns estimated row count.
func (s *QueryResultStream) EstimatedRows() int64 {
	return int64(len(s.result.Rows))
}

// BridgeAdapterRegistry creates a federation AdapterRegistry from gateway adapters.
func BridgeAdapterRegistry(gatewayRegistry *adapters.AdapterRegistry) *AdapterRegistry {
	registry := NewAdapterRegistry()

	for _, name := range gatewayRegistry.Available() {
		adapter, ok := gatewayRegistry.Get(name)
		if ok {
			registry.Register(NewGatewayAdapterBridge(adapter))
		}
	}

	return registry
}
