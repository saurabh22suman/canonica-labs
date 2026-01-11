// Package federation provides cross-engine query federation.
package federation

import (
	"context"
	"fmt"
	"sync"
)

// ColumnDef defines a column in a result schema.
type ColumnDef struct {
	Name string
	Type string
}

// ResultSchema defines the schema of query results.
type ResultSchema struct {
	Columns []ColumnDef
}

// Row represents a single result row as a map of column names to values.
type Row map[string]interface{}

// ResultStream represents a stream of rows from a query.
// Per phase-9-spec.md §2.1.
type ResultStream interface {
	// Schema returns the result schema.
	Schema() *ResultSchema

	// Next returns the next row, or nil if exhausted.
	Next(ctx context.Context) (Row, error)

	// Close releases resources.
	Close() error

	// EstimatedRows returns estimated row count (-1 if unknown).
	EstimatedRows() int64
}

// ResultStore is an interface for storing intermediate results.
type ResultStore interface {
	// Append adds a row to the store.
	Append(row Row) error

	// Stream returns a stream over the stored rows.
	Stream() ResultStream

	// Size returns the number of stored rows.
	Size() int

	// Close releases resources.
	Close() error
}

// MemoryResultStore stores results in memory.
// Per phase-9-spec.md §2.2.
type MemoryResultStore struct {
	rows   []Row
	schema *ResultSchema
	mu     sync.RWMutex
}

// NewMemoryResultStore creates a new in-memory result store.
func NewMemoryResultStore(schema *ResultSchema) *MemoryResultStore {
	return &MemoryResultStore{
		rows:   make([]Row, 0),
		schema: schema,
	}
}

// Append adds a row to the store.
func (s *MemoryResultStore) Append(row Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, row)
	return nil
}

// Stream returns a stream over the stored rows.
func (s *MemoryResultStore) Stream() ResultStream {
	s.mu.RLock()
	// Copy rows to prevent mutation during iteration
	rows := make([]Row, len(s.rows))
	copy(rows, s.rows)
	s.mu.RUnlock()

	return &memoryStream{
		rows:   rows,
		schema: s.schema,
		idx:    0,
	}
}

// Size returns the number of stored rows.
func (s *MemoryResultStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// Close releases resources.
func (s *MemoryResultStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = nil
	return nil
}

// memoryStream implements ResultStream for in-memory results.
type memoryStream struct {
	rows   []Row
	schema *ResultSchema
	idx    int
	mu     sync.Mutex
}

// Schema returns the result schema.
func (s *memoryStream) Schema() *ResultSchema {
	return s.schema
}

// Next returns the next row.
func (s *memoryStream) Next(ctx context.Context) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if s.idx >= len(s.rows) {
		return nil, nil // Exhausted
	}

	row := s.rows[s.idx]
	s.idx++
	return row, nil
}

// Close releases resources.
func (s *memoryStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = nil
	return nil
}

// EstimatedRows returns the row count.
func (s *memoryStream) EstimatedRows() int64 {
	return int64(len(s.rows))
}

// SliceStream wraps a slice of rows as a ResultStream.
type SliceStream struct {
	rows   []Row
	schema *ResultSchema
	idx    int
	mu     sync.Mutex
}

// NewSliceStream creates a stream from a slice of rows.
func NewSliceStream(rows []Row, schema *ResultSchema) *SliceStream {
	return &SliceStream{
		rows:   rows,
		schema: schema,
		idx:    0,
	}
}

// Schema returns the result schema.
func (s *SliceStream) Schema() *ResultSchema {
	return s.schema
}

// Next returns the next row.
func (s *SliceStream) Next(ctx context.Context) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if s.idx >= len(s.rows) {
		return nil, nil
	}

	row := s.rows[s.idx]
	s.idx++
	return row, nil
}

// Close releases resources.
func (s *SliceStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = nil
	return nil
}

// EstimatedRows returns the row count.
func (s *SliceStream) EstimatedRows() int64 {
	return int64(len(s.rows))
}

// ChannelStream wraps a channel as a ResultStream.
type ChannelStream struct {
	ch     <-chan Row
	errCh  <-chan error
	schema *ResultSchema
	closed bool
	mu     sync.Mutex
}

// NewChannelStream creates a stream from channels.
func NewChannelStream(ch <-chan Row, errCh <-chan error, schema *ResultSchema) *ChannelStream {
	return &ChannelStream{
		ch:     ch,
		errCh:  errCh,
		schema: schema,
	}
}

// Schema returns the result schema.
func (s *ChannelStream) Schema() *ResultSchema {
	return s.schema
}

// Next returns the next row.
func (s *ChannelStream) Next(ctx context.Context) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-s.errCh:
		if err != nil {
			return nil, err
		}
	case row, ok := <-s.ch:
		if !ok {
			s.closed = true
			return nil, nil
		}
		return row, nil
	}

	return nil, nil
}

// Close releases resources.
func (s *ChannelStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// EstimatedRows returns -1 (unknown for channel streams).
func (s *ChannelStream) EstimatedRows() int64 {
	return -1
}

// ConcatStream concatenates multiple streams.
type ConcatStream struct {
	streams []ResultStream
	idx     int
	mu      sync.Mutex
}

// NewConcatStream creates a stream that reads from multiple streams in order.
func NewConcatStream(streams ...ResultStream) *ConcatStream {
	return &ConcatStream{
		streams: streams,
		idx:     0,
	}
}

// Schema returns the schema from the first stream.
func (s *ConcatStream) Schema() *ResultSchema {
	if len(s.streams) == 0 {
		return nil
	}
	return s.streams[0].Schema()
}

// Next returns the next row from the current stream.
func (s *ConcatStream) Next(ctx context.Context) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.idx < len(s.streams) {
		row, err := s.streams[s.idx].Next(ctx)
		if err != nil {
			return nil, err
		}
		if row != nil {
			return row, nil
		}
		// Current stream exhausted, move to next
		s.idx++
	}

	return nil, nil
}

// Close closes all underlying streams.
func (s *ConcatStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for _, stream := range s.streams {
		if err := stream.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// EstimatedRows returns the sum of estimated rows from all streams.
func (s *ConcatStream) EstimatedRows() int64 {
	var total int64
	for _, stream := range s.streams {
		est := stream.EstimatedRows()
		if est < 0 {
			return -1 // Unknown if any stream is unknown
		}
		total += est
	}
	return total
}

// EmptyStream returns an empty result stream.
type EmptyStream struct {
	schema *ResultSchema
}

// NewEmptyStream creates an empty stream with the given schema.
func NewEmptyStream(schema *ResultSchema) *EmptyStream {
	return &EmptyStream{schema: schema}
}

// Schema returns the schema.
func (s *EmptyStream) Schema() *ResultSchema {
	return s.schema
}

// Next always returns nil.
func (s *EmptyStream) Next(ctx context.Context) (Row, error) {
	return nil, nil
}

// Close is a no-op.
func (s *EmptyStream) Close() error {
	return nil
}

// EstimatedRows returns 0.
func (s *EmptyStream) EstimatedRows() int64 {
	return 0
}

// CollectStream collects all rows from a stream into a slice.
func CollectStream(ctx context.Context, stream ResultStream) ([]Row, error) {
	var rows []Row
	for {
		row, err := stream.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("collect stream: %w", err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}
