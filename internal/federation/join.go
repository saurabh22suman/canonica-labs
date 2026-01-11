// Package federation provides cross-engine query federation.
package federation

import (
	"context"
	"fmt"
	"sync"
)

// HashJoinConfig configures a hash join operation.
// Per phase-9-spec.md §3.1.
type HashJoinConfig struct {
	// BuildSide is the smaller input (used to build hash table).
	BuildSide ResultStream

	// ProbeSide is the larger input (streamed through).
	ProbeSide ResultStream

	// BuildKey is the join key column on the build side.
	BuildKey string

	// ProbeKey is the join key column on the probe side.
	ProbeKey string

	// Type is the join type.
	Type JoinType

	// AllowSpill enables spilling to disk for large tables.
	AllowSpill bool

	// SpillThreshold is the row count threshold before spilling.
	SpillThreshold int
}

// HashJoinExecutor executes hash join operations.
type HashJoinExecutor struct {
	config HashJoinConfig
}

// NewHashJoinExecutor creates a new hash join executor.
func NewHashJoinExecutor(config HashJoinConfig) *HashJoinExecutor {
	if config.SpillThreshold == 0 {
		config.SpillThreshold = 100000 // Default 100K rows
	}
	return &HashJoinExecutor{config: config}
}

// Execute performs the hash join and returns a result stream.
// Per phase-9-spec.md §3.1.
func (e *HashJoinExecutor) Execute(ctx context.Context) (ResultStream, error) {
	// Validate inputs
	if e.config.BuildSide == nil {
		return nil, fmt.Errorf("hash join: build side is nil")
	}
	if e.config.ProbeSide == nil {
		return nil, fmt.Errorf("hash join: probe side is nil")
	}

	// Phase 1: Build hash table from build side
	hashTable := make(map[interface{}][]Row)
	buildSchema := e.config.BuildSide.Schema()

	rowCount := 0
	for {
		row, err := e.config.BuildSide.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("hash join build phase failed: %w", err)
		}
		if row == nil {
			break
		}

		key := row[e.config.BuildKey]
		hashTable[key] = append(hashTable[key], row)
		rowCount++

		// Check if we should spill (future enhancement)
		if e.config.AllowSpill && rowCount > e.config.SpillThreshold {
			// TODO: Implement spill to disk for large datasets
			// For now, continue in memory with warning
		}
	}

	// Phase 2: Create probe stream
	return &hashJoinStream{
		hashTable:   hashTable,
		probeSide:   e.config.ProbeSide,
		probeKey:    e.config.ProbeKey,
		joinType:    e.config.Type,
		buildSchema: buildSchema,
		probeSchema: e.config.ProbeSide.Schema(),
	}, nil
}

// hashJoinStream implements ResultStream for hash join results.
type hashJoinStream struct {
	hashTable   map[interface{}][]Row
	probeSide   ResultStream
	probeKey    string
	joinType    JoinType
	buildSchema *ResultSchema
	probeSchema *ResultSchema

	// Current state
	currentProbeRow Row
	matchIdx        int
	matches         []Row

	// For RIGHT/FULL OUTER joins: track matched build rows
	matchedBuildKeys map[interface{}]bool

	mu     sync.Mutex
	closed bool
}

// Schema returns the merged schema.
func (s *hashJoinStream) Schema() *ResultSchema {
	if s.probeSchema == nil || s.buildSchema == nil {
		return nil
	}

	// Merge schemas
	columns := make([]ColumnDef, 0)
	columns = append(columns, s.probeSchema.Columns...)
	columns = append(columns, s.buildSchema.Columns...)

	return &ResultSchema{Columns: columns}
}

// Next returns the next joined row.
func (s *hashJoinStream) Next(ctx context.Context) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil
	}

	// Check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	for {
		// If we have pending matches, emit them
		if s.matchIdx < len(s.matches) {
			result := s.mergeRows(s.currentProbeRow, s.matches[s.matchIdx])
			s.matchIdx++
			return result, nil
		}

		// Get next probe row
		probeRow, err := s.probeSide.Next(ctx)
		if err != nil {
			return nil, err
		}
		if probeRow == nil {
			// No more probe rows
			// For FULL OUTER: emit unmatched build rows
			if s.joinType == JoinTypeFull {
				return s.emitUnmatchedBuildRow()
			}
			return nil, nil
		}

		// Look up in hash table
		key := probeRow[s.probeKey]
		matches := s.hashTable[key]

		if len(matches) == 0 {
			// No matches
			if s.joinType == JoinTypeLeft || s.joinType == JoinTypeFull {
				// LEFT/FULL: emit probe row with nulls for build side
				return s.mergeRowsWithNulls(probeRow, nil, true), nil
			}
			// INNER: skip non-matching rows
			continue
		}

		// Track matched keys for RIGHT/FULL OUTER
		if s.matchedBuildKeys == nil && (s.joinType == JoinTypeRight || s.joinType == JoinTypeFull) {
			s.matchedBuildKeys = make(map[interface{}]bool)
		}
		if s.matchedBuildKeys != nil {
			s.matchedBuildKeys[key] = true
		}

		s.currentProbeRow = probeRow
		s.matches = matches
		s.matchIdx = 0
	}
}

// mergeRows combines probe and build rows.
func (s *hashJoinStream) mergeRows(probe, build Row) Row {
	result := make(Row)
	for k, v := range probe {
		result[k] = v
	}
	for k, v := range build {
		result[k] = v
	}
	return result
}

// mergeRowsWithNulls handles NULL padding for outer joins.
func (s *hashJoinStream) mergeRowsWithNulls(probe, build Row, probeIsLeft bool) Row {
	result := make(Row)

	if probe != nil {
		for k, v := range probe {
			result[k] = v
		}
	} else if s.probeSchema != nil {
		// Add nulls for probe columns
		for _, col := range s.probeSchema.Columns {
			result[col.Name] = nil
		}
	}

	if build != nil {
		for k, v := range build {
			result[k] = v
		}
	} else if s.buildSchema != nil {
		// Add nulls for build columns
		for _, col := range s.buildSchema.Columns {
			result[col.Name] = nil
		}
	}

	return result
}

// emitUnmatchedBuildRow emits unmatched build rows for FULL OUTER join.
func (s *hashJoinStream) emitUnmatchedBuildRow() (Row, error) {
	for key, rows := range s.hashTable {
		if s.matchedBuildKeys != nil && s.matchedBuildKeys[key] {
			continue // Already matched
		}
		if len(rows) > 0 {
			row := rows[0]
			// Remove this row to avoid re-emitting
			s.hashTable[key] = rows[1:]
			if len(s.hashTable[key]) == 0 {
				delete(s.hashTable, key)
			}
			return s.mergeRowsWithNulls(nil, row, false), nil
		}
	}
	return nil, nil
}

// Close releases resources.
func (s *hashJoinStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.hashTable = nil
	s.matches = nil

	if s.probeSide != nil {
		return s.probeSide.Close()
	}
	return nil
}

// EstimatedRows returns -1 (unknown for join results).
func (s *hashJoinStream) EstimatedRows() int64 {
	return -1
}

// JoinStrategySelector selects the optimal join strategy.
// Per phase-9-spec.md §3.2.
type JoinStrategySelector struct {
	memoryLimit int64
}

// NewJoinStrategySelector creates a new join strategy selector.
func NewJoinStrategySelector(memoryLimit int64) *JoinStrategySelector {
	if memoryLimit == 0 {
		memoryLimit = 500 * 1024 * 1024 // Default 500MB
	}
	return &JoinStrategySelector{memoryLimit: memoryLimit}
}

// JoinConfig configures a join operation.
type JoinConfig struct {
	BuildSide   ResultStream
	ProbeSide   ResultStream
	BuildKey    string
	ProbeKey    string
	Type        JoinType
	AllowSpill  bool
	LeftStream  ResultStream // For merge join
	RightStream ResultStream
	LeftKey     string
	RightKey    string
}

// SelectStrategy chooses the optimal join strategy.
func (s *JoinStrategySelector) SelectStrategy(
	leftStream ResultStream,
	rightStream ResultStream,
	join *JoinCondition,
) (JoinStrategy, *JoinConfig) {
	leftRows := leftStream.EstimatedRows()
	rightRows := rightStream.EstimatedRows()

	// Rule 1: If one side is small, use hash join with small side as build
	const smallTableThreshold int64 = 100000

	if leftRows >= 0 && leftRows < smallTableThreshold {
		return JoinStrategyHash, &JoinConfig{
			BuildSide:  leftStream,
			ProbeSide:  rightStream,
			BuildKey:   join.LeftCol,
			ProbeKey:   join.RightCol,
			Type:       join.Type,
			AllowSpill: false,
		}
	}

	if rightRows >= 0 && rightRows < smallTableThreshold {
		return JoinStrategyHash, &JoinConfig{
			BuildSide:  rightStream,
			ProbeSide:  leftStream,
			BuildKey:   join.RightCol,
			ProbeKey:   join.LeftCol,
			Type:       join.Type,
			AllowSpill: false,
		}
	}

	// Rule 2: Default to hash join with spill enabled
	// Pick smaller estimated side as build
	if leftRows < rightRows || rightRows < 0 {
		return JoinStrategyHash, &JoinConfig{
			BuildSide:  leftStream,
			ProbeSide:  rightStream,
			BuildKey:   join.LeftCol,
			ProbeKey:   join.RightCol,
			Type:       join.Type,
			AllowSpill: true,
		}
	}

	return JoinStrategyHash, &JoinConfig{
		BuildSide:  rightStream,
		ProbeSide:  leftStream,
		BuildKey:   join.RightCol,
		ProbeKey:   join.LeftCol,
		Type:       join.Type,
		AllowSpill: true,
	}
}

// ExecuteJoin executes a join based on the selected strategy.
func ExecuteJoin(
	ctx context.Context,
	strategy JoinStrategy,
	config *JoinConfig,
) (ResultStream, error) {
	switch strategy {
	case JoinStrategyHash:
		executor := NewHashJoinExecutor(HashJoinConfig{
			BuildSide:  config.BuildSide,
			ProbeSide:  config.ProbeSide,
			BuildKey:   config.BuildKey,
			ProbeKey:   config.ProbeKey,
			Type:       config.Type,
			AllowSpill: config.AllowSpill,
		})
		return executor.Execute(ctx)

	case JoinStrategyNestedLoop:
		// Nested loop join for cross joins or when no key available
		return executeNestedLoopJoin(ctx, config)

	case JoinStrategyMerge:
		// Merge join for sorted inputs
		return nil, fmt.Errorf("merge join not yet implemented")

	default:
		return nil, fmt.Errorf("unknown join strategy: %s", strategy)
	}
}

// executeNestedLoopJoin performs a nested loop join.
func executeNestedLoopJoin(ctx context.Context, config *JoinConfig) (ResultStream, error) {
	// Collect left side (should be smaller for efficiency)
	leftRows, err := CollectStream(ctx, config.BuildSide)
	if err != nil {
		return nil, fmt.Errorf("nested loop join: %w", err)
	}

	return &nestedLoopJoinStream{
		leftRows:    leftRows,
		rightStream: config.ProbeSide,
		joinType:    config.Type,
		leftSchema:  config.BuildSide.Schema(),
		rightSchema: config.ProbeSide.Schema(),
	}, nil
}

// nestedLoopJoinStream implements cross/nested loop join.
type nestedLoopJoinStream struct {
	leftRows    []Row
	rightStream ResultStream
	joinType    JoinType
	leftSchema  *ResultSchema
	rightSchema *ResultSchema

	currentRightRow Row
	leftIdx         int
	rightExhausted  bool

	mu sync.Mutex
}

// Schema returns the merged schema.
func (s *nestedLoopJoinStream) Schema() *ResultSchema {
	if s.leftSchema == nil || s.rightSchema == nil {
		return nil
	}
	columns := make([]ColumnDef, 0)
	columns = append(columns, s.leftSchema.Columns...)
	columns = append(columns, s.rightSchema.Columns...)
	return &ResultSchema{Columns: columns}
}

// Next returns the next joined row.
func (s *nestedLoopJoinStream) Next(ctx context.Context) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		// If we have a current right row and more left rows to pair
		if s.currentRightRow != nil && s.leftIdx < len(s.leftRows) {
			result := s.mergeRows(s.leftRows[s.leftIdx], s.currentRightRow)
			s.leftIdx++
			return result, nil
		}

		// Get next right row
		if s.rightExhausted {
			return nil, nil
		}

		rightRow, err := s.rightStream.Next(ctx)
		if err != nil {
			return nil, err
		}
		if rightRow == nil {
			s.rightExhausted = true
			return nil, nil
		}

		s.currentRightRow = rightRow
		s.leftIdx = 0
	}
}

func (s *nestedLoopJoinStream) mergeRows(left, right Row) Row {
	result := make(Row)
	for k, v := range left {
		result[k] = v
	}
	for k, v := range right {
		result[k] = v
	}
	return result
}

// Close releases resources.
func (s *nestedLoopJoinStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leftRows = nil
	if s.rightStream != nil {
		return s.rightStream.Close()
	}
	return nil
}

// EstimatedRows returns -1 (unknown).
func (s *nestedLoopJoinStream) EstimatedRows() int64 {
	return -1
}
