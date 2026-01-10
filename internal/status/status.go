// Package status provides operational status and audit functionality.
// Per phase-5-spec.md §4: "Provide high-signal visibility without dashboards."
package status

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// StatusResult represents the result of a status check.
type StatusResult struct {
	Ready            bool   `json:"ready"`
	Reason           string `json:"reason,omitempty"`
	GatewayReady     bool   `json:"gateway_ready"`
	RepositoryHealth string `json:"repository_health"`
	EnginesAvailable int    `json:"engines_available"`
	EnginesMessage   string `json:"engines_message"`
	ConfigVersion    string `json:"config_version"`
}

// StatusChecker provides status checking functionality.
type StatusChecker interface {
	GetStatus(ctx context.Context) (*StatusResult, error)
}

// ReadinessResult represents gateway readiness (matches gateway.ReadinessResult).
type ReadinessResult struct {
	Ready      bool
	Components map[string]ComponentStatus
}

// ComponentStatus represents the status of a component.
type ComponentStatus struct {
	Ready   bool
	Message string
}

// FuncStatusChecker implements StatusChecker using functions.
// This allows adapting any gateway implementation.
type FuncStatusChecker struct {
	getReadiness func(ctx context.Context) *ReadinessResult
	getVersion   func() string
}

// NewFuncStatusChecker creates a new functional status checker.
func NewFuncStatusChecker(
	getReadiness func(ctx context.Context) *ReadinessResult,
	getVersion func() string,
) *FuncStatusChecker {
	return &FuncStatusChecker{
		getReadiness: getReadiness,
		getVersion:   getVersion,
	}
}

// GetStatus implements StatusChecker.
func (c *FuncStatusChecker) GetStatus(ctx context.Context) (*StatusResult, error) {
	readiness := c.getReadiness(ctx)
	
	result := &StatusResult{
		Ready:         readiness.Ready,
		GatewayReady:  readiness.Ready,
		ConfigVersion: c.getVersion(),
	}

	// Process components
	if dbStatus, ok := readiness.Components["database"]; ok {
		result.RepositoryHealth = dbStatus.Message
		if !dbStatus.Ready {
			result.Ready = false
			result.Reason = "database not ready: " + dbStatus.Message
		}
	}

	if engineStatus, ok := readiness.Components["engines"]; ok {
		result.EnginesMessage = engineStatus.Message
		if !engineStatus.Ready {
			result.Ready = false
			if result.Reason == "" {
				result.Reason = "engines not ready: " + engineStatus.Message
			}
		}
	}

	return result, nil
}

// MockStatusChecker is a test implementation of StatusChecker.
type MockStatusChecker struct {
	mu            sync.RWMutex
	repoReady     bool
	repoMessage   string
	engineReady   bool
	engineMessage string
	configVersion string
}

// NewMockStatusChecker creates a new mock status checker.
func NewMockStatusChecker() *MockStatusChecker {
	return &MockStatusChecker{
		repoReady:     true,
		repoMessage:   "connected",
		engineReady:   true,
		engineMessage: "available",
		configVersion: "v1.0.0",
	}
}

// SetRepositoryStatus sets the repository status.
func (m *MockStatusChecker) SetRepositoryStatus(ready bool, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.repoReady = ready
	m.repoMessage = message
}

// SetEngineStatus sets the engine status.
func (m *MockStatusChecker) SetEngineStatus(ready bool, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.engineReady = ready
	m.engineMessage = message
}

// SetConfigVersion sets the configuration version.
func (m *MockStatusChecker) SetConfigVersion(version string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configVersion = version
}

// GetStatus implements StatusChecker.
func (m *MockStatusChecker) GetStatus(ctx context.Context) (*StatusResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := &StatusResult{
		Ready:            true,
		GatewayReady:     true,
		RepositoryHealth: m.repoMessage,
		EnginesMessage:   m.engineMessage,
		ConfigVersion:    m.configVersion,
	}

	// Check conditions for readiness
	if !m.repoReady {
		result.Ready = false
		result.Reason = "repository not ready: " + m.repoMessage
	}

	if !m.engineReady {
		result.Ready = false
		if result.Reason == "" {
			result.Reason = "engines not ready: " + m.engineMessage
		}
	}

	if m.configVersion == "" {
		result.Ready = false
		if result.Reason == "" {
			result.Reason = "no configuration loaded"
		}
	}

	return result, nil
}

// QueryAuditEntry represents a logged query for audit.
type QueryAuditEntry struct {
	QueryID  string
	User     string
	SQL      string
	Tables   []string
	Engine   string
	Accepted bool
	Error    string
	Duration time.Duration
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

// String returns a safe string representation without sensitive data.
func (s *AuditSummary) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Query Summary:\n"))
	sb.WriteString(fmt.Sprintf("  Accepted: %d\n", s.AcceptedCount))
	sb.WriteString(fmt.Sprintf("  Rejected: %d\n", s.RejectedCount))
	
	if len(s.TopRejectionReasons) > 0 {
		sb.WriteString("Top Rejection Reasons:\n")
		for _, r := range s.TopRejectionReasons {
			sb.WriteString(fmt.Sprintf("  - %s: %d\n", r.Reason, r.Count))
		}
	}
	
	if len(s.TopQueriedTables) > 0 {
		sb.WriteString("Top Queried Tables:\n")
		for _, t := range s.TopQueriedTables {
			sb.WriteString(fmt.Sprintf("  - %s: %d\n", t.Table, t.Count))
		}
	}
	
	return sb.String()
}

// AuditLogger provides audit logging and summary.
type AuditLogger interface {
	LogQuery(entry QueryAuditEntry)
	GetAuditSummary(ctx context.Context) (*AuditSummary, error)
}

// MockAuditLogger is a test implementation of AuditLogger.
type MockAuditLogger struct {
	mu      sync.RWMutex
	entries []QueryAuditEntry
}

// NewMockAuditLogger creates a new mock audit logger.
func NewMockAuditLogger() *MockAuditLogger {
	return &MockAuditLogger{
		entries: make([]QueryAuditEntry, 0),
	}
}

// LogQuery logs a query audit entry.
func (m *MockAuditLogger) LogQuery(entry QueryAuditEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
}

// GetAuditSummary returns an audit summary.
// Per phase-5-spec.md §4: "No raw data exposure" - only aggregates.
func (m *MockAuditLogger) GetAuditSummary(ctx context.Context) (*AuditSummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary := &AuditSummary{}

	// Count accepted and rejected
	rejectionReasons := make(map[string]int)
	tableCounts := make(map[string]int)

	for _, entry := range m.entries {
		if entry.Accepted {
			summary.AcceptedCount++
		} else {
			summary.RejectedCount++
			if entry.Error != "" {
				rejectionReasons[entry.Error]++
			}
		}

		// Count table usage
		for _, table := range entry.Tables {
			tableCounts[table]++
		}
	}

	// Build top rejection reasons
	for reason, count := range rejectionReasons {
		summary.TopRejectionReasons = append(summary.TopRejectionReasons, RejectionReasonStat{
			Reason: reason,
			Count:  count,
		})
	}
	// Sort by count descending
	sort.Slice(summary.TopRejectionReasons, func(i, j int) bool {
		return summary.TopRejectionReasons[i].Count > summary.TopRejectionReasons[j].Count
	})
	// Keep top 5
	if len(summary.TopRejectionReasons) > 5 {
		summary.TopRejectionReasons = summary.TopRejectionReasons[:5]
	}

	// Build top queried tables
	for table, count := range tableCounts {
		summary.TopQueriedTables = append(summary.TopQueriedTables, TableQueryStat{
			Table: table,
			Count: count,
		})
	}
	// Sort by count descending
	sort.Slice(summary.TopQueriedTables, func(i, j int) bool {
		return summary.TopQueriedTables[i].Count > summary.TopQueriedTables[j].Count
	})
	// Keep top 5
	if len(summary.TopQueriedTables) > 5 {
		summary.TopQueriedTables = summary.TopQueriedTables[:5]
	}

	return summary, nil
}
