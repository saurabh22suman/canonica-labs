// Package redflag contains tests that MUST fail if invariants are violated.
// Per docs/test.md: "Red-Flag tests are mandatory for all new features."
//
// This file tests T030: Audit Log Persistence.
package redflag

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/canonica-labs/canonica/internal/observability"

	_ "modernc.org/sqlite" // Pure Go SQLite driver for testing
)

// TestPersistentLogger_RequiresDatabase verifies that persistent logger
// requires a database connection.
// Per copilot-instructions.md: "If unsure, code must fail"
func TestPersistentLogger_RequiresDatabase(t *testing.T) {
	_, err := observability.NewPersistentLogger(nil)
	if err == nil {
		t.Error("Expected error when creating PersistentLogger with nil database")
	}
}

// TestPersistentLogger_RejectsMissingFields verifies that audit entries
// with missing required fields are rejected.
// Per phase-4-spec.md: "Every request MUST log these fields"
func TestPersistentLogger_RejectsMissingFields(t *testing.T) {
	// Create mock database (SQLite for testing)
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	// Create audit_logs table (simplified for SQLite)
	_, err = db.Exec(`CREATE TABLE audit_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		query_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		role TEXT,
		tables_json TEXT DEFAULT '[]',
		auth_decision TEXT,
		planner_decision TEXT,
		engine TEXT,
		execution_time_ms INTEGER DEFAULT 0,
		outcome TEXT,
		error_message TEXT,
		invariant_violated TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	logger, err := observability.NewPersistentLogger(db)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	ctx := context.Background()

	// Missing QueryID should fail
	err = logger.LogQuery(ctx, observability.QueryLogEntry{
		User:          "test-user",
		ExecutionTime: time.Millisecond,
	})
	if err == nil {
		t.Error("Expected error for missing QueryID")
	}

	// Missing User should fail
	err = logger.LogQuery(ctx, observability.QueryLogEntry{
		QueryID:       "q-123",
		ExecutionTime: time.Millisecond,
	})
	if err == nil {
		t.Error("Expected error for missing User")
	}
}

// TestPersistentLogger_PersistsEntries verifies that audit entries
// are actually written to the database.
// Per T030: "Audit logs must be persisted to PostgreSQL"
func TestPersistentLogger_PersistsEntries(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	// Create audit_logs table
	_, err = db.Exec(`CREATE TABLE audit_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		query_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		role TEXT,
		tables_json TEXT DEFAULT '[]',
		auth_decision TEXT,
		planner_decision TEXT,
		engine TEXT,
		execution_time_ms INTEGER DEFAULT 0,
		outcome TEXT,
		error_message TEXT,
		invariant_violated TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	logger, err := observability.NewPersistentLogger(db)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	ctx := context.Background()

	// Log a valid entry
	err = logger.LogQuery(ctx, observability.QueryLogEntry{
		QueryID:       "q-test-123",
		User:          "test-user",
		Role:          "admin",
		Tables:        []string{"sales.orders"},
		Engine:        "duckdb",
		ExecutionTime: 100 * time.Millisecond,
		Outcome:       "success",
	})
	if err != nil {
		t.Fatalf("Failed to log query: %v", err)
	}

	// Verify entry was persisted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE query_id = 'q-test-123'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query audit_logs: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 audit log entry, got %d", count)
	}
}

// TestPersistentLogger_SurvivesRestart verifies that audit data
// is recoverable after logger recreation.
// Per T030: "Audit logs lost on gateway restart" must be fixed
func TestPersistentLogger_SurvivesRestart(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	// Create audit_logs table
	_, err = db.Exec(`CREATE TABLE audit_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		query_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		role TEXT,
		tables_json TEXT DEFAULT '[]',
		auth_decision TEXT,
		planner_decision TEXT,
		engine TEXT,
		execution_time_ms INTEGER DEFAULT 0,
		outcome TEXT,
		error_message TEXT,
		invariant_violated TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := context.Background()

	// Create first logger and log entry
	logger1, _ := observability.NewPersistentLogger(db)
	_ = logger1.LogQuery(ctx, observability.QueryLogEntry{
		QueryID:       "q-persist-1",
		User:          "user1",
		ExecutionTime: time.Millisecond,
		Outcome:       "success",
	})

	// Create second logger (simulating restart)
	logger2, _ := observability.NewPersistentLogger(db)
	_ = logger2.LogQuery(ctx, observability.QueryLogEntry{
		QueryID:       "q-persist-2",
		User:          "user2",
		ExecutionTime: time.Millisecond,
		Outcome:       "success",
	})

	// Verify both entries are persisted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM audit_logs").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count audit logs: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 audit log entries after restart, got %d", count)
	}
}
