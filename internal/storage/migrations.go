// Package storage provides database access and migrations.
// Per execution-checklist.md 4.4: Migration runner integrated.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	cerrors "github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/migrations"
)

// MigrationRunner handles database schema migrations.
// Per execution-checklist.md 4.4: Migrations run automatically on startup.
type MigrationRunner struct {
	db *sql.DB
}

// NewMigrationRunner creates a new migration runner.
func NewMigrationRunner(db *sql.DB) *MigrationRunner {
	return &MigrationRunner{db: db}
}

// Run executes all pending migrations.
// Per execution-checklist.md 4.4: Gateway fails startup on migration failure.
func (r *MigrationRunner) Run(ctx context.Context) error {
	// Create migrations tracking table if it doesn't exist
	if err := r.ensureMigrationsTable(ctx); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get list of applied migrations
	applied, err := r.getAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	// Get list of migration files
	migrations, err := r.getMigrationFiles()
	if err != nil {
		return fmt.Errorf("failed to read migration files: %w", err)
	}

	// Apply pending migrations
	for _, m := range migrations {
		if _, ok := applied[m.version]; ok {
			continue // Already applied
		}

		if err := r.applyMigration(ctx, m); err != nil {
			return cerrors.NewMigrationFailed(m.name, err)
		}
	}

	return nil
}

type migration struct {
	version  string
	name     string
	filename string
	content  []byte
}

func (r *MigrationRunner) ensureMigrationsTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`
	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *MigrationRunner) getAppliedMigrations(ctx context.Context) (map[string]bool, error) {
	query := `SELECT version FROM schema_migrations`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

func (r *MigrationRunner) getMigrationFiles() ([]migration, error) {
	var migrationList []migration

	// Read from embedded filesystem in migrations package
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		// No migrations found - this is OK for tests
		return migrationList, nil
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		// Only process .up.sql files
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		// Parse version from filename (e.g., "000001_create_virtual_tables.up.sql")
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			continue
		}
		version := parts[0]
		baseName := strings.TrimSuffix(name, ".up.sql")

		content, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration %s: %w", name, err)
		}

		migrationList = append(migrationList, migration{
			version:  version,
			name:     baseName,
			filename: name,
			content:  content,
		})
	}

	// Sort by version
	sort.Slice(migrationList, func(i, j int) bool {
		return migrationList[i].version < migrationList[j].version
	})

	return migrationList, nil
}

func (r *MigrationRunner) applyMigration(ctx context.Context, m migration) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Execute migration
	if _, err := tx.ExecContext(ctx, string(m.content)); err != nil {
		return fmt.Errorf("failed to execute migration: %w", err)
	}

	// Record migration
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)`,
		m.version, time.Now(),
	); err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration: %w", err)
	}

	return nil
}
