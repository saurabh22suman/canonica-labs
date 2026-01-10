// Package storage provides persistence for the canonica control plane.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/canonica-labs/canonica/internal/capabilities"
	"github.com/canonica-labs/canonica/internal/errors"
	"github.com/canonica-labs/canonica/internal/tables"
)

// PostgresRepository implements TableRepository using PostgreSQL.
// This is the production implementation per docs/plan.md.
type PostgresRepository struct {
	db *sql.DB
}

// PostgresConfig configures the PostgreSQL repository.
type PostgresConfig struct {
	// ConnectionString is the PostgreSQL connection string.
	ConnectionString string

	// MaxOpenConns is the maximum number of open connections.
	MaxOpenConns int

	// MaxIdleConns is the maximum number of idle connections.
	MaxIdleConns int

	// ConnMaxLifetime is the maximum connection lifetime.
	ConnMaxLifetime time.Duration
}

// NewPostgresRepository creates a new PostgreSQL repository.
func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

// Create registers a new virtual table.
func (r *PostgresRepository) Create(ctx context.Context, table *tables.VirtualTable) error {
	// Validate table definition first
	if err := table.Validate(); err != nil {
		return err
	}

	// Start transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if table already exists
	var exists bool
	err = tx.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM virtual_tables WHERE name = $1)",
		table.Name,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if exists {
		return errors.NewTableAlreadyExists(table.Name)
	}

	// Insert virtual table
	var tableID string
	err = tx.QueryRowContext(ctx,
		`INSERT INTO virtual_tables (name, description) 
		 VALUES ($1, $2) 
		 RETURNING id`,
		table.Name, table.Description,
	).Scan(&tableID)
	if err != nil {
		return fmt.Errorf("failed to insert virtual table: %w", err)
	}

	// Insert physical sources
	for _, src := range table.Sources {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO physical_sources (virtual_table_id, format, location, engine)
			 VALUES ($1, $2, $3, $4)`,
			tableID, string(src.Format), src.Location, src.Engine,
		)
		if err != nil {
			return fmt.Errorf("failed to insert physical source: %w", err)
		}
	}

	// Insert capabilities
	for _, cap := range table.Capabilities {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO table_capabilities (virtual_table_id, capability)
			 VALUES ($1, $2)`,
			tableID, string(cap),
		)
		if err != nil {
			return fmt.Errorf("failed to insert capability: %w", err)
		}
	}

	// Insert constraints
	for _, con := range table.Constraints {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO table_constraints (virtual_table_id, constraint_type)
			 VALUES ($1, $2)`,
			tableID, string(con),
		)
		if err != nil {
			return fmt.Errorf("failed to insert constraint: %w", err)
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Get retrieves a virtual table by name.
func (r *PostgresRepository) Get(ctx context.Context, name string) (*tables.VirtualTable, error) {
	if name == "" {
		return nil, errors.NewInvalidTableDefinition("name", "cannot be empty")
	}

	// Get virtual table
	var tableID string
	var description sql.NullString
	var createdAt, updatedAt time.Time

	err := r.db.QueryRowContext(ctx,
		`SELECT id, description, created_at, updated_at 
		 FROM virtual_tables WHERE name = $1`,
		name,
	).Scan(&tableID, &description, &createdAt, &updatedAt)

	if err == sql.ErrNoRows {
		return nil, errors.NewTableNotFound(name)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get virtual table: %w", err)
	}

	table := &tables.VirtualTable{
		Name:        name,
		Description: description.String,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}

	// Get physical sources
	rows, err := r.db.QueryContext(ctx,
		`SELECT format, location, engine 
		 FROM physical_sources WHERE virtual_table_id = $1`,
		tableID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get physical sources: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var format, location string
		var engine sql.NullString
		if err := rows.Scan(&format, &location, &engine); err != nil {
			return nil, fmt.Errorf("failed to scan physical source: %w", err)
		}
		table.Sources = append(table.Sources, tables.PhysicalSource{
			Format:   tables.StorageFormat(format),
			Location: location,
			Engine:   engine.String,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating physical sources: %w", err)
	}

	// Get capabilities
	rows, err = r.db.QueryContext(ctx,
		`SELECT capability FROM table_capabilities WHERE virtual_table_id = $1`,
		tableID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get capabilities: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cap string
		if err := rows.Scan(&cap); err != nil {
			return nil, fmt.Errorf("failed to scan capability: %w", err)
		}
		table.Capabilities = append(table.Capabilities, capabilities.Capability(cap))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating capabilities: %w", err)
	}

	// Get constraints
	rows, err = r.db.QueryContext(ctx,
		`SELECT constraint_type FROM table_constraints WHERE virtual_table_id = $1`,
		tableID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var con string
		if err := rows.Scan(&con); err != nil {
			return nil, fmt.Errorf("failed to scan constraint: %w", err)
		}
		table.Constraints = append(table.Constraints, capabilities.Constraint(con))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating constraints: %w", err)
	}

	return table, nil
}

// Update modifies an existing virtual table.
func (r *PostgresRepository) Update(ctx context.Context, table *tables.VirtualTable) error {
	// Validate table definition first
	if err := table.Validate(); err != nil {
		return err
	}

	// Start transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get table ID and check existence
	var tableID string
	err = tx.QueryRowContext(ctx,
		"SELECT id FROM virtual_tables WHERE name = $1",
		table.Name,
	).Scan(&tableID)
	if err == sql.ErrNoRows {
		return errors.NewTableNotFound(table.Name)
	}
	if err != nil {
		return fmt.Errorf("failed to get table ID: %w", err)
	}

	// Update virtual table
	_, err = tx.ExecContext(ctx,
		`UPDATE virtual_tables SET description = $1, updated_at = NOW() WHERE id = $2`,
		table.Description, tableID,
	)
	if err != nil {
		return fmt.Errorf("failed to update virtual table: %w", err)
	}

	// Delete and re-insert physical sources
	_, err = tx.ExecContext(ctx, "DELETE FROM physical_sources WHERE virtual_table_id = $1", tableID)
	if err != nil {
		return fmt.Errorf("failed to delete physical sources: %w", err)
	}
	for _, src := range table.Sources {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO physical_sources (virtual_table_id, format, location, engine)
			 VALUES ($1, $2, $3, $4)`,
			tableID, string(src.Format), src.Location, src.Engine,
		)
		if err != nil {
			return fmt.Errorf("failed to insert physical source: %w", err)
		}
	}

	// Delete and re-insert capabilities
	_, err = tx.ExecContext(ctx, "DELETE FROM table_capabilities WHERE virtual_table_id = $1", tableID)
	if err != nil {
		return fmt.Errorf("failed to delete capabilities: %w", err)
	}
	for _, cap := range table.Capabilities {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO table_capabilities (virtual_table_id, capability)
			 VALUES ($1, $2)`,
			tableID, string(cap),
		)
		if err != nil {
			return fmt.Errorf("failed to insert capability: %w", err)
		}
	}

	// Delete and re-insert constraints
	_, err = tx.ExecContext(ctx, "DELETE FROM table_constraints WHERE virtual_table_id = $1", tableID)
	if err != nil {
		return fmt.Errorf("failed to delete constraints: %w", err)
	}
	for _, con := range table.Constraints {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO table_constraints (virtual_table_id, constraint_type)
			 VALUES ($1, $2)`,
			tableID, string(con),
		)
		if err != nil {
			return fmt.Errorf("failed to insert constraint: %w", err)
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Delete removes a virtual table by name.
func (r *PostgresRepository) Delete(ctx context.Context, name string) error {
	if name == "" {
		return errors.NewInvalidTableDefinition("name", "cannot be empty")
	}

	result, err := r.db.ExecContext(ctx,
		"DELETE FROM virtual_tables WHERE name = $1",
		name,
	)
	if err != nil {
		return fmt.Errorf("failed to delete virtual table: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return errors.NewTableNotFound(name)
	}

	return nil
}

// List returns all registered virtual tables.
func (r *PostgresRepository) List(ctx context.Context) ([]*tables.VirtualTable, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT name FROM virtual_tables ORDER BY name",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list virtual tables: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table names: %w", err)
	}

	// Get full details for each table
	result := make([]*tables.VirtualTable, 0, len(names))
	for _, name := range names {
		table, err := r.Get(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("failed to get table %s: %w", name, err)
		}
		result = append(result, table)
	}

	return result, nil
}

// Exists checks if a table with the given name exists.
func (r *PostgresRepository) Exists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM virtual_tables WHERE name = $1)",
		name,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}
	return exists, nil
}

// Verify PostgresRepository implements TableRepository interface.
var _ TableRepository = (*PostgresRepository)(nil)
