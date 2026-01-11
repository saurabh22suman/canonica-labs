// Package main is the entrypoint for the Canonic Gateway server.
// The gateway accepts SQL queries, authenticates requests, resolves virtual tables,
// and routes queries to the appropriate engine.
//
// Per docs/plan.md:
//   - "Accept SQL, Authenticate requests, Parse SQL into logical plan"
//   - "Resolve virtual tables, Forward to planner"
//   - "Explicitly does NOT: execute queries, optimize plans, access storage"
//
// Per execution-checklist.md 4.1:
//   - "Gateway startup fails if PostgreSQL is unavailable"
//   - "Repository is mandatory in gateway constructor"
//
// Per execution-checklist.md 4.3:
//   - "Gateway startup fails if adapter registry is empty"
//   - "Trino adapter registered in AdapterRegistry"
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/canonica-labs/canonica/internal/adapters"
	duckdb "github.com/canonica-labs/canonica/internal/adapters/duckdb"
	"github.com/canonica-labs/canonica/internal/adapters/spark"
	"github.com/canonica-labs/canonica/internal/adapters/trino"
	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/router"
	"github.com/canonica-labs/canonica/internal/storage"

	_ "github.com/lib/pq" // PostgreSQL driver
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Parse command line flags
	var (
		addr      = flag.String("addr", ":8080", "HTTP listen address")
		token     = flag.String("token", "", "Static auth token (required)")
		dbURL     = flag.String("db", "", "PostgreSQL connection URL (required in production)")
		trinoHost = flag.String("trino-host", "", "Trino server host (optional)")
		trinoPort = flag.Int("trino-port", 8080, "Trino server port")
		sparkHost = flag.String("spark-host", "", "Spark Thrift Server host (optional)")
		sparkPort = flag.Int("spark-port", 10000, "Spark Thrift Server port")
		showHelp  = flag.Bool("help", false, "Show help message")
		showVer   = flag.Bool("version", false, "Show version")
		devMode   = flag.Bool("dev", false, "Development mode (allows in-memory repository)")
	)
	flag.Parse()

	if *showHelp {
		flag.Usage()
		return nil
	}

	if *showVer {
		fmt.Printf("canonic-gateway %s (commit: %s, built: %s)\n", version, commit, date)
		return nil
	}

	// Validate required flags
	if *token == "" {
		// Check environment variable
		*token = os.Getenv("CANONIC_TOKEN")
		if *token == "" {
			return fmt.Errorf("auth token required: use -token flag or CANONIC_TOKEN env var")
		}
	}

	// Check for database URL from environment
	if *dbURL == "" {
		*dbURL = os.Getenv("CANONIC_DATABASE_URL")
	}

	// Per execution-checklist.md 4.1: Gateway startup fails if PostgreSQL is unavailable
	// Unless in dev mode
	if *dbURL == "" && !*devMode {
		return fmt.Errorf("PostgreSQL connection required: use -db flag or CANONIC_DATABASE_URL env var (use -dev for development mode)")
	}

	// Create authenticator
	authenticator := auth.NewStaticTokenAuthenticator()
	authenticator.RegisterToken(*token, &auth.User{
		ID:    "default-user",
		Name:  "Default User",
		Roles: []string{"admin"},
	})

	// Create repository
	// Per execution-checklist.md 4.1: Repository is mandatory
	var repo storage.TableRepository
	if *dbURL != "" {
		// Connect to PostgreSQL
		db, err := sql.Open("postgres", *dbURL)
		if err != nil {
			return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
		}
		defer db.Close()

		// Verify connectivity
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("PostgreSQL connectivity check failed: %w", err)
		}

		// Per execution-checklist.md 4.4: Migrations run automatically on startup
		log.Println("Running database migrations...")
		migrationRunner := storage.NewMigrationRunner(db)
		if err := migrationRunner.Run(ctx); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		log.Println("Database migrations completed")

		repo = storage.NewPostgresRepository(db)
		log.Println("Connected to PostgreSQL")
	} else {
		// Development mode: use mock repository
		log.Println("WARNING: Development mode - using in-memory repository (not for production)")
		repo = storage.NewMockRepository()
	}

	// Create engine router
	engineRouter := router.DefaultRouter()

	// Create adapter registry
	// Per execution-checklist.md 4.3: At least one adapter required
	adapterRegistry := adapters.NewAdapterRegistry()

	// Register DuckDB adapter (always available as fallback)
	duckdbAdapter := duckdb.NewAdapter()
	adapterRegistry.Register(duckdbAdapter)
	log.Println("Registered DuckDB adapter")

	// Per execution-checklist.md 4.3: Trino adapter registered in AdapterRegistry
	if *trinoHost != "" || os.Getenv("CANONIC_TRINO_HOST") != "" {
		host := *trinoHost
		if host == "" {
			host = os.Getenv("CANONIC_TRINO_HOST")
		}
		trinoAdapter := trino.NewAdapter(trino.AdapterConfig{
			Host: host,
			Port: *trinoPort,
		})
		adapterRegistry.Register(trinoAdapter)
		log.Printf("Registered Trino adapter at %s:%d", host, *trinoPort)
	}

	// Per T003: Spark adapter wiring - register Spark when configured
	if *sparkHost != "" || os.Getenv("CANONIC_SPARK_HOST") != "" {
		host := *sparkHost
		if host == "" {
			host = os.Getenv("CANONIC_SPARK_HOST")
		}
		sparkAdapter := spark.NewAdapter(spark.AdapterConfig{
			Host: host,
			Port: *sparkPort,
		})
		adapterRegistry.Register(sparkAdapter)
		log.Printf("Registered Spark adapter at %s:%d", host, *sparkPort)
	}

	// Create gateway
	// Per execution-checklist.md: NewGateway validates repository and adapter registry
	gw, err := gateway.NewGateway(
		authenticator,
		repo,
		engineRouter,
		adapterRegistry,
		gateway.Config{
			Version:        version,
			ProductionMode: !*devMode,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create gateway: %w", err)
	}

	// Create HTTP server
	server := &http.Server{
		Addr:         *addr,
		Handler:      gw,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Handle graceful shutdown
	done := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("Shutting down gateway...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Shutdown error: %v", err)
		}
		close(done)
	}()

	// Start server
	log.Printf("Canonic Gateway starting on %s", *addr)
	log.Printf("Version: %s, Commit: %s", version, commit)
	log.Printf("Health check: http://localhost%s/health", *addr)
	log.Printf("Readiness: http://localhost%s/readyz", *addr)

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}

	<-done
	log.Println("Gateway stopped")
	return nil
}
