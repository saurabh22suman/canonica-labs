// Package main is the entrypoint for the Canonic Gateway server.
// The gateway accepts SQL queries, authenticates requests, resolves virtual tables,
// and routes queries to the appropriate engine.
//
// Per docs/plan.md:
//   - "Accept SQL, Authenticate requests, Parse SQL into logical plan"
//   - "Resolve virtual tables, Forward to planner"
//   - "Explicitly does NOT: execute queries, optimize plans, access storage"
package main

import (
	"context"
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
	"github.com/canonica-labs/canonica/internal/auth"
	"github.com/canonica-labs/canonica/internal/gateway"
	"github.com/canonica-labs/canonica/internal/router"
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
		showHelp  = flag.Bool("help", false, "Show help message")
		showVer   = flag.Bool("version", false, "Show version")
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

	// Create authenticator
	authenticator := auth.NewStaticTokenAuthenticator()
	authenticator.RegisterToken(*token, &auth.User{
		ID:    "default-user",
		Name:  "Default User",
		Roles: []string{"admin"},
	})

	// Create table registry (in-memory for now)
	tableRegistry := gateway.NewInMemoryTableRegistry()

	// Create engine router
	engineRouter := router.DefaultRouter()

	// Create adapter registry
	adapterRegistry := adapters.NewAdapterRegistry()
	adapterRegistry.Register(duckdb.NewAdapter())

	// Create gateway
	gw := gateway.NewGateway(
		authenticator,
		tableRegistry,
		engineRouter,
		adapterRegistry,
		gateway.Config{Version: version},
	)

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

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}

	<-done
	log.Println("Gateway stopped")
	return nil
}
