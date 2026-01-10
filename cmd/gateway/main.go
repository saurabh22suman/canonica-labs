// Package main is the entrypoint for the Canonic Gateway server.
// The gateway accepts SQL queries, authenticates requests, resolves virtual tables,
// and routes queries to the appropriate engine.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// TODO: Implement gateway server
	// See tracker.md for implementation details
	fmt.Println("Canonic Gateway - Not yet implemented")
	return nil
}
