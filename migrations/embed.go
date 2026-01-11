// Package migrations provides embedded migration SQL files.
// Per execution-checklist.md 4.4: Migration runner integrated.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
