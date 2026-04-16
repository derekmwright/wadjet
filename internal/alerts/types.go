package alerts

import (
	"context"
	"time"
)

// AlertFire is the payload delivered to each sink on a matching evaluation.
type AlertFire struct {
	AlertName   string           `json:"alert"`
	EvaluatedAt time.Time        `json:"evaluated_at"`
	RowCount    int64            `json:"row_count"`          // true count, pre-truncation
	Rows        []map[string]any `json:"rows"`               // capped at MaxRowsPerFire
	Truncated   bool             `json:"truncated"`
	Schema      []ColumnMeta     `json:"schema"`
}

// ColumnMeta describes one result column.
type ColumnMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// MaxRowsPerFire is the hard cap on rows included in AlertFire.Rows.
const MaxRowsPerFire = 1000

// AlertSink delivers an AlertFire to its destination.
// Implementations must be safe to call concurrently and should respect ctx cancellation.
type AlertSink interface {
	Name() string
	Deliver(ctx context.Context, fire AlertFire) error
}

// SQLExecutor is the narrow interface the TableSink and scheduler use to
// run SQL against the engine. Implemented by *coordinator.Coordinator so
// this package doesn't import coordinator (breaks a would-be import cycle).
type SQLExecutor interface {
	// Execute runs a mutation statement (INSERT INTO ...). Returns an error
	// on failure; no result is surfaced.
	Execute(ctx context.Context, sql string) error

	// Query runs a SELECT and returns rows as []map[string]any plus a schema.
	// Implementations should cap the number of rows returned to at most limit.
	// If the underlying result has more rows, truncated is true and total is
	// the true (pre-truncation) row count.
	Query(ctx context.Context, sql string, limit int) (rows []map[string]any, schema []ColumnMeta, total int64, truncated bool, err error)
}
