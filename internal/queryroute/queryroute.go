// Package queryroute is the seam pgwire takes when a SELECT is answered by
// something other than the embedded wadjet.DB it was opened over.
//
// The wire server owns a protocol, not an execution strategy. It used to
// hold a *coordinator.Coordinator directly, which made the PostgreSQL wire
// protocol — the part every embedded user needs — reach the distributed
// engine at compile time: an embedded server could not be built without
// linking the coordinator, and the license boundary (LICENSING.md) could not
// be drawn between them at all. The three interfaces here are that seam,
// stated in terms both sides already speak: record batches and declared
// parquet columns.
//
// A Router is optional. With none installed, pgwire answers every statement
// through wadjet.DB — that is the embedded server mode. With one installed,
// SELECT and WITH route through it and everything else still goes to the DB.
package queryroute

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Stream is a lazy, single-consumer iterator over a routed result's batches.
// Next returns (nil, nil) when exhausted. Close releases everything still
// held — buffered batches and, for spill-backed streams, replay scratch on
// disk — and must be called by whoever owns the stream, including on error
// and early-exit paths.
//
// Method-for-method identical to coordinator.BatchStream, deliberately: a
// coordinator stream is assignable to this interface with no adapter, so the
// routed path hands back the same object it always did.
type Stream interface {
	Next(ctx context.Context) (*batch.RecordBatch, error)
	Close() error
}

// Result is one routed statement's result: the column names the wire will
// describe, the declared schema those names resolve to, the two plan
// properties that decide a column's wire type modifier, and the batches.
//
// Every method must be safe to call on a result the router returned
// alongside an error — pgwire closes such a result before surfacing the
// error, exactly as it did when this was a nil *coordinator.SQLResult whose
// methods were nil-safe.
type Result interface {
	// ColumnNames returns the result's output column names, in order.
	ColumnNames() []string
	// OutputSchema returns the declared type of each output column, or nil
	// when the result carries no schema (introspection, error results).
	OutputSchema() []parquet.Column
	// WireUnconstrainedDecimal names the DECIMAL output columns whose wire
	// typmod must say "unconstrained" (-1) regardless of the declared
	// precision and scale (#457/#458).
	WireUnconstrainedDecimal() map[string]bool
	// StringLength names the output columns whose declaration carries a
	// string length — CAST(x AS VARCHAR(4)) — and what it is (#838).
	StringLength() map[string]int
	// Stream returns a consuming iterator over the result batches. The
	// first call detaches them; the caller owns the stream and must drain
	// or Close it.
	Stream() Stream
	// Close releases whatever the result still holds. Idempotent.
	Close() error
}

// Router answers SELECT and WITH statements in place of wadjet.DB.Query.
//
// EnforcesABAC reports whether ExecuteSQL applies the same access policies
// (row filters, column masks, table denial) that DB.Query applies. pgwire
// refuses to route an authenticated connection to a router that does not —
// see pgConn.canBypassDB, which is the security gate on this seam.
type Router interface {
	ExecuteSQL(ctx context.Context, sql string) (Result, error)
	EnforcesABAC() bool
}
