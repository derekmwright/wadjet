package coordinator

import (
	"context"

	"github.com/derekmwright/wadjet/internal/queryroute"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// NewQueryRouter adapts a Coordinator to the seam pgwire routes SELECT
// through (queryroute.Router). It exists because the wire server no longer
// names this package: pgwire holds the interface, the coordinator supplies
// the implementation, and the direction of that import — distributed engine
// to wire protocol, never the reverse — is what LICENSING.md's boundary
// gate holds.
//
// A nil Coordinator yields a nil Router, so a caller that has none installs
// none and pgwire answers every statement through wadjet.DB.
func NewQueryRouter(c *Coordinator) queryroute.Router {
	if c == nil {
		return nil
	}
	return queryRouter{c: c}
}

type queryRouter struct{ c *Coordinator }

// ExecuteSQL runs the statement on the coordinator and wraps its result.
//
// The wrapper is returned even alongside an error, and even around a nil
// *SQLResult: pgwire closes the result before surfacing the error, which was
// safe when the result was a nil *SQLResult with nil-safe methods and stays
// safe here for the same reason. Returning a nil interface instead would
// turn that Close into a panic.
func (r queryRouter) ExecuteSQL(ctx context.Context, sql string) (queryroute.Result, error) {
	res, err := r.c.ExecuteSQL(ctx, sql)
	return routedResult{res: res}, err
}

// EnforcesABAC reports whether ExecuteSQL applies access policies itself.
func (r queryRouter) EnforcesABAC() bool { return r.c.EnforcesABAC() }

// routedResult presents an *SQLResult through queryroute.Result. Every
// method is nil-safe because every SQLResult method it calls is.
type routedResult struct{ res *SQLResult }

func (r routedResult) ColumnNames() []string {
	if r.res == nil {
		return nil
	}
	return r.res.Columns
}

func (r routedResult) OutputSchema() []parquet.Column { return r.res.OutputSchema() }

func (r routedResult) WireUnconstrainedDecimal() map[string]bool {
	if r.res == nil {
		return nil
	}
	return r.res.WireUnconstrainedDecimal
}

func (r routedResult) StringLength() map[string]int {
	if r.res == nil {
		return nil
	}
	return r.res.StringLength
}

// Stream hands back the coordinator's own BatchStream. No adapter: the two
// interfaces have the same method set, so the value crosses unchanged and
// the consumer still sees the lazy, spill-backed stream when there is one.
func (r routedResult) Stream() queryroute.Stream { return r.res.Stream() }

func (r routedResult) Close() error { return r.res.Close() }
