// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/queryroute"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A router that returns (nil, err) must be an error, never a panic.
//
// queryroute.Result's doc asks every method to be safe on a result returned
// alongside an error, and the coordinator honours it by always handing back a
// non-nil wrapper. Nothing asserted the OTHER side of that contract: a second
// implementation returning a plain nil interface would have panicked inside
// queryViaRouter's unconditional res.Close() — on the wire, as a dropped
// connection rather than an error message (LS review P3).
type nilResultRouter struct{ err error }

func (r nilResultRouter) ExecuteSQL(context.Context, string) (queryroute.Result, error) {
	return nil, r.err
}
func (r nilResultRouter) EnforcesABAC() bool { return true }

// bareResultRouter returns a result AND no error, with nothing in it: the
// other end of the same contract.
type bareResultRouter struct{}

func (bareResultRouter) ExecuteSQL(context.Context, string) (queryroute.Result, error) {
	return nil, nil
}
func (bareResultRouter) EnforcesABAC() bool { return true }

func TestARouterThatReturnsNoResultIsAnErrorNotAPanic(t *testing.T) {
	cases := []struct {
		name   string
		router queryroute.Router
		want   string
	}{
		{"nil result with an error", nilResultRouter{err: errors.New("router said no")}, "router said no"},
		{"nil result with no error", bareResultRouter{}, "no result"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &pgConn{router: tc.router}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("queryViaRouter panicked on a nil result: %v", r)
				}
			}()
			res, stream, nested, err := c.queryViaRouter(context.Background(), "SELECT 1")
			if err == nil {
				t.Fatal("a router that returned no result was not reported as an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
			if res != nil || stream != nil || nested != nil {
				t.Errorf("a failed route returned a result (%v), a stream (%v) or a schema (%v)", res, stream, nested)
			}
		})
	}
}

// And the contract the coordinator does honour, asserted rather than assumed:
// a non-nil wrapper whose methods are safe.
func TestAClosableResultIsClosedOnTheErrorPath(t *testing.T) {
	closed := false
	c := &pgConn{router: closingRouter{onClose: func() { closed = true }}}
	if _, _, _, err := c.queryViaRouter(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("expected the router's error")
	}
	if !closed {
		t.Error("the routed result was not closed on the error path — its spill scratch would outlive the statement")
	}
}

type closingRouter struct{ onClose func() }

func (r closingRouter) ExecuteSQL(context.Context, string) (queryroute.Result, error) {
	return closingResult{onClose: r.onClose}, errors.New("failed after producing a result")
}
func (r closingRouter) EnforcesABAC() bool { return true }

type closingResult struct{ onClose func() }

func (r closingResult) ColumnNames() []string                     { return nil }
func (r closingResult) OutputSchema() []parquet.Column            { return nil }
func (r closingResult) WireUnconstrainedDecimal() map[string]bool { return nil }
func (r closingResult) StringLength() map[string]int              { return nil }
func (r closingResult) Stream() queryroute.Stream                 { return queryroute.NewSliceStream(nil) }
func (r closingResult) Close() error                              { r.onClose(); return nil }
