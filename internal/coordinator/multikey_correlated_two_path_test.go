package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/multikey"
)

// The distributed half of #562 — a correlated subquery keyed on MORE THAN ONE
// column.
//
// `EXISTS (SELECT 1 FROM b WHERE b.s = a.s AND b.n = a.n)` answered ZERO rows
// and its NOT EXISTS twin answered EVERY row, because dedupSemiAntiBuildSide
// read the join keys out of the condition TEXT with a split on " and " while a
// decorrelation renders " AND " — so the build side was projected down to the
// FIRST conjunct's key and the column the second conjunct compares was gone.
//
// This gate has TWO assertions, and it needs both.
//
// The two-arm compare is the weaker one, and on its own it would have proved
// nothing here: the defect is in the LOGICAL optimizer, which both arms share,
// so the stage DAG and the single-process pipeline answered 0 together. That
// is why every expectation below is PostgreSQL 17's answer over the same
// fixture (internal/oracle/multikey, whose own test re-checks those constants
// against a live server) rather than one arm's answer recorded as the other's
// target.
//
// The compare still earns its place: it is the half that catches a divergence
// the shuffle introduces — a multi-column key crosses an exchange as a
// composite, and #474/#459 are what happens when the router and the key
// disagree about one.
func TestMultiKeyCorrelatedTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range multikey.Corpus() {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			single64, singleErr := mkctpCount(func() ([]map[string]any, error) {
				res, err := tmdRunSingle(ctx, single, c.SQL)
				if err != nil {
					return nil, err
				}
				return res.Rows, nil
			})
			dag64, dagErr := mkctpCount(func() ([]map[string]any, error) {
				res, err := tmdRunDAG(ctx, coord, c.SQL)
				if err != nil {
					return nil, err
				}
				return res.Rows, nil
			})

			if c.KnownBug != "" {
				// The pin keeps Want exactly as PostgreSQL wrote it and only
				// stops the failure being reported as this corpus's. An entry
				// that starts agreeing on BOTH arms is the fix landing, and
				// the pin has to go with it.
				if singleErr == nil && dagErr == nil && single64 == c.Want && dag64 == c.Want {
					t.Errorf("both arms now AGREE with PostgreSQL (%d), so %s is FIXED:\n  %s\n"+
						"Delete its pin in internal/oracle/multikey.", c.Want, c.Issue, c.KnownBug)
					return
				}
				t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  single=%d (%v) dag=%d (%v) want %d",
					c.Issue, c.KnownBug, single64, singleErr, dag64, dagErr, c.Want)
				return
			}

			if singleErr != nil {
				t.Fatalf("the single-process engine refused this query: %v\n  SQL: %s", singleErr, c.SQL)
			}
			if dagErr != nil {
				t.Fatalf("the stage DAG refused a query the single-process engine answered (%d): %v\n  SQL: %s",
					single64, dagErr, c.SQL)
			}
			if single64 != c.Want {
				t.Errorf("single-process: got %d, want %d (live PostgreSQL 17, %d correlated key(s))\n  SQL: %s",
					single64, c.Want, c.Keys, c.SQL)
			}
			if dag64 != c.Want {
				t.Errorf("stage DAG: got %d, want %d (live PostgreSQL 17, %d correlated key(s))\n  SQL: %s",
					dag64, c.Want, c.Keys, c.SQL)
			}
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d\n  SQL: %s",
					single64, dag64, c.SQL)
			}
		})
	}
}

// mkctpCount runs one arm and reduces its single-cell answer to an int64.
func mkctpCount(run func() ([]map[string]any, error)) (int64, error) {
	rows, err := run()
	if err != nil {
		return -1, err
	}
	if len(rows) != 1 {
		return -1, fmt.Errorf("expected exactly one row, got %d", len(rows))
	}
	switch v := rows[0]["n"].(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	}
	return -1, fmt.Errorf("count came back as %#v (%T)", rows[0]["n"], rows[0]["n"])
}
