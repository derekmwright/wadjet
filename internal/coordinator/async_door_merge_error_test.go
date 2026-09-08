package coordinator

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// ADR-0026 §8a IS TRUE OF EVERY DOOR — the ASYNC one included (#1002, round 2).
//
// The coordinator's merge applies the query's `ORDER BY` over the workers'
// partials, and it REFUSES `0A000` when it cannot: a key that does not resolve
// over the merged columns, or partials that do not describe one relation. The
// point of the refusal is that rows in an order the client did not ask for and
// cannot detect is the worse outcome.
//
// `Coordinator.GetQueryResults` — the whole of the async HTTP door's result
// path (`internal/server.handleGetQueryResults`) — discarded `mergeErr` and
// returned the UNMERGED batches, so that door answered 200 with the rows in
// whatever order the tasks finished in. `ExecuteSQL` has reported the failure
// since the refusal was added; a rule that holds on one door and not the other
// is not a rule.
//
// It rides on `SQLResult.Error`, which is the channel that handler already
// answers 200-with-an-error from, rather than on the returned error, which is
// its query-not-found / not-authorized channel (mapped to 404).
//
// The mismatched-schema refusal is NOT reachable through this door and that is
// stated rather than left to be discovered: `readFinalResults` runs
// `wshf.SchemaGuard` over the partials first, so two that describe different
// relations are refused one layer down, before the merge ever sees them. What
// reaches here is the key that does not resolve — which is #1002's own defect.
func TestTheAsyncDoorReportsAMergeItCouldNotApply(t *testing.T) {
	// One stage, one inline partial of 64 int64 rows in a column called `x`.
	newCoord := func(t *testing.T, mi *logical.MergeInfo) (*Coordinator, []physical.Stage) {
		t.Helper()
		tracker := NewQueryTracker()
		stages := []physical.Stage{{ID: "s1", Type: "pipeline", Tasks: 1}}
		tracker.Register("q1", "SELECT x FROM t ORDER BY x", auth.IdentitySnapshot{},
			map[string]*StageInfo{"s1": {StageID: "s1", TotalTasks: 1}}, []string{"s1"})
		tracker.RecordResult(distributed.ResultNotification{
			QueryID: "q1", StageID: "s1", TaskID: "t1", Success: true,
			NumRows: 64, InlineData: wshfInt64Payload(t, 64, 0),
		})
		tracker.Complete("q1")
		c := &Coordinator{
			tracker:    tracker,
			logger:     slog.Default(),
			config:     Config{GatherResultBudget: 1 << 20},
			queryMetas: map[string]*queryMeta{"q1": {stages: stages, mergeInfo: mi}},
		}
		return c, stages
	}

	for _, tc := range []struct {
		name string
		mi   *logical.MergeInfo
		// want is a substring of the reported failure, or "" for a result.
		want string
	}{
		{
			// #1002's own refusal, through this door: the key names a column
			// the merged result does not carry.
			name: "a key that does not resolve is reported, not discarded",
			mi:   &logical.MergeInfo{OrderBy: []logical.OrderExpr{{Column: "q.nope"}}},
			want: `ORDER BY key "q.nope" does not resolve in the merged columns [x]`,
		},
		{
			// The same through the TOP-K comparator rather than the full sort
			// — `mergeProbePartials` picks it when a small LIMIT is set, so it
			// is a second code path to the same refusal.
			name: "the same through the top-K heap",
			mi: &logical.MergeInfo{
				OrderBy:  []logical.OrderExpr{{Column: "q.nope"}},
				Limit:    2,
				HasLimit: true,
			},
			want: `ORDER BY key "q.nope" does not resolve in the merged columns [x]`,
		},
		{
			// CONTROL: a key that DOES resolve. The merge runs, the door
			// answers rows, and nothing is reported — which is what says the
			// cells above report a refusal rather than any failure at all.
			name: "control: a key that resolves is merged and answered",
			mi:   &logical.MergeInfo{OrderBy: []logical.OrderExpr{{Column: "x", Desc: true}}},
		},
		{
			// CONTROL: no ORDER BY at all. The merge is a concatenation and
			// there is nothing to refuse.
			name: "control: no ordering to apply",
			mi:   &logical.MergeInfo{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newCoord(t, tc.mi)
			res, err := c.GetQueryResults(context.Background(), "q1")
			if err != nil {
				t.Fatalf("GetQueryResults returned an error, which is this door's "+
					"not-found/not-authorized channel and maps to 404: %v", err)
			}
			if tc.want == "" {
				if res.Error != "" {
					t.Fatalf("the door reported %q where the merge succeeds", res.Error)
				}
				if res.TotalRows == 0 {
					t.Errorf("the door answered no rows for a merge that succeeds")
				}
				return
			}
			if res.Error == "" {
				t.Fatalf("the door answered %d rows and reported NOTHING — the merge "+
					"could not apply the query's ordering, so these rows are in an order "+
					"the client did not ask for (ADR-0026 §8a)", res.TotalRows)
			}
			if !strings.Contains(res.Error, tc.want) {
				t.Errorf("reported %q, want it to carry %q", res.Error, tc.want)
			}
			// The CLASS is asserted at the source, not here:
			// `SQLResult.Error` is a string and the SQLSTATE does not survive
			// into it, so `TestTheMergeRefusesAnOrderingItCannotApply` is what
			// holds the 0A000. What this cell owes is that the failure REACHES
			// the door at all.
		})
	}
}
