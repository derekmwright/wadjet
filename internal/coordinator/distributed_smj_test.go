//go:build !race

// Shares setupTPCHDistributed with distributed_tpch_test.go, which is
// excluded under -race for a known upstream nats-server data race (see that
// file's header). Distributed -race coverage for the sort-merge join comes
// from the harness gate run with a race-built wadjet binary.
package coordinator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	tpch "github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestDistributedTPCHSortMergeJoin is the phase-3 in-process gate for the
// distributed sort-merge join (docs/design/sort-merge-join.md §4): with
// broadcast disabled and the SMJ threshold forced to 1, every eligible inner
// equi-join runs as a sort_merge_join stage over the SAME exchange children
// the hash join would use. Each query runs twice on the same coordinator —
// hash-shuffle then SMJ — and must return identical rows. The
// SortMergeJoinsPlanned counter proves the SMJ arm actually took the path.
func TestDistributedTPCHSortMergeJoin(t *testing.T) {
	ctx, coord := setupTPCHDistributed(t)
	// Never broadcast: both arms take the shuffled-join DAG shape, so the
	// comparison isolates the join operator.
	coord.config.BroadcastBytesOverride = -1

	for _, qn := range []int{3, 5, 10, 12, 14} {
		q := tpch.TPCHQueries[qn]
		t.Run(fmt.Sprintf("Q%02d_%s", qn, q.Name), func(t *testing.T) {
			coord.config.SortMergeJoinBytes = 0
			hashResult, err := coord.ExecuteSQL(ctx, q.SQL)
			if err != nil {
				t.Fatalf("hash arm: %v", err)
			}
			if hashResult.Error != "" {
				t.Fatalf("hash arm error: %s", hashResult.Error)
			}
			want := mustRows(t, hashResult)

			coord.config.SortMergeJoinBytes = 1
			before := physical.SortMergeJoinsPlanned.Load()
			smjResult, err := coord.ExecuteSQL(ctx, q.SQL)
			if err != nil {
				t.Fatalf("SMJ arm: %v", err)
			}
			if smjResult.Error != "" {
				t.Fatalf("SMJ arm error: %s", smjResult.Error)
			}
			got := mustRows(t, smjResult)
			if planned := physical.SortMergeJoinsPlanned.Load() - before; planned == 0 {
				t.Fatal("SMJ arm planned zero sort-merge joins — the gate never fired")
			} else {
				t.Logf("Q%02d: %d sort-merge join(s) planned, %d rows", qn, planned, len(got))
			}

			w, g := canonRowsSMJ(want), canonRowsSMJ(got)
			if len(w) != len(g) {
				t.Fatalf("row count: SMJ %d vs hash %d", len(g), len(w))
			}
			for i := range w {
				if w[i] != g[i] {
					t.Fatalf("row %d differs:\n  hash %s\n  smj  %s", i, w[i], g[i])
				}
			}
		})
	}
}

// canonRowsSMJ renders rows order-independently with floats at 6 significant
// digits (accumulation order differs between join algorithms).
func canonRowsSMJ(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	var sb strings.Builder
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.Reset()
		for _, k := range keys {
			switch v := r[k].(type) {
			case float64:
				sb.WriteString(k + "=" + strconv.FormatFloat(v, 'g', 6, 64) + "|")
			default:
				fmt.Fprintf(&sb, "%s=%v|", k, v)
			}
		}
		out = append(out, sb.String())
	}
	sort.Strings(out)
	return out
}
