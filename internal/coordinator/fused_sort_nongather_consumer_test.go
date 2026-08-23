//go:build !race

package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// #390, the case #288's ordered gather does not reach. The same fold —
// fuseSortIntoPredecessor moving a Singleton sort's SortKeys+Limit onto a
// Singleton broadcast_join and dropping the sort stage — is applied when
// the ORDER BY + LIMIT sits inside a derived table, and then the
// dispatcher probe-splits the join across workers. Each task emits its own
// sorted top-41 slice and the CONSUMER here is an aggregate, not the
// terminal gather: it reads the task files concatenated, with no
// merge-sort fragment in front of them, so the count came back as the
// whole probe side instead of 41.
//
// A count is the right assertion: it does not depend on the inner order
// (SQL guarantees none through a derived table), only on the LIMIT
// selecting the right number of rows, and there is no top-level LIMIT for
// the coordinator's own post-gather trim to mask the defect with.
func TestDistributedFusedSortLimitNonGatherConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{
			name: "aggregate consumer",
			sql: `SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey, n_comment
				FROM supplier JOIN nation ON s_nationkey = n_nationkey
				ORDER BY n_comment DESC
				LIMIT 41) t`,
			want: 41,
		},
		{
			name: "join consumer",
			sql: `SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey, s_acctbal
				FROM supplier JOIN nation ON s_nationkey = n_nationkey
				ORDER BY s_acctbal DESC
				LIMIT 7) t
				JOIN supplier s2 ON t.s_suppkey = s2.s_suppkey`,
			want: 7,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := coord.ExecuteSQL(ctx, tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if res.Error != "" {
				t.Fatal(res.Error)
			}
			rows := mustRows(t, res)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if got := toInt64(rows[0]["c"]); got != tc.want {
				t.Fatalf("count over a fused sort+LIMIT join = %d, want %d — each probe-split "+
					"task emitted its own top-%d and the consumer concatenated them",
					got, tc.want, tc.want)
			}
		})
	}
}
