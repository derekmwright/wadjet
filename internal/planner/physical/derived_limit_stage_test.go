package physical

import (
	"testing"
)

// The plan-shape half of #478: which LIMITs get a StageLimit and which do not.
//
// Exactly one thing may bound a given LIMIT, and the three candidates cover
// disjoint cases — see needsLimitStage. Before the stage existed, a LIMIT that
// was neither the plan root (the coordinator's post-gather pass) nor sitting
// on a sort with no OFFSET (the sort's top-N) was bounded by nothing at all,
// and the derived table yielded every row.
//
// These assert the SHAPE rather than an answer, because the shape is the
// invariant: an unbounded producer feeding an aggregate is the defect whatever
// a particular fixture's row count makes of it.
func TestDerivedLimitEmitsAStage(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	cases := []struct {
		name string
		sql  string
		// want is the number of limit stages the plan must carry.
		want int
		// limit/offset are asserted on the single stage when want == 1.
		limit    int
		hasLimit bool
		offset   int
	}{
		{
			name:     "bare LIMIT in a derived table feeding an aggregate",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3) u`,
			want:     1,
			limit:    3,
			hasLimit: true,
		},
		{
			name:     "the same with an OFFSET",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3 OFFSET 5) u`,
			want:     1,
			limit:    3,
			hasLimit: true,
			offset:   5,
		},
		{
			name:     "LIMIT 0 is a bound, not an absence",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 0) u`,
			want:     1,
			limit:    0,
			hasLimit: true,
		},
		{
			name:   "an OFFSET alone still has to skip",
			sql:    `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation OFFSET 5) u`,
			want:   1,
			offset: 5,
		},
		{
			name:     "under a DISTINCT producer, post-#466",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT DISTINCT n_regionkey FROM nation LIMIT 2) u`,
			want:     1,
			limit:    2,
			hasLimit: true,
		},
		{
			name:     "under an explicit GROUP BY producer",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT n_regionkey FROM nation GROUP BY n_regionkey LIMIT 2) u`,
			want:     1,
			limit:    2,
			hasLimit: true,
		},
		{
			name:     "a bounded derived table feeding a JOIN",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3) u JOIN region r ON u.n_nationkey = r.r_regionkey`,
			want:     1,
			limit:    3,
			hasLimit: true,
		},
		{
			// The sort stage's top-N is already the global bound here, so a
			// second stage would be pure cost. This is the shape that
			// happened to work before #478 and must keep working the same
			// way.
			name: "a sorted derived LIMIT with no OFFSET rides the sort stage",
			sql:  `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) u`,
			want: 0,
		},
		{
			// …but the sort truncates to limit+OFFSET and never skips, so
			// the OFFSET still needs somewhere to happen.
			name:     "a sorted derived LIMIT WITH an OFFSET does not",
			sql:      `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5) u`,
			want:     1,
			limit:    3,
			hasLimit: true,
			offset:   5,
		},
		{
			// The coordinator's post-gather pass owns the top-level LIMIT.
			// Emitting a stage as well would apply the OFFSET twice.
			name: "a top-level LIMIT stays the coordinator's",
			sql:  `SELECT n_nationkey FROM nation LIMIT 3`,
			want: 0,
		},
		{
			name: "a top-level LIMIT with an ORDER BY and an OFFSET too",
			sql:  `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5`,
			want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)
			var got []Stage
			for _, s := range stages {
				if s.Type == StageLimit {
					got = append(got, s)
				}
			}
			if len(got) != tc.want {
				t.Fatalf("plan carries %d limit stages, want %d\n  SQL: %s\n%s",
					len(got), tc.want, tc.sql, renderStages(stages))
			}
			if tc.want != 1 {
				return
			}
			s := got[0]
			if s.Limit != tc.limit || s.HasLimit != tc.hasLimit || s.Offset != tc.offset {
				t.Errorf("limit stage carries (Limit=%d HasLimit=%v Offset=%d), want (%d %v %d)",
					s.Limit, s.HasLimit, s.Offset, tc.limit, tc.hasLimit, tc.offset)
			}
			// Singleton is what makes the bound global. A multi-task limit
			// stage keeps n rows PER TASK, and k*n is not n.
			if s.Distribution.Kind != DistSingleton {
				t.Errorf("limit stage distribution = %v, want Singleton", s.Distribution.Kind)
			}
			if s.Tasks != 1 {
				t.Errorf("limit stage Tasks = %d, want 1", s.Tasks)
			}
			if len(s.Dependencies) != 1 {
				t.Errorf("limit stage has %d dependencies, want 1: %v", len(s.Dependencies), s.Dependencies)
			}
			// It has to sit BETWEEN the producer and the consumer, not
			// dangle: something downstream must read it, or the bound is a
			// stage nobody runs.
			consumed := false
			for _, other := range stages {
				for _, d := range other.Dependencies {
					if d == s.ID {
						consumed = true
					}
				}
			}
			if !consumed {
				t.Errorf("limit stage %s is a leaf — nothing downstream reads its bounded output\n%s",
					s.ID, renderStages(stages))
			}
		})
	}
}

// renderStages formats a stage list for a failure message.
func renderStages(stages []Stage) string {
	out := ""
	for _, s := range stages {
		out += "  " + s.ID + " type=" + s.Type + " deps=["
		for i, d := range s.Dependencies {
			if i > 0 {
				out += " "
			}
			out += d
		}
		out += "]\n"
	}
	return out
}

// #525: two LIMITs, one sort, and the ownership rule that decides which of
// them the sort belongs to.
//
// walkStages scans BACKWARDS over the whole stage list for a sort to hand its
// bound to. For a nested LIMIT that scan reaches the INNER one's sort — the
// outer `LIMIT 5` wrote 5 over the inner's 3, and then suppressed its own
// stage because it had just found a sort, so the query answered 5 where
// PostgreSQL answers 3. Both halves were wrong and each hid the other.
//
// Asserted on the SHAPE, because the shape is the invariant. Note which half
// applies where: when the outer LIMIT is the plan ROOT, the coordinator's
// post-gather pass owns it and no stage is emitted for it — so all that must
// hold there is that the INNER bound survived on the sort. When the outer
// LIMIT is itself one level down, it gets a StageLimit above the sort, which
// is the composition the all-bare nesting already produced.
func TestNestedLimitDoesNotStealTheInnerSort(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	cases := []struct {
		name string
		sql  string
		// wantSortLimit is the bound the sort stage must still carry, or -1
		// when the plan must carry no bounded sort at all.
		wantSortLimit int
		// wantLimitStages are the (Limit, Offset) of the StageLimits, in
		// plan order.
		wantLimitStages [][2]int
	}{
		{
			// #525's own repro. The outer LIMIT is inside a derived table,
			// so it needs a stage of its own AND must leave the inner's 3
			// alone. Before the fix: sort.Limit=5 and no stage at all.
			name: "outer LIMIT one level down, over an inner ORDER BY LIMIT",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5) o`,
			wantSortLimit:   3,
			wantLimitStages: [][2]int{{5, 0}},
		},
		{
			name: "…with an OFFSET on the outer one",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5 OFFSET 1) o`,
			wantSortLimit:   3,
			wantLimitStages: [][2]int{{5, 1}},
		},
		{
			// The outer bound is the tighter one here, and it still may not
			// be written onto the inner's sort: the rule must not depend on
			// which number happens to be smaller.
			name: "outer LIMIT tighter than the inner one",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 5) i LIMIT 2) o`,
			wantSortLimit:   5,
			wantLimitStages: [][2]int{{2, 0}},
		},
		{
			// An inner OFFSET with no LIMIT leaves the sort UNBOUNDED but
			// gives the inner its own StageLimit. The outer must compose on
			// top of that stage, not reach under it to the sort.
			name: "inner OFFSET-only under an outer LIMIT",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 20) i LIMIT 3) o`,
			wantSortLimit:   -1,
			wantLimitStages: [][2]int{{0, 20}, {3, 0}},
		},
		{
			// The outer LIMIT at the ROOT: the coordinator's post-gather
			// pass owns it, so no stage — but the inner's bound still has
			// to survive on the sort, and that is the half #525 broke.
			name:            "outer LIMIT at the plan root",
			sql:             `SELECT n_nationkey FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5`,
			wantSortLimit:   3,
			wantLimitStages: nil,
		},
		{
			name:            "…and at the root with an OFFSET",
			sql:             `SELECT n_nationkey FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5 OFFSET 1`,
			wantSortLimit:   3,
			wantLimitStages: nil,
		},
		{
			// The control: ONE sorted LIMIT still rides the sort and emits
			// no stage of its own. The rule narrows who may claim a sort, it
			// does not stop the claim.
			name:            "a single sorted derived LIMIT is unchanged",
			sql:             `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) u`,
			wantSortLimit:   3,
			wantLimitStages: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tc.sql, 3)

			gotSortLimit := -1
			for _, s := range stages {
				if (s.Type == "sort" || s.Type == "merge_sort") && s.HasLimit {
					gotSortLimit = s.Limit
					break
				}
			}
			if gotSortLimit != tc.wantSortLimit {
				t.Errorf("bounded sort stage carries Limit=%d, want %d — an outer LIMIT "+
					"overwrote a bound that is not its own\n  SQL: %s\n%s",
					gotSortLimit, tc.wantSortLimit, tc.sql, renderStages(stages))
			}

			var got [][2]int
			for _, s := range stages {
				if s.Type == StageLimit {
					got = append(got, [2]int{s.Limit, s.Offset})
				}
			}
			if len(got) != len(tc.wantLimitStages) {
				t.Fatalf("plan carries %d limit stages %v, want %d %v\n  SQL: %s\n%s",
					len(got), got, len(tc.wantLimitStages), tc.wantLimitStages, tc.sql, renderStages(stages))
			}
			for i, w := range tc.wantLimitStages {
				if got[i] != w {
					t.Errorf("limit stage %d carries (Limit=%d Offset=%d), want (%d %d)",
						i, got[i][0], got[i][1], w[0], w[1])
				}
			}
		})
	}
}
