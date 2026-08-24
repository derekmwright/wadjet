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
