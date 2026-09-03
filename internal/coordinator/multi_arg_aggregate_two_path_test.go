package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A MULTI-ARGUMENT AGGREGATE'S PRE-PROJECTION CARRIES EVERY ARGUMENT (#713).
//
// A fragment that has to COMPUTE an aggregate's input runs a projection below
// the aggregate, and a projection NARROWS to its outputs. That projection was
// built from the FIRST argument alone: the second was added only by the branch
// that handles a BARE first argument, which a computed one skips. So
//
//	SELECT MIN_BY(a*2, id) FROM decpair
//
// reached `exec.HashAggregate` with `input has: a * 2` and failed loud on both
// DAG arms — `aggregate input "id" is not a column of its input` — for a query
// the single-process path answers with PostgreSQL's value. Every two-column
// aggregate has the shape: CORR, COVAR_SAMP, COVAR_POP, MIN_BY, MAX_BY.
//
// The BOUNDARY, attempted from both sides: a computed SECOND argument
// (`MIN_BY(a, id*2)`) is materialized by NO engine — the single-process
// pre-aggregate projection does not carry it either — so both paths fail loud
// with the same message and the same class, and the fix deliberately does not
// invent a pass-through for a name nothing produces. That would turn one
// engine's loud failure into a column of NULLs, which is the trade this
// package refuses. Those cells assert the refusal on ALL arms, so a later fix
// has to lift it on all of them together.
type maaCell struct {
	name, sql string
	// want is the answer every arm must give, unordered.
	want []string
	// wantErrLikeAll, when set, is a refusal EVERY arm must give — the
	// boundary shapes no engine materializes.
	wantErrLikeAll string
	pgSays         string
}

func maaCells() []maaCell {
	return []maaCell{
		{name: "min_by_computed_first_argument",
			sql:    `SELECT MIN_BY(a*2, id) AS v FROM decpair`,
			want:   []string{"v=25.50"},
			pgSays: "(ARRAY_AGG(a*2 ORDER BY id))[1] = 25.50"},
		{name: "max_by_computed_first_argument",
			sql:  `SELECT MAX_BY(a+b, id) AS v FROM decpair`,
			want: []string{"v=0.0000"},
			// PostgreSQL's own equivalent answers NULL here — the row with the
			// largest id has a NULL in both columns — where wadjet's MAX_BY
			// skips NULL inputs. That is a MIN_BY/MAX_BY NULL-handling
			// question, identical on all three arms and unchanged by this fix;
			// what this cell asserts is that the DAG answers what the
			// single-process arm answers.
			pgSays: "(ARRAY_AGG(a+b ORDER BY id DESC))[1] = NULL; wadjet skips NULL inputs"},
		{name: "corr_computed_first_argument",
			sql:    `SELECT CORR(a*2, b) AS v FROM decpair`,
			want:   []string{"v=float:0.874701"},
			pgSays: "0.8747011273063944"},
		{name: "covar_samp_computed_first_argument",
			sql:    `SELECT COVAR_SAMP(a*2, b) AS v FROM decpair`,
			want:   []string{"v=float:73.6632"},
			pgSays: "73.6632"},
		{name: "grouped_min_by_computed_first_argument",
			sql: `SELECT s, MIN_BY(a*2, id) AS v FROM decpair GROUP BY s ORDER BY s`,
			want: []string{"s=-1|v=25.50", "s=0|v=NULL", "s=1.5|v=25.50", "s=1.50|v=25.50",
				"s=1.500|v=0.00", "s=10|v=-0.02", "s=9|v=4.00", "s=abc|v=25.50"},
			pgSays: "eight groups, digit for digit"},
		// The BOUNDARY: a computed SECOND argument, loud on every arm.
		{name: "computed_second_argument_is_loud_on_every_arm",
			sql:            `SELECT MIN_BY(a, id*2) AS v FROM decpair`,
			wantErrLikeAll: `aggregate input "id * 2" is not a column of its input`,
			pgSays:         "PostgreSQL answers it; no wadjet engine materializes a computed second argument"},
		{name: "both_arguments_computed_is_loud_on_every_arm",
			sql:            `SELECT CORR(a*2, b*3) AS v FROM decpair`,
			wantErrLikeAll: `aggregate input "b * 3" is not a column of its input`,
			pgSays:         "PostgreSQL answers it; same boundary as above"},
		// Controls: the bare spellings, which were right before, and a
		// one-argument aggregate over a computed input, which is the shape
		// whose projection this fix appends to.
		{name: "ctl_min_by_bare_arguments",
			sql: `SELECT MIN_BY(a, id) AS v FROM decpair`, want: []string{"v=12.75"},
			pgSays: "12.75"},
		{name: "ctl_corr_bare_arguments",
			sql: `SELECT CORR(a, b) AS v FROM decpair`, want: []string{"v=float:0.874701"},
			pgSays: "0.8747011273063944"},
		{name: "ctl_sum_over_a_computed_input",
			sql: `SELECT SUM(a*2) AS v FROM decpair`, want: []string{"v=105.98"},
			pgSays: "105.98"},
		{name: "ctl_string_agg_over_a_computed_input",
			sql:    `SELECT STRING_AGG(s || 'x', ',') AS v FROM decpair`,
			want:   []string{"v=1.50x,1.5x,abcx,10x,9x,1.500x,0x,-1x,1.5x"},
			pgSays: "the same nine, in id order"},
	}
}

func TestAMultiArgumentAggregateCarriesEveryArgument(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range maaCells() {
		t.Run(tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			for _, arm := range []struct {
				name string
				run  func() ([]string, error)
			}{
				{"single", func() ([]string, error) { return na2Run(tmdRunSingle(ctx, single, tc.sql)) }},
				{"dag", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, tc.sql)) }},
				{"dag-shuffled", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, tc.sql)) }},
			} {
				got, err := arm.run()
				if tc.wantErrLikeAll != "" {
					if err == nil {
						t.Errorf("%s arm: ANSWERED %v — a computed SECOND argument is now "+
							"materialized somewhere. If that is deliberate, lift the boundary on "+
							"ALL THREE arms in one change and assert the value; one engine "+
							"answering while another fails loud is the divergence this cell "+
							"exists to hold.\n  SQL: %s", arm.name, got, tc.sql)
					} else if !strings.Contains(err.Error(), tc.wantErrLikeAll) {
						t.Errorf("%s arm: %v\n  want one containing %q\n  SQL: %s",
							arm.name, err, tc.wantErrLikeAll, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s",
						arm.name, err, tc.pgSays, tc.sql)
					continue
				}
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
						arm.name, got, want, tc.pgSays, tc.sql)
				}
			}
		})
	}
}
