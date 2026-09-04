package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// A WINDOW in a set-operation ARM is that arm's OWN output (#733, #746).
//
// `SelectInfo.Windows` — the flag the logical builder gates window planning on
// — was collected from the OUTERMOST SelectInfo's columns only, and a set
// operation's outermost SelectInfo has no columns: they live on the arms. So
// an arm whose SELECT list is a BARE window got no Window node at all, and its
// projection was left reading the alias off the arm's INPUT:
//
//   - `SUM(a) OVER () AS s` over decpair, which STORES a TEXT column `s`,
//     answered decpair.s on the single-process path and made the two arms look
//     like STRING vs DECIMAL on the stage DAG, which refused the query (#733);
//   - the same query with a name the input does NOT carry (`AS s2`, or an
//     unaliased window whose output name is `sum`) failed on every arm with
//     `column "s2" does not exist in the input schema` — so the "non-colliding
//     control" #733 describes as correct was not correct either;
//   - `SUM(id) OVER () AS w` over a table storing a reserved-family column
//     failed the same way on all three arms (#746);
//   - two window arms UNIONed and then joined failed the same way (#733's
//     third face, its 2026-08-30 comment).
//
// A window NESTED in a larger expression (`SUM(a) OVER () + 1`) was never
// affected: the builder extracts those from the column's own AST rather than
// from the list. That is why every arithmetic-shaped window in the corpus
// passed while the bare ones did not, and it is why this gate carries the bare
// spellings.
//
// The values are compared NUMERICALLY (canonical big.Rat) rather than as
// rendered text: a set operation's DECIMAL rung resolves to max(scale) over
// the arms, so wadjet renders the window's 52.99 as 52.9900 under a
// DECIMAL(18,4) sibling arm where PostgreSQL's unconstrained numeric renders
// 52.99. That is a declared scale, not a value.
type setOpWinCell struct {
	issue, name, sql string
	// want is the row multiset PostgreSQL 17 answers, one canonical string per
	// row, measured live over these exact fixture rows.
	want []string
	// cols is the output column list both paths must publish. Empty means the
	// cell does not assert names (a zero-row result carries none on the DAG).
	cols []string
	// wantRoutes is the routing delta each DAG arm must show; the zero value
	// means the DAG executed the shape rather than refusing it to the
	// coordinator-local pipeline.
	wantRoutes a2Routes
}

// setOpWinCells: decpair carries a = {12.75, 12.75, 12.75, -0.01, 2.00, 0.00,
// NULL, 12.75, NULL} so SUM(a) OVER () is 52.99, and a TEXT column `s` whose
// values are numeric-looking text — the collision #733's headline turns on.
func setOpWinCells() []setOpWinCell {
	// decpair.b by id, as PostgreSQL renders it.
	b := []string{"12.75", "12.7501", "12.7499", "-0.01", "10", "0", "1", "NULL", "NULL"}
	unionRows := func() []string {
		out := make([]string, 0, 18)
		for i := 1; i <= 9; i++ {
			out = append(out, fmt.Sprintf("%d|52.99", i))
			out = append(out, fmt.Sprintf("%d|%s", i, b[i-1]))
		}
		return out
	}
	winOnly := func() []string {
		out := make([]string, 0, 9)
		for i := 1; i <= 9; i++ {
			out = append(out, fmt.Sprintf("%d|52.99", i))
		}
		return out
	}
	return []setOpWinCell{
		{issue: "#733", name: "aliased_window_collides_with_an_input_column",
			sql:  `SELECT id, SUM(a) OVER () AS s FROM decpair UNION ALL SELECT id, b AS s FROM decpair`,
			want: unionRows(), cols: []string{"id", "s"}},
		{issue: "#733", name: "aliased_window_with_no_input_column_of_that_name",
			sql:  `SELECT id, SUM(a) OVER () AS s2 FROM decpair UNION ALL SELECT id, b AS s2 FROM decpair`,
			want: unionRows(), cols: []string{"id", "s2"}},
		{issue: "#733", name: "unaliased_window_in_an_arm",
			sql:  `SELECT id, SUM(a) OVER () FROM decpair UNION ALL SELECT id, b FROM decpair`,
			want: unionRows(), cols: []string{"id", "sum"}},
		// The INTERSECT / EXCEPT siblings of the headline shape. INTERSECT is
		// empty — no (id, b) pair equals (id, 52.99) — and a zero-row result
		// carries no column list on the DAG, so that cell asserts rows only.
		{issue: "#733", name: "intersect_sibling",
			sql:  `SELECT id, SUM(a) OVER () AS s FROM decpair INTERSECT SELECT id, b AS s FROM decpair`,
			want: nil},
		{issue: "#733", name: "except_sibling",
			sql:  `SELECT id, SUM(a) OVER () AS s FROM decpair EXCEPT SELECT id, b AS s FROM decpair`,
			want: winOnly(), cols: []string{"id", "s"}},
		{issue: "#733", name: "except_all_sibling",
			sql:  `SELECT id, SUM(a) OVER () AS s FROM decpair EXCEPT ALL SELECT id, b AS s FROM decpair`,
			want: winOnly(), cols: []string{"id", "s"}},
		// The third face: two window arms UNIONed, then JOINed to a third
		// block. wintab0's SUM(plain) is 10000 and its SUM(id) is 10 —
		// deliberately different, so an arm reading the other arm's window is
		// visible as a value and not only as a failure.
		{issue: "#733", name: "two_window_arms_under_a_join",
			sql: `SELECT z.w AS zw, k.id AS kid FROM (SELECT id, SUM(plain) OVER () AS w FROM wintab0 ` +
				`UNION ALL SELECT id, SUM(id) OVER () AS w FROM wintab0) z JOIN wintab0 k ON z.id = k.id`,
			want: []string{"10000|1", "10000|2", "10000|3", "10000|4", "10|1", "10|2", "10|3", "10|4"},
			cols: []string{"zw", "kid"}},
		// #746: an aliased window in an arm over a table that STORES a
		// reserved-family column. The issue's own spelling unions that arm
		// with oldtab's TEXT `plain` column, which PostgreSQL refuses
		// (42804, bigint vs text) — see the numeric-∪-text gate; the shape
		// this cell keeps is the one PostgreSQL answers.
		{issue: "#746", name: "aliased_window_over_a_stored_reserved_column",
			sql:  `SELECT __win_0, SUM(id) OVER () AS w FROM wintab0 UNION ALL SELECT __win_0, id FROM oldtab`,
			want: []string{"100|1", "100|10", "200|10", "200|2", "300|10", "300|3", "400|10", "400|4"},
			cols: []string{"__win_0", "w"}},
		// The controls that were already right and must stay so: a window
		// inside a larger EXPRESSION (which the builder extracts from the
		// column's AST, not from the list), and an arm with no window at all.
		{issue: "#733", name: "ctl_window_inside_an_expression",
			sql:  `SELECT id, SUM(a) OVER () + 0 AS s FROM decpair UNION ALL SELECT id, b AS s FROM decpair`,
			want: unionRows(), cols: []string{"id", "s"}},
		{issue: "#733", name: "ctl_no_window_in_either_arm",
			sql: `SELECT id, a AS s FROM decpair UNION ALL SELECT id, b AS s FROM decpair`,
			want: []string{"1|12.75", "1|12.75", "2|12.75", "2|12.7501", "3|12.75", "3|12.7499",
				"4|-0.01", "4|-0.01", "5|2", "5|10", "6|0", "6|0", "7|1", "7|NULL",
				"8|12.75", "8|NULL", "9|NULL", "9|NULL"},
			cols: []string{"id", "s"}},
	}
}

func TestAWindowInASetOperationArmIsTheArmsOwnOutput(t *testing.T) {
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
	rsWriteOldTable(t, ctx, infra, infraB)
	rsIngestOldTable(t, ctx, single)

	for _, tc := range setOpWinCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			// Both sides sorted: the cells list PostgreSQL's rows in the order
			// it printed them, and a set operation's row ORDER is not defined.
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, res *oracle.Result, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17 answers %d rows",
						arm, err, tc.sql, len(tc.want))
				}
				got := setOpCanonRows(res)
				if strings.Join(got, " ") != strings.Join(want, " ") {
					t.Errorf("%s arm rows\n  got  %v\n  want %v (PostgreSQL 17)\n  SQL: %s",
						arm, got, want, tc.sql)
				}
				if len(tc.cols) > 0 && strings.Join(res.Columns, ",") != strings.Join(tc.cols, ",") {
					t.Errorf("%s arm columns\n  got  %v\n  want %v\n  SQL: %s",
						arm, res.Columns, tc.cols, tc.sql)
				}
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			check("single", sres, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				check(arm.name, dres, derr)
			}
		})
	}
}

// setOpCanonRows renders a result as one sorted canonical string per row.
//
// A numeric cell is normalised through big.Rat, so a DECIMAL rendered at the
// set operation's resolved scale (52.9900 under a DECIMAL(18,4) sibling arm)
// compares equal to PostgreSQL's unconstrained numeric (52.99). Anything that
// is not a number keeps its text, which is what makes a TEXT column answered
// in place of a window value visible rather than silently normalised away.
func setOpCanonRows(res *oracle.Result) []string {
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			parts = append(parts, setOpCanonValue(r[c]))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out
}

func setOpCanonValue(v any) string {
	if v == nil {
		return "NULL"
	}
	s := fmt.Sprintf("%v", v)
	rat, ok := new(big.Rat).SetString(s)
	if !ok {
		return s
	}
	// FloatString(40) then trimmed: exact for every value these fixtures hold
	// — the widest is a DECIMAL(38,s) — and it renders 12.7500 and 12.75
	// identically. Ten places was not enough: it silently rounded a scale-20
	// literal to nine digits, which is the comparator hiding the very thing a
	// literal-arm gate is about.
	t := strings.TrimRight(rat.FloatString(40), "0")
	t = strings.TrimSuffix(t, ".")
	if t == "-0" {
		return "0"
	}
	return t
}
