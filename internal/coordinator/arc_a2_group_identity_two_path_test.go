package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// One expression written two ways is one GROUP BY key (#738).
//
// `SELECT typemx.g + 1 AS k, COUNT(*) FROM typemx GROUP BY g + 1` was 42803 on
// every arm — `column "typemx.g" must appear in the GROUP BY clause` — and
// PostgreSQL answers it. So did `CAST(g AS DEC(9,2))` against
// `GROUP BY CAST(g AS DECIMAL(9,2))`, and `CAST(g AS INT)` against
// `CAST(g AS INTEGER)`.
//
// Two sites in `plansql.canonicalExpr`, the function `ExprIdentity` renders
// through:
//
//   - the `*CastNode` arm upper-cased the destination and passed it on, so
//     `INT` and `INTEGER` rendered differently and never matched. It folds
//     synonyms now, over PostgreSQL's own set — measured, not assumed, because
//     getting it wrong in the other direction makes two DIFFERENT expressions
//     one identity.
//   - the `*ColRef` arm kept the table qualifier. That one CANNOT be erased in
//     `canonicalExpr`, which is a pure text function: `a.x` and `b.x` over a
//     join are two expressions and `t.x` and `x` in a single-relation block are
//     one, and only a caller holding the FROM list can tell them apart. So the
//     erasure lives in `physical.groupCheck`, which has that scope, and fires
//     only when the block has ONE source.
//
// The erasure is on the TERM and not on the KEY, and the MIRROR spelling below
// is the boundary that states it.
type a2IdentCell struct {
	issue, name, sql string
	want             []string
	wantErrLike      string
	wantState        string
	// wantUnreach is the UnreachableOutputLocalRoutes delta each DAG arm must
	// show. A derived key that routes is right-but-routed, and rows alone
	// cannot tell that from the DAG executing it (rule 11).
	wantUnreach int64
	pgSays      string
}

func a2IdentCells() []a2IdentCell {
	// typemx has 5000 rows over 8 values of g, one of them NULL.
	rows := func(vals ...string) []string { return vals }
	return []a2IdentCell{
		// ---- the qualifier on the SELECT ITEM -----------------------------
		{issue: "#738", name: "qualified_select_item_bare_key",
			sql: `SELECT typemx.g + 1 AS k, COUNT(*) AS n FROM typemx WHERE id < 24 GROUP BY g + 1 ORDER BY k`,
			want: rows("k=int64:1|n=int64:4", "k=int64:2|n=int64:4", "k=int64:3|n=int64:4",
				"k=int64:4|n=int64:3", "k=int64:5|n=int64:3", "k=int64:6|n=int64:2",
				"k=int64:7|n=int64:3", "k=NULL|n=int64:1"),
			pgSays: "8 groups: 4,4,4,3,3,2,3 and one NULL group of 1"},
		{issue: "#738", name: "aliased_qualifier_select_item",
			sql: `SELECT t.g + 1 AS k, COUNT(*) AS n FROM typemx t WHERE t.id < 24 GROUP BY g + 1 ORDER BY k`,
			want: rows("k=int64:1|n=int64:4", "k=int64:2|n=int64:4", "k=int64:3|n=int64:4",
				"k=int64:4|n=int64:3", "k=int64:5|n=int64:3", "k=int64:6|n=int64:2",
				"k=int64:7|n=int64:3", "k=NULL|n=int64:1"),
			pgSays: "8 groups: 4,4,4,3,3,2,3 and one NULL group of 1"},

		// ---- the CAST type synonyms ---------------------------------------
		{issue: "#738", name: "cast_dec_against_decimal",
			sql: `SELECT CAST(g AS DEC(9,2)) AS k, COUNT(*) AS n FROM typemx WHERE id < 24 ` +
				`GROUP BY CAST(g AS DECIMAL(9,2)) ORDER BY k`,
			want: rows("k=0.00|n=int64:4", "k=1.00|n=int64:4", "k=2.00|n=int64:4",
				"k=3.00|n=int64:3", "k=4.00|n=int64:3", "k=5.00|n=int64:2",
				"k=6.00|n=int64:3", "k=NULL|n=int64:1"),
			wantUnreach: 1,
			pgSays:      "8 groups: 4,4,4,3,3,2,3 and one NULL group of 1"},
		// The MIRROR of the same pair, which is a different code path: the
		// synonym is on the KEY rather than on the select item.
		{issue: "#738", name: "cast_decimal_against_dec",
			sql: `SELECT CAST(g AS DECIMAL(9,2)) AS k, COUNT(*) AS n FROM typemx WHERE id < 24 ` +
				`GROUP BY CAST(g AS DEC(9,2)) ORDER BY k`,
			want: rows("k=0.00|n=int64:4", "k=1.00|n=int64:4", "k=2.00|n=int64:4",
				"k=3.00|n=int64:3", "k=4.00|n=int64:3", "k=5.00|n=int64:2",
				"k=6.00|n=int64:3", "k=NULL|n=int64:1"),
			wantUnreach: 1,
			pgSays:      "8 groups: 4,4,4,3,3,2,3 and one NULL group of 1"},
		{issue: "#738", name: "cast_int_against_integer",
			sql: `SELECT CAST(g AS INT) AS k, COUNT(*) AS n FROM typemx WHERE id < 24 ` +
				`GROUP BY CAST(g AS INTEGER) ORDER BY k`,
			want: rows("k=int64:0|n=int64:4", "k=int64:1|n=int64:4", "k=int64:2|n=int64:4",
				"k=int64:3|n=int64:3", "k=int64:4|n=int64:3", "k=int64:5|n=int64:2",
				"k=int64:6|n=int64:3", "k=NULL|n=int64:1"),
			wantUnreach: 1,
			pgSays:      "8 groups: 4,4,4,3,3,2,3 and one NULL group of 1"},

		// ---- the controls that were right and must stay right -------------
		{issue: "#738", name: "ctl_bare_qualified_reference",
			sql: `SELECT typemx.g AS k, COUNT(*) AS n FROM typemx WHERE id < 24 GROUP BY g ORDER BY k`,
			want: rows("k=int32:0|n=int64:4", "k=int32:1|n=int64:4", "k=int32:2|n=int64:4",
				"k=int32:3|n=int64:3", "k=int32:4|n=int64:3", "k=int32:5|n=int64:2",
				"k=int32:6|n=int64:3", "k=NULL|n=int64:1"),
			pgSays: "already right before this, through GroupKeyName"},
		{issue: "#738", name: "ctl_parenthesised_spelling_routes",
			sql: `SELECT (g) + 1 AS k, COUNT(*) AS n FROM typemx WHERE id < 24 GROUP BY g + 1 ORDER BY k`,
			want: rows("k=int64:1|n=int64:4", "k=int64:2|n=int64:4", "k=int64:3|n=int64:4",
				"k=int64:4|n=int64:3", "k=int64:5|n=int64:3", "k=int64:6|n=int64:2",
				"k=int64:7|n=int64:3", "k=NULL|n=int64:1"),
			wantUnreach: 1,
			pgSays: "right, and RIGHT BY ROUTING on the DAG. An identity that " +
				"is right for the aggregate can still be a name the gather cannot reach"},

		// ---- the boundaries, each with a fixture that attempts it ---------
		//
		// VARCHAR and TEXT are NOT synonyms. PostgreSQL refuses this pair with
		// the same 42803, so the refusal is right in kind and the synonym set
		// must not grow to include it.
		{issue: "#738", name: "boundary_varchar_is_not_text",
			sql:         `SELECT CAST(g AS VARCHAR) AS k, COUNT(*) AS n FROM typemx GROUP BY CAST(g AS TEXT) ORDER BY k`,
			wantErrLike: `column "g" must appear in the GROUP BY clause`,
			wantState:   "42803",
			pgSays:      `42803 — PostgreSQL refuses it too`},
		// Two JOIN SIDES. The qualifier is identity there, not spelling, and
		// erasing it would license `SELECT zzp.d92` under `GROUP BY zzj.d92` —
		// a different table's value under the key's name. PostgreSQL refuses
		// this as well.
		{issue: "#738", name: "boundary_two_join_sides",
			sql: `SELECT zzp.d92 AS k, COUNT(*) AS n FROM zzp JOIN zzj ON zzp.id = zzj.id ` +
				`GROUP BY zzj.d92 ORDER BY k`,
			wantErrLike: `column "zzp.d92" must appear in the GROUP BY clause`,
			wantState:   "42803",
			pgSays:      `42803 — PostgreSQL refuses it too`},
		// THE BOUND THIS COMMIT SETS, and PostgreSQL ANSWERS it: the MIRROR
		// spelling, a QUALIFIED key with a bare select item. The erasure is on
		// the TERM alone, because answering this needs the aggregate to
		// evaluate `typemx.g + 1` over a batch whose column is `g` — it
		// cannot, and when the identity was erased on both sides the
		// projection above read a column that does not exist and every group's
		// key came back NULL. A loud 42803 beats a plausible NULL (protocol
		// method 8), so the refusal stays and this fixture holds it.
		{issue: "#738", name: "boundary_qualified_key_bare_select_item",
			sql:         `SELECT g + 1 AS k, COUNT(*) AS n FROM typemx GROUP BY typemx.g + 1 ORDER BY k`,
			wantErrLike: `column "g" must appear in the GROUP BY clause`,
			wantState:   "42803",
			pgSays: "PostgreSQL ANSWERS this. Wadjet refuses it, loudly, because the aggregate " +
				"cannot evaluate a qualified key over an unqualified batch"},
	}
}

func TestTheIdentityErasesAQualifierAndATypeSynonym(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range a2IdentCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, got []string, err error) {
				t.Helper()
				sort.Strings(got)
				if tc.wantErrLike != "" {
					if err == nil {
						t.Errorf("%s arm: ANSWERED %v — this shape's refusal is a stated BOUND. "+
							"If it is lifted deliberately, assert the rows and delete this cell's "+
							"wantErrLike.\n  PostgreSQL 17: %s\n  SQL: %s", arm, got, tc.pgSays, tc.sql)
						return
					}
					if !strings.Contains(err.Error(), tc.wantErrLike) {
						t.Errorf("%s arm: error %v\n  want one containing %q\n  SQL: %s",
							arm, err, tc.wantErrLike, tc.sql)
					}
					if s := sqlerr.StateOf(err); s != tc.wantState {
						t.Errorf("%s arm: SQLSTATE %q, want %q\n  SQL: %s", arm, s, tc.wantState, tc.sql)
					}
					return
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", arm, err, tc.pgSays, tc.sql)
					return
				}
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
						arm, got, want, tc.pgSays, tc.sql)
				}
			}

			sgot, serr := na2Run(tmdRunSingle(ctx, single, tc.sql))
			check("single", sgot, serr)
			for i := 0; i < 5; i++ {
				got, err := na2Run(tmdRunSingle(ctx, spilled, tc.sql))
				check("spilled", got, err)
				if t.Failed() {
					break
				}
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := arm.c.UnreachableOutputLocalRoutes()
				got, err := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
				if d := arm.c.UnreachableOutputLocalRoutes() - before; d != tc.wantUnreach {
					t.Errorf("%s arm: UnreachableOutputLocalRoutes moved by %d, want %d\n"+
						"  (0 = the DAG executed this shape; 1 = it refused the plan and the "+
						"coordinator-local pipeline answered)\n  SQL: %s",
						arm.name, d, tc.wantUnreach, tc.sql)
				}
			}
		})
	}
}
