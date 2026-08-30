package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// Sibling subqueries under a JOIN keep their own values (#747, #742, #751).
//
// The window slot counter lives in `logical.BuildFromSelectWithCTEs`, which
// recurses per SELECT BLOCK, so every block starts at zero and two sibling
// subqueries mint the SAME `__win_0`. Both arms then carried a column of that
// one name into the join, and the projection above it published one window's
// value under both output columns — silently, and at three siblings on every
// execution path.
//
// ADR-0025 said the opposite ("the blocks' slots are already distinct — the
// allocator is per query") and no fixture attempted it, which is method 10 of
// docs/design/correctness-fix-protocol.md exactly. `renameCollidingSlots` now
// renumbers a slot a sibling block already minted, and this is the corpus that
// attempts the impossibility the claim asserts.
//
// The second half is the payload. Distinct slots put a name on the join stage
// that its exchanges' manifests were built without — those come from the join
// node's NeededColumns, computed long before `attachScanSelectProjections`
// decides the join is where the SELECT list is evaluated — so the shuffle
// dropped the column the fragment was about to read (`column "__win_0" does
// not exist in the input schema`, and before that a silent zero for the same
// reason on a filter, #700). `ensureJoinCarriesEvaluatedColumns` and
// `ensureJoinCarriesGatherOutputs` close that loop.
//
// Values are PostgreSQL 17's over the nine decpair rows, from a live server:
// SUM(b) = 49.2400, SUM(a) = 52.99, MIN(a) = -0.01. They are deliberately all
// DIFFERENT — two equal window values make a collapse invisible.
func TestSiblingWindowSubqueriesUnderAJoinKeepTheirOwnValues(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB)
	coordB.config.BroadcastBytesOverride = 1

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }},
	}

	const pw = "(SELECT id, SUM(b) OVER () AS w FROM " + dbpTable + ") p"
	const qw = "(SELECT id, SUM(a) OVER () AS w FROM " + dbpTable + ") q"
	const rw = "(SELECT id, MIN(a) OVER () AS w FROM " + dbpTable + ") r"

	for _, tc := range []struct {
		name string
		sql  string
		rows int
		// want is the PostgreSQL answer per output column, on every row.
		want map[string]string
		// singleScale names a column whose value the SINGLE arm renders at
		// the OTHER join arm's DECIMAL scale — right digits, wrong typmod.
		// TODO(#754): delete the entry when the two agree; the assertion
		// below FAILS if the single arm starts rendering PostgreSQL's
		// spelling, which is that fix's proof.
		singleScale map[string]string
	}{
		{
			name:        "two siblings under an INNER join",
			sql:         "SELECT p.w AS pw, q.w AS qw, p.id FROM " + pw + " JOIN " + qw + " ON p.id = q.id ORDER BY p.id",
			rows:        9,
			want:        map[string]string{"pw": "49.2400", "qw": "52.99"},
			singleScale: map[string]string{"qw": "52.9900"},
		},
		{
			name: "two siblings under a LEFT join",
			sql: "SELECT p.w AS pw, q.w AS qw, p.id FROM " + pw + " LEFT JOIN " + qw +
				" ON p.id = q.id ORDER BY p.id",
			rows:        9,
			want:        map[string]string{"pw": "49.2400", "qw": "52.99"},
			singleScale: map[string]string{"qw": "52.9900"},
		},
		{
			// The arms publish DIFFERENT output names, so nothing about this
			// shape is a name collision — only the hidden slot is shared.
			// It is the entry that proves the defect is provenance and not
			// spelling: it collapsed on both DAG arms while `w` and `w2`
			// were as distinct as two names can be.
			name: "two siblings publishing different names",
			sql: "SELECT p.w AS pw, q.w2 AS qw, p.id FROM " + pw + " JOIN " +
				"(SELECT id, SUM(a) OVER () AS w2 FROM " + dbpTable + ") q ON p.id = q.id ORDER BY p.id",
			rows: 9,
			want: map[string]string{"pw": "49.2400", "qw": "52.99"},
		},
		{
			// Selection order swapped: which reference is written first must
			// not decide which window each one gets.
			name: "two siblings, the later arm selected first",
			sql: "SELECT q.w AS qw, p.w AS pw, p.id FROM " + pw + " JOIN " + qw +
				" ON p.id = q.id ORDER BY p.id",
			rows:        9,
			want:        map[string]string{"pw": "49.2400", "qw": "52.99"},
			singleScale: map[string]string{"qw": "52.9900"},
		},
		{
			// The CTE spelling of the same two blocks.
			name: "two sibling CTEs",
			sql: "WITH p AS (SELECT id, SUM(b) OVER () AS w FROM " + dbpTable + "), " +
				"q AS (SELECT id, SUM(a) OVER () AS w FROM " + dbpTable + ") " +
				"SELECT p.w AS pw, q.w AS qw, p.id FROM p JOIN q ON p.id = q.id ORDER BY p.id",
			rows: 9,
			// TODO(#753): the single arm answers p's window for both, which
			// is the join-of-derived-computed-columns defect rather than the
			// slot collision — the DAG arms are right here.
			want: map[string]string{"pw": "49.2400", "qw": "52.99"},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				if len(res.Rows) != tc.rows {
					t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
						arm.name, len(res.Rows), tc.rows, tc.sql)
				}
				for col, want := range tc.want {
					if arm.name == "single" {
						if s, ok := tc.singleScale[col]; ok {
							want = s
						}
					}
					if tc.name == "two sibling CTEs" && arm.name == "single" && col == "qw" {
						want = "49.2400" // TODO(#753)
					}
					for i, r := range res.Rows {
						got := fmt.Sprintf("%v", r[col])
						if got == want {
							continue
						}
						if arm.name == "single" && tc.singleScale[col] != "" {
							t.Errorf("the single arm now answers %s=%q where it rendered %q — "+
								"TODO(#754) is fixed, delete the singleScale entry\n  SQL: %s",
								col, got, want, tc.sql)
							break
						}
						if tc.name == "two sibling CTEs" && arm.name == "single" {
							t.Errorf("the single arm now answers %s=%q — TODO(#753) is fixed, "+
								"delete the pin and assert PostgreSQL's 52.99\n  SQL: %s",
								col, got, tc.sql)
							break
						}
						t.Errorf("%s arm row %d: %s = %q, PostgreSQL 17 answers %q — two sibling "+
							"blocks handed one of them the other's window\n  SQL: %s",
							arm.name, i, col, got, want, tc.sql)
					}
				}
			}
		})
	}

	// THREE siblings, which is the shape that collapsed on every path: the
	// third block's slot won and all three outputs answered MIN(a).
	//
	// The single arm is a DIFFERENT defect at this width — a derived table's
	// computed column read above a join OF JOINS, which #753 reproduces with
	// no window anywhere — so it is pinned rather than exempted: the day it
	// agrees, the pin fails.
	t.Run("three siblings", func(t *testing.T) {
		sql := "SELECT p.w AS pw, q.w AS qw, r.w AS rw, p.id FROM " + pw + " JOIN " + qw +
			" ON p.id = q.id JOIN " + rw + " ON p.id = r.id ORDER BY p.id"
		want := map[string]string{"pw": "49.2400", "qw": "52.99", "rw": "-0.01"}
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 9 {
				t.Fatalf("%s arm returned %d rows, want 9\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			if arm.name == "single" {
				// TODO(#753): pinned at r's window for all three columns.
				for _, r := range res.Rows {
					if got := fmt.Sprintf("%v", r["pw"]); got != "-0.01" {
						t.Errorf("the single arm now answers pw=%q for three siblings — "+
							"TODO(#753) is fixed, delete this pin and assert %q on every arm\n  SQL: %s",
							got, want["pw"], sql)
						break
					}
				}
				continue
			}
			for col, wantVal := range want {
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r[col]); got != wantVal {
						t.Errorf("%s arm row %d: %s = %q, PostgreSQL 17 answers %q\n  SQL: %s",
							arm.name, i, col, got, wantVal, sql)
					}
				}
			}
		}
	})

	// The same three blocks with NO extra column in the SELECT list. It takes
	// a different route through attachScanSelectProjections — a synthetic
	// sort key rather than a real one — and that route is where the payload
	// gap showed as a loud failure instead of a wrong number.
	t.Run("three siblings, no passthrough column", func(t *testing.T) {
		sql := "SELECT p.w AS pw, q.w AS qw, r.w AS rw FROM " + pw + " JOIN " + qw +
			" ON p.id = q.id JOIN " + rw + " ON p.id = r.id ORDER BY p.id"
		for _, arm := range arms {
			if arm.name == "single" {
				continue // TODO(#753), asserted above
			}
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			for col, wantVal := range map[string]string{"pw": "49.2400", "qw": "52.99", "rw": "-0.01"} {
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r[col]); got != wantVal {
						t.Errorf("%s arm row %d: %s = %q, PostgreSQL 17 answers %q\n  SQL: %s",
							arm.name, i, col, got, wantVal, sql)
					}
				}
			}
		}
	})

	// A sibling NESTED inside a sibling (#751). The DAG is right; the single
	// path answers the outer sibling's window for the inner one, which is a
	// two-path divergence with the wrong side being the local engine.
	t.Run("sibling nested in a sibling", func(t *testing.T) {
		sql := "SELECT p.w AS pw, q.w AS qw, p.id FROM " +
			"(SELECT id, SUM(plain) OVER () AS w FROM " + rsWinTab + ") p JOIN " +
			"(SELECT x.id, x.w FROM (SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") x) q " +
			"ON p.id = q.id ORDER BY p.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 4 {
				t.Fatalf("%s arm returned %d rows, want 4\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			// PostgreSQL: pw = SUM(plain) = 10000, qw = SUM(id) = 10.
			want := "10"
			pinned := false
			if arm.name == "single" {
				want, pinned = "10000", true // TODO(#751)
			}
			for i, r := range res.Rows {
				got := fmt.Sprintf("%v", r["qw"])
				if got == want {
					continue
				}
				if pinned {
					t.Errorf("the single arm now answers qw=%q — TODO(#751) is fixed, delete "+
						"this pin and assert 10 on every arm\n  SQL: %s", got, sql)
					break
				}
				t.Errorf("%s arm row %d: qw = %q, PostgreSQL 17 answers 10\n  SQL: %s",
					arm.name, i, got, sql)
			}
			for i, r := range res.Rows {
				if got := fmt.Sprintf("%v", r["pw"]); got != "10000" {
					t.Errorf("%s arm row %d: pw = %q, want 10000\n  SQL: %s", arm.name, i, got, sql)
				}
			}
		}
	})

	// #742's shape: a qualified reference satisfied by ANOTHER arm's
	// identically-named column, with a base table between the two derived
	// ones. Wrong on every arm, and pinned in that direction, because it is
	// the face of this family the slot fix does not reach — the two `w`s here
	// are ordinary computed aliases, not slots.
	t.Run("qualified reference across two arms publishing one name", func(t *testing.T) {
		sql := "SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " + dbpTable + ") x " +
			"JOIN " + dbpTable + " y ON x.id = y.id " +
			"JOIN (SELECT id, a * 3 AS w FROM " + dbpTable + ") z ON x.id = z.id ORDER BY x.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				// The shuffled arm has its own loud failure on a chained join
				// over derived arms (#755); it is not this shape's defect.
				if arm.name == "dag-shuffled" && strings.Contains(err.Error(), "output not found") {
					t.Logf("tracked separately (#755): %v", err)
					continue
				}
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			// PostgreSQL answers x's WINDOW — 52.99 on every one of the nine
			// rows. TODO(#742): every arm answers z's `a * 3` instead, which
			// VARIES per row (38.25, 38.25, 38.25, -0.03, 6.00, …); a
			// per-row value where the query asks for a whole-partition window
			// is the tell, so the pin reads the first row and the constancy.
			if len(res.Rows) != 9 {
				t.Fatalf("%s arm returned %d rows, want 9\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			first := fmt.Sprintf("%v", res.Rows[0]["w"])
			fourth := fmt.Sprintf("%v", res.Rows[3]["w"])
			if first == "52.99" && fourth == "52.99" {
				t.Errorf("%s arm now answers x.w=52.99 on every row — TODO(#742) is fixed, "+
					"delete this pin and assert it on all three arms\n  SQL: %s", arm.name, sql)
				continue
			}
			if first != "38.25" || fourth != "-0.03" {
				t.Errorf("%s arm answered x.w=%q/%q, which is neither the pinned wrong values "+
					"(38.25 and -0.03, z's `a * 3`) nor PostgreSQL's constant 52.99\n  SQL: %s",
					arm.name, first, fourth, sql)
			}
		}
	})

	// The control the whole family needs: two sibling blocks with NO window,
	// which resolve correctly today and must keep doing so. It is what says
	// the repair moved the slot and not the name resolution.
	t.Run("control: two sibling blocks with no window", func(t *testing.T) {
		sql := "SELECT p.w AS pw, q.w AS qw, p.id FROM " +
			"(SELECT id, a + 1 AS w FROM " + dbpTable + ") p JOIN " +
			"(SELECT id, a * 3 AS w FROM " + dbpTable + ") q ON p.id = q.id ORDER BY p.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["pw"]); got != "13.75" {
				t.Errorf("%s arm: pw = %q, want 13.75\n  SQL: %s", arm.name, got, sql)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["qw"]); got != "38.25" {
				t.Errorf("%s arm: qw = %q, want 38.25\n  SQL: %s", arm.name, got, sql)
			}
		}
	})

	// And the control one level in: TWO windows inside ONE block already got
	// distinct slots from the per-block allocator, and must keep them.
	t.Run("control: two windows in one block", func(t *testing.T) {
		sql := "SELECT SUM(b) OVER () AS w1, SUM(a) OVER () AS w2, id FROM " + dbpTable + " ORDER BY id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			for i, r := range res.Rows {
				if got := fmt.Sprintf("%v", r["w1"]); got != "49.2400" {
					t.Errorf("%s arm row %d: w1 = %q, want 49.2400\n  SQL: %s", arm.name, i, got, sql)
				}
				if got := fmt.Sprintf("%v", r["w2"]); got != "52.99" {
					t.Errorf("%s arm row %d: w2 = %q, want 52.99\n  SQL: %s", arm.name, i, got, sql)
				}
			}
		}
	})
}
