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
// Two more mechanisms show up in the same shapes and are fixed with it. The
// PAYLOAD: distinct slots put a name on the join stage that its exchanges'
// manifests were built without, so the shuffle dropped the column the fragment
// was about to read (`ensureJoinCarriesEvaluatedColumns`,
// `ensureJoinCarriesGatherOutputs`). And the PRUNER: a subtree's available
// columns were read off its SCANS alone, so a name a Project MINTS belonged to
// neither side of a join and was dropped at the partition — which is why every
// three-arm shape below needed a second join to fail (#700, #726).
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

	// check holds every arm to PostgreSQL's value, with two named escapes.
	//
	// singleScale is TODO(#754): where two join arms publish the SAME output
	// alias, the single-process path renders one of them at the OTHER arm's
	// DECIMAL scale. Right digits, wrong typmod, single arm only — and it is
	// the duplicate ALIAS that triggers it, not the DECIMAL: the `w`/`w2`
	// entry below carries identical values and agrees exactly. The escape
	// FAILS the day the single arm renders PostgreSQL's spelling.
	check := func(t *testing.T, sql string, rows int, want, singleScale map[string]string) {
		t.Helper()
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused a query PostgreSQL answers: %v\n  SQL: %s",
					arm.name, err, sql)
			}
			if len(res.Rows) != rows {
				t.Fatalf("%s arm returned %d rows, want %d\n  SQL: %s",
					arm.name, len(res.Rows), rows, sql)
			}
			for col, wantVal := range want {
				pinned := false
				if arm.name == "single" {
					if s, ok := singleScale[col]; ok {
						wantVal, pinned = s, true
					}
				}
				for i, r := range res.Rows {
					got := fmt.Sprintf("%v", r[col])
					if got == wantVal {
						continue
					}
					if pinned {
						t.Errorf("the single arm now renders %s=%q where it rendered %q — "+
							"TODO(#754) is fixed, delete the singleScale entry\n  SQL: %s",
							col, got, wantVal, sql)
						break
					}
					t.Errorf("%s arm row %d: %s = %q, PostgreSQL 17 answers %q — two sibling "+
						"blocks handed one of them the other's window\n  SQL: %s",
						arm.name, i, col, got, wantVal, sql)
					break
				}
			}
		}
	}

	scaleQW := map[string]string{"qw": "52.9900"}

	t.Run("two siblings under an INNER join", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, p.id FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"}, scaleQW)
	})
	t.Run("two siblings under a LEFT join", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, p.id FROM "+pw+" LEFT JOIN "+qw+
			" ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"}, scaleQW)
	})
	// The arms publish DIFFERENT output names, so nothing about this shape is
	// a name collision — only the hidden slot was shared. It is the entry that
	// proves the defect was provenance and not spelling: it collapsed on both
	// DAG arms while `w` and `w2` were as distinct as two names can be. It is
	// also the control for TODO(#754): identical values, no duplicate alias,
	// and every arm renders 52.99.
	t.Run("two siblings publishing different names", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w2 AS qw, p.id FROM "+pw+" JOIN "+
			"(SELECT id, SUM(a) OVER () AS w2 FROM "+dbpTable+") q ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"}, nil)
	})
	// Selection order swapped: which reference is written first must not
	// decide which window each one gets.
	t.Run("two siblings, the later arm selected first", func(t *testing.T) {
		check(t, "SELECT q.w AS qw, p.w AS pw, p.id FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"}, scaleQW)
	})

	// THREE siblings, which is the shape that collapsed on EVERY path: the
	// third block's slot won and all three outputs answered MIN(a). Both
	// spellings are here because they take different routes through
	// attachScanSelectProjections — with a passthrough column the sort keys on
	// a real column, without one it keys on a synthetic `__sortkey_0`, and
	// that second route is where the payload gap showed as a LOUD failure
	// rather than a wrong number.
	t.Run("three siblings", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, r.w AS rw, p.id FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id JOIN "+rw+" ON p.id = r.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99", "rw": "-0.01"},
			map[string]string{"qw": "52.9900", "rw": "-0.0100"})
	})
	t.Run("three siblings, no passthrough column", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, r.w AS rw FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id JOIN "+rw+" ON p.id = r.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99", "rw": "-0.01"},
			map[string]string{"qw": "52.9900", "rw": "-0.0100"})
	})

	// The CTE spelling of the two blocks. Both DAG arms are right and the
	// single path publishes p's window under BOTH columns, which is the last
	// face of #753 — a CTE reference is materialized differently from the
	// identical derived table, whose spelling agrees two entries up.
	// TODO(#753): the pin FAILS the day the single arm agrees.
	t.Run("two sibling CTEs", func(t *testing.T) {
		sql := "WITH p AS (SELECT id, SUM(b) OVER () AS w FROM " + dbpTable + "), " +
			"q AS (SELECT id, SUM(a) OVER () AS w FROM " + dbpTable + ") " +
			"SELECT p.w AS pw, q.w AS qw, p.id FROM p JOIN q ON p.id = q.id ORDER BY p.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			want := "52.99"
			pinned := false
			if arm.name == "single" {
				want, pinned = "49.2400", true // TODO(#753)
			}
			for i, r := range res.Rows {
				got := fmt.Sprintf("%v", r["qw"])
				if got == want {
					continue
				}
				if pinned {
					t.Errorf("the single arm now answers qw=%q — TODO(#753) is fixed, delete "+
						"this pin and assert 52.99 on every arm\n  SQL: %s", got, sql)
					break
				}
				t.Errorf("%s arm row %d: qw = %q, PostgreSQL 17 answers 52.99\n  SQL: %s",
					arm.name, i, got, sql)
				break
			}
		}
	})

	// A sibling NESTED inside a sibling (#751). Both DAG arms are right and
	// the single path answers the OUTER sibling's window for the inner one, so
	// the wrong side is the local engine. The DAG arms are ASSERTED, not
	// skipped, so a fix that breaks them fails here rather than passing as
	// "both arms agree".
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
			want, pinned := "10", false
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
				break
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
	// ones. The pruner fix moved the SINGLE path onto PostgreSQL's answer —
	// `x.w` is a window, so 52.99 on every row — and left both DAG arms
	// answering z's `a * 3`, which VARIES per row. A per-row value where the
	// query asks for a whole-partition window is the tell, so the pin reads
	// two rows rather than one. TODO(#742).
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
					t.Logf("tracked separately (#755), NOT gated here: %v", err)
					continue
				}
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 9 {
				t.Fatalf("%s arm returned %d rows, want 9\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			first := fmt.Sprintf("%v", res.Rows[0]["w"])
			fourth := fmt.Sprintf("%v", res.Rows[3]["w"])
			if arm.name == "single" {
				// GATED: the single path answers the window on every row.
				if first != "52.99" || fourth != "52.99" {
					t.Errorf("the single arm answered x.w=%q/%q, PostgreSQL 17 answers 52.99 on "+
						"every row\n  SQL: %s", first, fourth, sql)
				}
				continue
			}
			if first == "52.99" && fourth == "52.99" {
				t.Errorf("the %s arm now answers x.w=52.99 on every row — TODO(#742) is fixed, "+
					"delete this pin and assert it on all three arms\n  SQL: %s", arm.name, sql)
				continue
			}
			if first != "38.25" || fourth != "-0.03" {
				t.Errorf("the %s arm answered x.w=%q/%q, which is neither the pinned wrong values "+
					"(38.25 and -0.03, z's `a * 3`) nor PostgreSQL's constant 52.99\n  SQL: %s",
					arm.name, first, fourth, sql)
			}
		}
	})

	// The control the whole family needs: two sibling blocks with NO window,
	// which resolved correctly before any of this and must keep doing so. It
	// is what says the repair moved the SLOT and not the name resolution.
	t.Run("control: two sibling blocks with no window", func(t *testing.T) {
		// Both outputs are PER-ROW expressions here, not whole-partition
		// windows, so the assertion is the first row's pair rather than a
		// constant: id=1 has a=12.75, giving a+1=13.75 and a*3=38.25.
		sql := "SELECT p.w AS pw, q.w AS qw, p.id FROM " +
			"(SELECT id, a + 1 AS w FROM " + dbpTable + ") p JOIN " +
			"(SELECT id, a * 3 AS w FROM " + dbpTable + ") q ON p.id = q.id ORDER BY p.id"
		for _, arm := range arms {
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 9 {
				t.Fatalf("%s arm returned %d rows, want 9\n  SQL: %s", arm.name, len(res.Rows), sql)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["pw"]); got != "13.75" {
				t.Errorf("%s arm: pw = %q, PostgreSQL 17 answers 13.75\n  SQL: %s", arm.name, got, sql)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["qw"]); got != "38.25" {
				t.Errorf("%s arm: qw = %q, PostgreSQL 17 answers 38.25\n  SQL: %s", arm.name, got, sql)
			}
		}
	})
	// And the control one level in: TWO windows inside ONE block already got
	// distinct slots from the per-block allocator, and must keep them.
	t.Run("control: two windows in one block", func(t *testing.T) {
		check(t, "SELECT SUM(b) OVER () AS w1, SUM(a) OVER () AS w2, id FROM "+dbpTable+
			" ORDER BY id", 9,
			map[string]string{"w1": "49.2400", "w2": "52.99"}, nil)
	})
}
