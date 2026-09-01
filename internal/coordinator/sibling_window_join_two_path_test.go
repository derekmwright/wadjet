package coordinator

import (
	"context"
	"fmt"
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
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

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

	// check holds every arm to PostgreSQL's value.
	//
	// It used to carry a singleScale escape for TODO(#754) — where two join
	// arms publish the SAME output alias, the single-process path rendered one
	// of them at the OTHER arm's DECIMAL scale. That is the same
	// qualified-reference disagreement as #706: the VALUE came through `q.w`
	// and the DECLARATION through the first bare `w`, which is the other arm's
	// column. The projection reads both from the qualified spelling now, and
	// every arm renders PostgreSQL's.
	check := func(t *testing.T, sql string, rows int, want map[string]string) {
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
				for i, r := range res.Rows {
					got := fmt.Sprintf("%v", r[col])
					if got == wantVal {
						continue
					}
					t.Errorf("%s arm row %d: %s = %q, PostgreSQL 17 answers %q — two sibling "+
						"blocks handed one of them the other's window\n  SQL: %s",
						arm.name, i, col, got, wantVal, sql)
					break
				}
			}
		}
	}

	t.Run("two siblings under an INNER join", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, p.id FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"})
	})
	t.Run("two siblings under a LEFT join", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, p.id FROM "+pw+" LEFT JOIN "+qw+
			" ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"})
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
			map[string]string{"pw": "49.2400", "qw": "52.99"})
	})
	// Selection order swapped: which reference is written first must not
	// decide which window each one gets.
	t.Run("two siblings, the later arm selected first", func(t *testing.T) {
		check(t, "SELECT q.w AS qw, p.w AS pw, p.id FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"})
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
			map[string]string{"pw": "49.2400", "qw": "52.99", "rw": "-0.01"})
	})
	t.Run("three siblings, no passthrough column", func(t *testing.T) {
		check(t, "SELECT p.w AS pw, q.w AS qw, r.w AS rw FROM "+pw+" JOIN "+qw+
			" ON p.id = q.id JOIN "+rw+" ON p.id = r.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99", "rw": "-0.01"})
	})

	// The CTE spelling of the two blocks. It was TODO(#753): the single path
	// published p's window under BOTH columns while the identical
	// DERIVED-table spelling two entries up agreed. The cause was the name
	// the join qualifies a build arm's duplicate column with — `findScanAlias`
	// read the SCAN below the CTE, so q's `w` shipped as `decpair.w` and
	// `q.w` matched neither it nor p's bare `w`. `joinArmAlias` names the arm
	// `q`, and the entry asserts PostgreSQL's value on every arm now.
	t.Run("two sibling CTEs", func(t *testing.T) {
		check(t, "WITH p AS (SELECT id, SUM(b) OVER () AS w FROM "+dbpTable+"), "+
			"q AS (SELECT id, SUM(a) OVER () AS w FROM "+dbpTable+") "+
			"SELECT p.w AS pw, q.w AS qw, p.id FROM p JOIN q ON p.id = q.id ORDER BY p.id", 9,
			map[string]string{"pw": "49.2400", "qw": "52.99"})
	})

	// A sibling NESTED inside a sibling (#751). The single path answered the
	// OUTER sibling's window for the inner one — both DAG arms were right, so
	// the wrong side was the local engine's, and they are ASSERTED here rather
	// than skipped so a fix that breaks them fails.
	//
	// The cause is the one `joinArmAlias` already fixed for a CTE arm, in the
	// DERIVED spelling: the join qualifies a build arm's duplicate columns by
	// the arm's name, and that name came from the SCAN below it.
	// `setSubtreeAlias` declines to overwrite a stamp an INNER derived table
	// already made, so the scan answered to `x` while the query calls the arm
	// `q`, and `q.w` matched neither that nor p's bare `w`. A derived table
	// records its own alias on its SUBTREE ROOT now, exactly as a CTE does.
	//
	// PostgreSQL 17: pw = SUM(plain) = 10000, qw = SUM(id) = 10. The two are
	// deliberately different numbers — equal ones make a capture invisible.
	for _, tc := range []struct {
		name, sql string
		pw, qw    string
	}{
		{
			name: "sibling nested in a sibling",
			sql: "SELECT p.w AS pw, q.w AS qw, p.id FROM " +
				"(SELECT id, SUM(plain) OVER () AS w FROM " + rsWinTab + ") p JOIN " +
				"(SELECT x.id, x.w FROM (SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") x) q " +
				"ON p.id = q.id ORDER BY p.id",
			pw: "10000", qw: "10",
		},
		{
			// The nested one FIRST, which was already right: the capture is
			// directional, and this says the repair is not merely a reorder.
			name: "the nested sibling is the first arm",
			sql: "SELECT p.w AS pw, q.w AS qw FROM " +
				"(SELECT x.id, x.w FROM (SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") x) q " +
				"JOIN (SELECT id, SUM(plain) OVER () AS w FROM " + rsWinTab + ") p " +
				"ON p.id = q.id ORDER BY p.id",
			pw: "10000", qw: "10",
		},
		{
			// BOTH siblings nested, so neither arm's scan answers to the name
			// the enclosing query wrote.
			name: "both siblings nested",
			sql: "SELECT p.w AS pw, q.w AS qw FROM " +
				"(SELECT y.id, y.w FROM (SELECT id, SUM(plain) OVER () AS w FROM " + rsWinTab + ") y) p " +
				"JOIN (SELECT x.id, x.w FROM (SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") x) q " +
				"ON p.id = q.id ORDER BY p.id",
			pw: "10000", qw: "10",
		},
		{
			// THREE derived levels on one arm: the alias the query wrote is
			// the outermost of three stamps.
			name: "three derived levels on the nested arm",
			sql: "SELECT p.w AS pw, q.w AS qw FROM " +
				"(SELECT z.id, z.w FROM (SELECT y.id, y.w FROM " +
				"(SELECT id, SUM(id) OVER () AS w FROM " + rsWinTab + ") y) z) q " +
				"JOIN (SELECT id, SUM(plain) OVER () AS w FROM " + rsWinTab + ") p " +
				"ON p.id = q.id ORDER BY p.id",
			pw: "10000", qw: "10",
		},
		{
			// The control: DISTINCT aliases, nothing contested, right before.
			name: "control, the nested sibling publishes its own alias",
			sql: "SELECT p.w1 AS pw, q.w2 AS qw FROM " +
				"(SELECT id, SUM(plain) OVER () AS w1 FROM " + rsWinTab + ") p JOIN " +
				"(SELECT x.id, x.w2 FROM (SELECT id, SUM(id) OVER () AS w2 FROM " + rsWinTab + ") x) q " +
				"ON p.id = q.id ORDER BY p.id",
			pw: "10000", qw: "10",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if len(res.Rows) != 4 {
					t.Fatalf("%s arm returned %d rows, want 4\n  SQL: %s",
						arm.name, len(res.Rows), tc.sql)
				}
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r["qw"]); got != tc.qw {
						t.Errorf("%s arm row %d: qw = %q, PostgreSQL 17 answers %q — a sibling "+
							"nested in a sibling answered the OUTER one's window\n  SQL: %s",
							arm.name, i, got, tc.qw, tc.sql)
						break
					}
				}
				for i, r := range res.Rows {
					if got := fmt.Sprintf("%v", r["pw"]); got != tc.pw {
						t.Errorf("%s arm row %d: pw = %q, want %q\n  SQL: %s",
							arm.name, i, got, tc.pw, tc.sql)
						break
					}
				}
			}
		})
	}

	// #742's shape: a qualified reference satisfied by ANOTHER arm's
	// identically-named column, with a base table between the two derived
	// ones. Both DAG arms answered z's `a * 3`, which VARIES per row, where
	// the query asks for x's whole-partition window — a per-row value under a
	// window's name is the tell, so every entry reads two rows rather than
	// one.
	//
	// Two mechanisms met here and the shape needed both closed. x's `w` is an
	// expression OVER a window slot, and nothing on the DAG materialized it:
	// the window stage emits `__win_0` and no column called `w`, so the name
	// was free for the other arm to satisfy. And the SELECT-list resolver's
	// qualified→bare fallback dropped the qualifier, so even where both arms
	// publish `w` it took the first arm that answered.
	//
	// The MIRROR is the entry that says the repair is scoped and not merely
	// reordered: `z.w` asked for while `x.w` is present has to answer
	// `a * 3`, which is the value the defect used to hand to `x.w`.
	for _, tc := range []struct {
		name, sql, col string
		row0, row3     string
	}{
		{
			name: "qualified reference across two arms publishing one name",
			sql: "SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " + dbpTable + ") x " +
				"JOIN " + dbpTable + " y ON x.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + dbpTable + ") z ON x.id = z.id ORDER BY x.id",
			col: "w", row0: "52.99", row3: "52.99",
		},
		{
			name: "the mirror: z.w asked for while x.w is present",
			sql: "SELECT x.id, z.w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " + dbpTable + ") x " +
				"JOIN " + dbpTable + " y ON x.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + dbpTable + ") z ON x.id = z.id ORDER BY x.id",
			col: "w", row0: "38.25", row3: "-0.03",
		},
		{
			// BOTH arms projected at once, which is the shape a per-column
			// repair passes and a per-arm one has to get right twice.
			name: "both arms projected",
			sql: "SELECT x.id, x.w AS xw, z.w AS zw FROM " +
				"(SELECT id, SUM(a) OVER () + 0 AS w FROM " + dbpTable + ") x " +
				"JOIN " + dbpTable + " y ON x.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + dbpTable + ") z ON x.id = z.id ORDER BY x.id",
			col: "xw", row0: "52.99", row3: "52.99",
		},
		{
			// A BARE window output beside a computed arm — the same collapse
			// with no wrapping expression anywhere, which is the spelling
			// #755's third repro carries and which answered p's window under
			// BOTH names on every DAG arm. Both arms publish at DECIMAL scale
			// 2 on purpose: a cross-SCALE pair renders one arm at the other's
			// typmod on the single path, which is #754 and not this.
			name: "bare window arm beside a computed arm",
			sql: "SELECT p.id, q.w AS w FROM (SELECT id, SUM(a) OVER () AS w FROM " + dbpTable + ") p " +
				"JOIN " + dbpTable + " y ON p.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + dbpTable + ") q ON p.id = q.id ORDER BY p.id",
			col: "w", row0: "38.25", row3: "-0.03",
		},
		{
			name: "bare window arm beside a computed arm, the window asked for",
			sql: "SELECT p.id, p.w AS w FROM (SELECT id, SUM(a) OVER () AS w FROM " + dbpTable + ") p " +
				"JOIN " + dbpTable + " y ON p.id = y.id " +
				"JOIN (SELECT id, a * 3 AS w FROM " + dbpTable + ") q ON p.id = q.id ORDER BY p.id",
			col: "w", row0: "52.99", row3: "52.99",
		},
		{
			// The CONTROL: one derived arm, no name to collide with. It was
			// right before any of this and says the repair did not move the
			// single-arm resolution.
			name: "control: one arm publishing w",
			sql: "SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " + dbpTable + ") x " +
				"JOIN " + dbpTable + " y ON x.id = y.id ORDER BY x.id",
			col: "w", row0: "52.99", row3: "52.99",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if len(res.Rows) != 9 {
					t.Fatalf("%s arm returned %d rows, want 9\n  SQL: %s",
						arm.name, len(res.Rows), tc.sql)
				}
				first := fmt.Sprintf("%v", res.Rows[0][tc.col])
				fourth := fmt.Sprintf("%v", res.Rows[3][tc.col])
				if first != tc.row0 || fourth != tc.row3 {
					t.Errorf("%s arm answered %s=%q/%q, PostgreSQL 17 answers %q/%q — a "+
						"qualified reference took another arm's identically-named column "+
						"(#742)\n  SQL: %s", arm.name, tc.col, first, fourth,
						tc.row0, tc.row3, tc.sql)
				}
			}
		})
	}

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
			map[string]string{"w1": "49.2400", "w2": "52.99"})
	})
}
