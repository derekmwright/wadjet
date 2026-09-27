// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// SET-OPERATION PRECEDENCE ON FIVE ARMS (#1349).
//
// PostgreSQL's grammar gives INTERSECT [ALL] higher precedence than UNION
// [ALL] and EXCEPT [ALL], left-associative within a level, parentheses
// overriding. The parser folded every operator into its left operand as it
// read it, so `A UNION B INTERSECT C` answered `(A UNION B) INTERSECT C` on
// every arm. The tree is the one place the precedence lives; the logical
// builder, the single-process planner and the distributed set-op stages read
// it as it stands, so this gate asserts the ANSWER on all five.
//
// The cells (arc_sp_setop_precedence_cells_test.go) are generated from
// PostgreSQL 17.11 over the fixture below: every ordered pair of the six
// operators between three arms, unparenthesised, left- and right-
// parenthesised, on two fixtures; a sample of four- and five-arm chains; and
// every three-arm chain again under a chain-level ORDER BY / LIMIT / OFFSET,
// in a derived table, in a CTE, in an IN body and as a scalar subquery. The
// fixtures carry duplicates on overlapping integer sets, so DISTINCT and ALL
// answer different multisets and — checked by the generator, which refuses a
// fixture that does not — every chain whose tree differs between the two
// readings answers a different row multiset (or a different ORDER BY / LIMIT
// window) under each. Each cell carries the LEFT-TO-RIGHT reading's answer
// too, so a failure says which reading an arm took.
//
// The full sorted row multiset is compared, never a count alone: a set
// operation's output order is unspecified, so an unordered cell sorts both
// sides; an ORDER BY cell compares the rows as sent.

type spCell struct {
	group, name, sql  string
	ordered           bool
	want, leftToRight string
}

func spSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}}
}

func spRows(vals []int64) []map[string]any {
	out := make([]map[string]any, len(vals))
	for i, v := range vals {
		out[i] = map[string]any{"x": v}
	}
	return out
}

// spTables is every fixture relation: sp1_a … sp2_e and sp_r, the outer
// relation of the IN cells.
func spTables() []tmdTable {
	var out []tmdTable
	for _, fx := range []string{"sp1", "sp2"} {
		for _, n := range []string{"a", "b", "c", "d", "e"} {
			out = append(out, tmdTable{fx + "_" + n, spSchema(), spRows(spFixtures[fx][n])})
		}
	}
	return append(out, tmdTable{"sp_r", spSchema(), spRows(spRange)})
}

// spStandalone is one embedded engine over the SP fixture, at the given memory
// budget (0 = none), each relation in row groups of three.
func spStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range spTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 3,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// spArms is the five arms over the SP fixture alone, which stays out of the
// shared corpus so no other budgeted gate loses headroom to it.
func spArms(t *testing.T, ctx context.Context) []c1Arm {
	t.Helper()
	single := spStandalone(t, ctx, 0)
	spilled := spStandalone(t, ctx, 512*1024)
	stand := func(opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, spTables())
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand()
	coordB := stand(func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infraM, nil, spTables())
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })
	return []c1Arm{
		{"single", func(s string) (string, error) { return f1RenderSingle(ctx, single, s) }, nil},
		{"spilled512k", func(s string) (string, error) { return f1RenderSingle(ctx, spilled, s) }, nil},
		{"dag", func(s string) (string, error) { return f1RenderDAG(ctx, coord, s) }, coord},
		{"dag-shuffled", func(s string) (string, error) { return f1RenderDAG(ctx, coordB, s) }, coordB},
		{"dag-morsel4", func(s string) (string, error) { return f1RenderDAG(ctx, coordM, s) }, coordM},
	}
}

// spSortRows sorts a rendered answer's rows, keeping the header first.
func spSortRows(rendered string) string {
	parts := strings.Split(rendered, " | ")
	if len(parts) > 2 {
		sort.Strings(parts[1:])
	}
	return strings.Join(parts, " | ")
}

func TestArcSPSetOpChainsTakePostgresPrecedenceOnFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := spArms(t, ctx)

	// How many cells each DAG arm answered in-process rather than through
	// its stages, logged so a reader can see the DAG was exercised.
	local := map[string]int{}
	for _, tc := range spCells {
		t.Run(tc.group+"/"+tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				got, err := arm.run(tc.sql)
				if err != nil {
					got = "ERR " + err.Error()
				} else if !tc.ordered {
					got = spSortRows(got)
				}
				if arm.coord != nil && c1RouteDelta(before, a2fReadRoutes(arm.coord)) != "" {
					local[arm.name]++
				}
				if got == tc.want {
					continue
				}
				reading := ""
				if got == tc.leftToRight {
					reading = "\n  (the LEFT-TO-RIGHT reading's answer)"
				}
				t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s", tc.sql, arm.name, got, tc.want, reading)
			}
		})
	}
	t.Logf("%d cells; DAG cells answered in-process: %v", len(spCells), local)
}
