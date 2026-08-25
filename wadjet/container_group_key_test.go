package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget is the embedded-API
// arm of #566/#576: an ARRAY, ROW, MAP or VECTOR column in a GROUP BY position
// — GROUP BY, DISTINCT, COUNT(DISTINCT) — answers the same VALUES with and
// without a per-query memory budget.
//
// This file replaces the #576 pin (TestVectorGroupKeyIsAPinnedFailure), which
// asserted the opposite: those shapes over c_vec FAILED with
//
//	batch: cannot store string into VECTOR vector (#361 silent-write guard)
//
// on a shipped, first-class type, with no memory pressure anywhere. #566 and
// #576 turned out to be ONE defect at one site: a drained partial aggregate
// captured a container group key as its rendered TEXT and handed it to a
// container vector, which refuses text. #566 reached that site through a
// spill; #576 reached it through the morsel-parallel merge, where a clone
// hands its partial to the primary as run FILES in the same format, so an
// ordinary in-memory query took the identical path.
//
// WHAT THIS ARM CAN AND CANNOT GATE. The budget here does NOT force a drain
// for the three row-reader container columns, and saying so is part of the
// gate: a morsel-parallel clone charges a TRACKING-ONLY SpillManager view
// whose ShouldSpillFor is hard-wired false, so an in-process container GROUP
// BY reaches the run format only through the clone-to-primary handoff — which
// only the columnar-readable VECTOR column takes. An earlier version of this
// test claimed to gate all four "under a 1 KiB budget" and measured ZERO
// drain writes for c_arr/c_row/c_rownest/c_map: it compared two in-memory runs
// and would have passed with the whole drain path deleted.
//
// So the drain assertion below is what it can honestly be — that this file as
// a whole exercised the path at least once — and the gates that force a real
// spill for every container type are elsewhere, named here so they cannot be
// deleted without this comment going stale:
//
//   - exec.TestContainerGroupByAcrossASpillMatchesMemory (operator, forced
//     tracker, asserts the external-merge path was entered)
//   - exec.TestNullContainerGroupKeyDoesNotDesyncLaterRows (the NULL key)
//   - worker.TestContainerGroupKeySpillsOnAWorker (stage DAG, real shared
//     budget, asserts the drain counter moved)
func TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget(t *testing.T) {
	ctx := context.Background()
	plain := cgkOpen(t, 0)
	budgeted := cgkOpen(t, 1024)
	n := typematrix.Nested

	drainsBefore := exec.ContainerKeyDrainWrites.Load()

	for _, col := range []string{"c_arr", "c_row", "c_rownest", "c_map", "c_vec"} {
		for _, shape := range []struct{ name, sql string }{
			{"distinct", `SELECT DISTINCT %[1]s AS v FROM %[2]s`},
			{"group_by", `SELECT %[1]s AS k, COUNT(*) AS n, SUM(id) AS s FROM %[2]s GROUP BY %[1]s`},
			{"count_distinct", `SELECT COUNT(DISTINCT %[1]s) AS n FROM %[2]s`},
			{"group_by_two_cols", `SELECT g, %[1]s AS k, COUNT(*) AS n FROM %[2]s GROUP BY g, %[1]s`},
			// A LEADING group column moves a NULL container key out of last
			// place in the merge order, which is where a null that fails to
			// advance its own offsets corrupts the row after it.
			{"group_by_leading_id", `SELECT id %% 7 AS m, %[1]s AS k, COUNT(*) AS n FROM %[2]s GROUP BY id %% 7, %[1]s`},
		} {
			t.Run(col+"_"+shape.name, func(t *testing.T) {
				sql := fmt.Sprintf(shape.sql, col, n)
				want, err := tmRun(ctx, plain, sql)
				if err != nil {
					t.Fatalf("unbudgeted: %v\n  SQL: %s", err, sql)
				}
				if len(want.Rows) == 0 {
					t.Fatalf("unbudgeted run returned no rows — the gate would compare nothing\n  SQL: %s", sql)
				}
				got, err := tmRun(ctx, budgeted, sql)
				if err != nil {
					t.Fatalf("under a 1 KiB budget: %v\n  SQL: %s", err, sql)
				}
				w, g := cgkRows(want.Columns, want.Rows), cgkRows(got.Columns, got.Rows)
				if len(w) != len(g) {
					t.Fatalf("%d rows under a budget, %d without one\n  SQL: %s", len(g), len(w), sql)
				}
				for i := range w {
					if w[i] != g[i] {
						t.Fatalf("row %d differs\n  budgeted:   %s\n  unbudgeted: %s\n  SQL: %s",
							i, g[i], w[i], sql)
					}
				}
			})
		}
	}

	if drains := exec.ContainerKeyDrainWrites.Load() - drainsBefore; drains == 0 {
		t.Errorf("not one container group key reached a partial-state run in this whole file — " +
			"every comparison above was between two in-memory runs, which is the vacuous shape " +
			"this gate was rewritten to stop. Find out what stopped taking the drain path.")
	}
}

// TestContainerKeyEncodingPathsStillAnswer keeps the half of the deleted #576
// pin that was never broken: the consumers that key a container without
// materializing it back into an output column.
//
// They are what LOCALIZED that defect — a join matched on c_vec, a UNION
// deduped on it and an ORDER BY sorted it, all while GROUP BY died — so a
// change that breaks one of them is a regression in the opposite direction,
// and typematrix.Corpus does not reach a container through a join or a set
// operation. Each entry asserts the ANSWER, derived from a second query, not
// merely that some rows came back: `SELECT COUNT(*) ... JOIN` returns one row
// whatever the count is, so a row count proves nothing about it.
func TestContainerKeyEncodingPathsStillAnswer(t *testing.T) {
	ctx := context.Background()
	db := cgkOpen(t, 0)
	n := typematrix.Nested

	for _, col := range []string{"c_arr", "c_row", "c_map", "c_vec"} {
		t.Run(col, func(t *testing.T) {
			// The reference: one row per distinct value, with its
			// multiplicity. Everything below is derived from it.
			ref, err := tmRun(ctx, db, fmt.Sprintf(`SELECT %s AS v, COUNT(*) AS n FROM %s GROUP BY %s`, col, n, col))
			if err != nil {
				t.Fatalf("reference GROUP BY: %v", err)
			}
			var wantSelfJoin int64
			distinct := make([]string, 0, len(ref.Rows))
			for _, r := range ref.Rows {
				c, _ := r["n"].(int64)
				distinct = append(distinct, fmt.Sprintf("v=%v", r["v"]))
				if r["v"] != nil {
					// NULL never equals NULL in a join predicate, so a NULL
					// group contributes nothing.
					wantSelfJoin += c * c
				}
			}
			sort.Strings(distinct)

			// UNION of a table with itself is its DISTINCT — same members,
			// same count, one row each.
			u, err := tmRun(ctx, db, fmt.Sprintf(`SELECT %s AS v FROM %s UNION SELECT %s FROM %s`, col, n, col, n))
			if err != nil {
				t.Fatalf("UNION: %v", err)
			}
			gotUnion := make([]string, 0, len(u.Rows))
			for _, r := range u.Rows {
				gotUnion = append(gotUnion, fmt.Sprintf("v=%v", r["v"]))
			}
			sort.Strings(gotUnion)
			if len(gotUnion) != len(distinct) {
				t.Fatalf("UNION returned %d members, GROUP BY says %d distinct values", len(gotUnion), len(distinct))
			}
			for i := range distinct {
				if gotUnion[i] != distinct[i] {
					t.Fatalf("UNION member %d is %s, GROUP BY says %s", i, gotUnion[i], distinct[i])
				}
			}

			// An equi-self-join on the container matches every pair sharing a
			// value: Σ n² over the non-NULL groups.
			j, err := tmRun(ctx, db, fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.%s = b.%s`, n, n, col, col))
			if err != nil {
				t.Fatalf("self join: %v", err)
			}
			if len(j.Rows) != 1 {
				t.Fatalf("COUNT(*) returned %d rows", len(j.Rows))
			}
			if got, _ := j.Rows[0]["n"].(int64); got != wantSelfJoin {
				t.Fatalf("self join on %s matched %d pairs, want %d (sum of n^2 over the non-NULL groups)",
					col, got, wantSelfJoin)
			}

			// ORDER BY must be a permutation of the column, and monotone
			// under the same comparator the engine sorts by — asserted as
			// "the ordered projection holds every value the table holds".
			o, err := tmRun(ctx, db, fmt.Sprintf(`SELECT %s AS v FROM %s ORDER BY %s`, col, n, col))
			if err != nil {
				t.Fatalf("ORDER BY: %v", err)
			}
			if len(o.Rows) != typematrix.Rows {
				t.Fatalf("ORDER BY returned %d rows, want the table's %d", len(o.Rows), typematrix.Rows)
			}
			counts := map[string]int{}
			for _, r := range o.Rows {
				counts[fmt.Sprintf("v=%v", r["v"])]++
			}
			for _, r := range ref.Rows {
				c, _ := r["n"].(int64)
				if got := counts[fmt.Sprintf("v=%v", r["v"])]; int64(got) != c {
					t.Fatalf("ORDER BY holds %d copies of %v, GROUP BY says %d", got, r["v"], c)
				}
			}
		})
	}
}

// cgkOpen loads the nested type-matrix table into an embedded DB, optionally
// under a per-query memory budget.
func cgkOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := typematrix.NestedSchema()
	if err := db.CreateTable(ctx, typematrix.Nested, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Nested, err)
	}
	ing := db.NewIngester(typematrix.Nested, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, typematrix.NestedData(typematrix.Rows)); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Nested, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Nested, err)
	}
	return db
}

// cgkRows renders a result to sorted, comparable text. A container value is a
// []any / map[string]any / []float32 tree — not comparable with ==, not
// orderable on its own — and fmt sorts a map's keys, so the same tree always
// renders the same way. Sorting drops row order, which none of these queries
// constrains.
func cgkRows(columns []string, rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s := ""
		for _, c := range columns {
			s += fmt.Sprintf("%s=%v|", c, r[c])
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
