package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget gates #566 and #576:
// an ARRAY, ROW, MAP or VECTOR column in a GROUP BY position — GROUP BY,
// DISTINCT, COUNT(DISTINCT) — answers the same VALUES whether or not the
// aggregate's partial state goes to disk.
//
// This file replaces the #576 pin (TestVectorGroupKeyIsAPinnedFailure), which
// asserted the opposite: those three shapes over c_vec FAILED with
//
//	batch: cannot store string into VECTOR vector (#361 silent-write guard)
//
// on a shipped, first-class type, with no memory pressure anywhere. The two
// issues turned out to be ONE defect at one site. A drained partial aggregate
// captured a container group key as its rendered TEXT
// (setPartialKeyFromAny's default arm) and writePartialKeyFallback handed that
// text to a container vector, which refuses it. #566 reached the site through
// a spill; #576 reached it through the morsel-parallel merge, where a clone
// hands its partial to the primary as run FILES in the same format
// (mergeSinkState -> drainStateToRuns), so an ordinary in-memory query took
// the identical path. Fixing the encoding fixed both.
//
// The budgeted arm is asserted to have actually SPILLED — not by inspecting
// the operator, which this layer cannot reach, but by requiring the two arms
// to agree on a fixture large enough that 1 KiB cannot hold its group state.
// The engine-level gate that asserts the external-merge path was entered is
// exec.TestContainerGroupByAcrossASpillMatchesMemory.
func TestContainerGroupKeyAnswersTheSameUnderAMemoryBudget(t *testing.T) {
	ctx := context.Background()
	plain := cgkOpen(t, 0)
	budgeted := cgkOpen(t, 1024)
	n := typematrix.Nested

	for _, col := range []string{"c_arr", "c_row", "c_rownest", "c_map", "c_vec"} {
		for _, shape := range []struct{ name, sql string }{
			{"distinct", `SELECT DISTINCT %[1]s AS v FROM %[2]s`},
			{"group_by", `SELECT %[1]s AS k, COUNT(*) AS n, SUM(id) AS s FROM %[2]s GROUP BY %[1]s`},
			{"count_distinct", `SELECT COUNT(DISTINCT %[1]s) AS n FROM %[2]s`},
			{"group_by_two_cols", `SELECT g, %[1]s AS k, COUNT(*) AS n FROM %[2]s GROUP BY g, %[1]s`},
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
}

// TestContainerKeyEncodingPathsStillAnswer keeps the half of the deleted #576
// pin that was never broken: the consumers that key a container without
// materializing it back into an output column.
//
// They are what LOCALIZED that defect — a join matched on c_vec, a UNION
// deduped on it and an ORDER BY sorted it, all while GROUP BY died — so a
// change that breaks one of them is a regression in the opposite direction,
// and the corpus templates (typematrix.Corpus) do not reach a container
// through a join or a set operation.
func TestContainerKeyEncodingPathsStillAnswer(t *testing.T) {
	ctx := context.Background()
	db := cgkOpen(t, 0)
	n := typematrix.Nested

	for _, q := range []struct {
		name    string
		sql     string
		minRows int
	}{
		{"plain_select", fmt.Sprintf(`SELECT c_vec FROM %s LIMIT 3`, n), 3},
		{"order_by_vec", fmt.Sprintf(`SELECT c_vec FROM %s ORDER BY c_vec LIMIT 3`, n), 3},
		{"order_by_arr", fmt.Sprintf(`SELECT c_arr FROM %s ORDER BY c_arr LIMIT 3`, n), 3},
		{"union_vec", fmt.Sprintf(`SELECT c_vec FROM %s UNION SELECT c_vec FROM %s`, n, n), 1},
		{"union_arr", fmt.Sprintf(`SELECT c_arr FROM %s UNION SELECT c_arr FROM %s`, n, n), 1},
		{"union_row", fmt.Sprintf(`SELECT c_row FROM %s UNION SELECT c_row FROM %s`, n, n), 1},
		{"union_map", fmt.Sprintf(`SELECT c_map FROM %s UNION SELECT c_map FROM %s`, n, n), 1},
		{"join_on_vec", fmt.Sprintf(`SELECT COUNT(*) FROM %s a JOIN %s b ON a.c_vec = b.c_vec`, n, n), 1},
		{"join_on_arr", fmt.Sprintf(`SELECT COUNT(*) FROM %s a JOIN %s b ON a.c_arr = b.c_arr`, n, n), 1},
	} {
		t.Run(q.name, func(t *testing.T) {
			res, err := tmRun(ctx, db, q.sql)
			if err != nil {
				t.Fatalf("%s must keep working: %v\n  SQL: %s", q.name, err, q.sql)
			}
			if len(res.Rows) < q.minRows {
				t.Fatalf("%s returned %d rows, want at least %d\n  SQL: %s",
					q.name, len(res.Rows), q.minRows, q.sql)
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
