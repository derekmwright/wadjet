package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #569's gate: windowed MIN/MAX over all 22 types, end to end from SQL.
//
// `MIN(c) OVER (…)` failed the query outright — Vector.SetValue's #361 guard,
// "cannot store string into FLOAT64 vector" — for twelve of the twenty-two
// types, while `MIN(c)` over the identical column answered correctly. The
// window declared FLOAT64 for anything outside a ten-type allow-list and then
// tried to write the chosen value into it.
//
// The reference is the engine's own GROUPED aggregate over the same
// partition, which is the property the fix claims: `MIN(c) OVER (PARTITION BY
// g …)` covering the whole partition and `MIN(c) … GROUP BY g` are the same
// question asked twice, and until this fix one of the two spellings did not
// answer at all. Using the aggregate rather than a hardcoded per-type
// expectation is deliberate — TestMinMaxEveryType already pins the aggregate
// against the engine's ORDER BY and (for the byte-backed types) against
// literal values from the fixture generator, so this gate inherits a
// reference that is itself anchored.
//
// This file is the IN-MEMORY, end-to-end-SQL arm. The two SPILLED evaluators —
// the partition-at-a-time walker over sorted runs and the empty-PARTITION-BY
// streamer, whose running extreme is a BOX compared through newBoxedCompare —
// are gated in exec.TestWindowExternalMinMaxEveryType and
// exec.TestWindowGlobalMinMaxEveryType over the same 22 types.
//
// They live there rather than here on purpose. A wadjet.Config MemoryBudget
// small enough to force the window to spill also forces the SCAN past its
// budget, and the never-OOM model makes each such reservation wait for relief
// before forcing it — about two seconds per query, which over 22 types x four
// shapes is five minutes of a unit-test suite spent sleeping. Worse, nothing
// the embedded API returns says whether the window actually spilled, so the
// arm could silently degrade into a second in-memory run and still pass. The
// exec-level harness has both properties this one cannot: it FAILS if no run
// file was written, and it compares the spilled answer against the in-memory
// one directly rather than each against a reference.

// wmmBudgets is the arm table. One entry today — see the file comment for why
// the spilled arm lives in package exec — kept as a table because the row
// range and the budget are the two knobs a second arm would set.
var wmmBudgets = []struct {
	name   string
	budget int64
	// rows is the id range the arm reads; 0 means the whole fixture.
	rows int
}{
	{"in_memory", 0, 0},
}

// wmmWhere ANDs an arm's row range onto a query's own predicate. An empty
// extra predicate yields the arm's range alone, and an unlimited arm with no
// predicate yields no WHERE clause at all.
func wmmWhere(rows int, extra string) string {
	var terms []string
	if rows > 0 {
		terms = append(terms, fmt.Sprintf("id < %d", rows))
	}
	if extra != "" {
		terms = append(terms, extra)
	}
	if len(terms) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(terms, " AND ")
}

// wmmRowCount is how many fixture rows an arm's range selects.
func wmmRowCount(rows int) int {
	if rows <= 0 || rows > mbRows {
		return mbRows
	}
	return rows
}

// wmmPartitionRef reads the grouped aggregate's MIN/MAX per group.
func wmmPartitionRef(t *testing.T, db *DB, col, where string) map[int32][2]any {
	t.Helper()
	res, err := db.Query(context.Background(), fmt.Sprintf(
		"SELECT g, MIN(%s) AS lo, MAX(%s) AS hi FROM mbtypes%s GROUP BY g", col, col, where))
	if err != nil {
		t.Fatalf("grouped reference (%s): %v", col, err)
	}
	ref := make(map[int32][2]any, len(res.Rows))
	for _, r := range res.Rows {
		g, ok := r["g"].(int32)
		if !ok {
			t.Fatalf("grouped reference: g boxed %T, want int32", r["g"])
		}
		ref[g] = [2]any{r["lo"], r["hi"]}
	}
	if len(ref) == 0 {
		t.Fatalf("grouped reference (%s) produced no groups", col)
	}
	return ref
}

// wmmAssertDeclaration checks the window column's declared type against the
// INPUT column's, which is the whole of #569's type half: MIN/MAX return one
// of their input's values untouched, so nothing else can hold the answer. A
// parameterized type carries its parameters too — a DECIMAL declared without
// its scale re-parses the formatted box against scale 0 and reads back a
// different NUMBER (#406's quiet half).
func wmmAssertDeclaration(t *testing.T, metas []ColumnMeta, in parquet.Column, cols ...string) {
	t.Helper()
	mbAssertTypes(t, metas, in.Type, cols...)
	if in.Type != parquet.TypeDecimal {
		return
	}
	for _, m := range metas {
		for _, want := range cols {
			if m.Name != want {
				continue
			}
			if m.Precision != in.Precision || m.Scale != in.Scale {
				t.Errorf("column %q declared DECIMAL(%d,%d), want the input's own (%d,%d)",
					m.Name, m.Precision, m.Scale, in.Precision, in.Scale)
			}
		}
	}
}

func TestWindowedMinMaxEveryType(t *testing.T) {
	ctx := context.Background()
	for _, arm := range wmmBudgets {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			db := mbOpenBudget(t, arm.budget)
			where := wmmWhere(arm.rows, "")
			wantRows := wmmRowCount(arm.rows)
			for _, col := range mbTypeCols() {
				col := col
				t.Run(col.Name, func(t *testing.T) {
					ref := wmmPartitionRef(t, db, col.Name, where)

					// The whole partition as an explicit ROWS frame: the
					// same rows the GROUP BY sees, so the two answers must
					// be identical value for value.
					full, err := db.Query(ctx, fmt.Sprintf(
						`SELECT id, g, MIN(%s) OVER (PARTITION BY g ORDER BY id `+
							`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo, `+
							`MAX(%s) OVER (PARTITION BY g ORDER BY id `+
							`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi `+
							`FROM mbtypes%s ORDER BY id`, col.Name, col.Name, where))
					if err != nil {
						t.Fatalf("windowed MIN/MAX over the whole partition: %v", err)
					}
					if len(full.Rows) != wantRows {
						t.Fatalf("whole-partition window returned %d rows, want %d", len(full.Rows), wantRows)
					}
					wmmAssertDeclaration(t, full.ColumnMetas, col, "lo", "hi")
					for _, r := range full.Rows {
						g := r["g"].(int32)
						want := ref[g]
						// mmWiden lifts the window's value into the shape
						// the grouped aggregate declares. It converts only
						// INT32→INT64 and FLOAT32→FLOAT64, which are the two
						// types whose ACCUMULATOR widens; every other type
						// must match exactly, DECIMAL as the same text.
						mbAssertEqual(t, fmt.Sprintf("id %v group %d lo", r["id"], g),
							mmWiden(t, col.Type, r["lo"]), want[0])
						mbAssertEqual(t, fmt.Sprintf("id %v group %d hi", r["id"], g),
							mmWiden(t, col.Type, r["hi"]), want[1])
					}

					// The running frame's LAST row per partition sees the
					// whole partition too, so it has the same reference —
					// and it reaches it through the deque's incremental
					// advance rather than one whole-frame pass.
					running, err := db.Query(ctx, fmt.Sprintf(
						`SELECT id, g, MIN(%s) OVER (PARTITION BY g ORDER BY id) AS lo, `+
							`MAX(%s) OVER (PARTITION BY g ORDER BY id) AS hi `+
							`FROM mbtypes%s ORDER BY id`, col.Name, col.Name, where))
					if err != nil {
						t.Fatalf("windowed MIN/MAX over a running frame: %v", err)
					}
					wmmAssertDeclaration(t, running.ColumnMetas, col, "lo", "hi")
					last := make(map[int32]map[string]any, len(ref))
					for _, r := range running.Rows {
						last[r["g"].(int32)] = r
					}
					for g, r := range last {
						mbAssertEqual(t, fmt.Sprintf("running group %d lo at the last row", g),
							mmWiden(t, col.Type, r["lo"]), ref[g][0])
						mbAssertEqual(t, fmt.Sprintf("running group %d hi at the last row", g),
							mmWiden(t, col.Type, r["hi"]), ref[g][1])
					}

					// A CURRENT ROW frame is the identity: the answer is the
					// row's own value, whatever the comparator does. It
					// gates the compare-and-copy ROUND TRIP on its own —
					// GetValue's box written back through SetValue into a
					// vector of the input's type — with no reference query
					// and no ordering involved.
					self, err := db.Query(ctx, fmt.Sprintf(
						`SELECT id, %s AS v, MIN(%s) OVER (PARTITION BY g ORDER BY id `+
							`ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS lo, `+
							`MAX(%s) OVER (PARTITION BY g ORDER BY id `+
							`ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS hi `+
							`FROM mbtypes%s ORDER BY id`,
						col.Name, col.Name, col.Name, wmmWhere(arm.rows, "id < 400")))
					if err != nil {
						t.Fatalf("windowed MIN/MAX over a CURRENT ROW frame: %v", err)
					}
					wmmAssertDeclaration(t, self.ColumnMetas, col, "lo", "hi")
					for _, r := range self.Rows {
						mbAssertEqual(t, fmt.Sprintf("id %v self-frame lo", r["id"]), r["lo"], r["v"])
						mbAssertEqual(t, fmt.Sprintf("id %v self-frame hi", r["id"]), r["hi"], r["v"])
					}
				})
			}
		})
	}
}

// TestWindowedMinMaxOverEverythingEveryType is the empty-PARTITION-BY half.
//
// `OVER ()` takes a different evaluator: past the memory budget the window
// streams the whole input as one partition (window_global.go) and carries its
// running extreme as a BOX across batches, so the comparison is
// newBoxedCompare's rather than the columnar one — the site where an IPV4 or
// IPV6 column's display text is not its address order.
func TestWindowedMinMaxOverEverythingEveryType(t *testing.T) {
	ctx := context.Background()
	for _, arm := range wmmBudgets {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			db := mbOpenBudget(t, arm.budget)
			where := wmmWhere(arm.rows, "")
			wantRows := wmmRowCount(arm.rows)
			for _, col := range mbTypeCols() {
				col := col
				t.Run(col.Name, func(t *testing.T) {
					scalar, err := db.Query(ctx, fmt.Sprintf(
						"SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM mbtypes%s", col.Name, col.Name, where))
					if err != nil {
						t.Fatalf("scalar reference: %v", err)
					}
					wantLo, wantHi := scalar.Rows[0]["lo"], scalar.Rows[0]["hi"]

					res, err := db.Query(ctx, fmt.Sprintf(
						"SELECT id, MIN(%s) OVER () AS lo, MAX(%s) OVER () AS hi "+
							"FROM mbtypes%s ORDER BY id", col.Name, col.Name, where))
					if err != nil {
						t.Fatalf("MIN/MAX OVER (): %v", err)
					}
					if len(res.Rows) != wantRows {
						t.Fatalf("OVER () returned %d rows, want %d", len(res.Rows), wantRows)
					}
					wmmAssertDeclaration(t, res.ColumnMetas, col, "lo", "hi")
					for _, r := range res.Rows {
						mbAssertEqual(t, fmt.Sprintf("id %v OVER () lo", r["id"]),
							mmWiden(t, col.Type, r["lo"]), wantLo)
						mbAssertEqual(t, fmt.Sprintf("id %v OVER () hi", r["id"]),
							mmWiden(t, col.Type, r["hi"]), wantHi)
					}
				})
			}
		})
	}
}

// TestWindowedMinMaxIgnoresNullsPerPostgres pins PostgreSQL's NULL rule at
// both ends: a NULL contributes nothing to the extreme, and a partition whose
// every value is NULL answers NULL rather than a zero.
//
// The filter builds exactly that shape out of the shared fixture without a
// per-type table: group 0 keeps only its NULL rows for this column, group 1
// keeps a slice of ordinary ones. Group 0's window value must be NULL for
// every one of its rows, and group 1's must equal the aggregate over the same
// filtered rows — so the entry is not merely "NULL somewhere" but the two
// halves side by side in one query.
func TestWindowedMinMaxIgnoresNullsPerPostgres(t *testing.T) {
	ctx := context.Background()
	for _, arm := range wmmBudgets {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			db := mbOpenBudget(t, arm.budget)
			for _, col := range mbTypeCols() {
				col := col
				t.Run(col.Name, func(t *testing.T) {
					where := wmmWhere(arm.rows,
						fmt.Sprintf("((g = 0 AND %s IS NULL) OR (g = 1 AND id < 900))", col.Name))
					ref, err := db.Query(ctx, fmt.Sprintf(
						"SELECT g, MIN(%s) AS lo, MAX(%s) AS hi FROM mbtypes%s GROUP BY g",
						col.Name, col.Name, where))
					if err != nil {
						t.Fatalf("grouped reference: %v", err)
					}
					want := make(map[int32][2]any, 2)
					for _, r := range ref.Rows {
						want[r["g"].(int32)] = [2]any{r["lo"], r["hi"]}
					}
					if _, ok := want[0]; !ok {
						t.Fatalf("the all-NULL group is missing from the reference — "+
							"the filter selected no g=0 row with a NULL %s", col.Name)
					}
					if v := want[0]; v[0] != nil || v[1] != nil {
						t.Fatalf("the grouped aggregate over an all-NULL group answered (%#v, %#v), "+
							"want NULL — the reference itself is wrong", v[0], v[1])
					}

					res, err := db.Query(ctx, fmt.Sprintf(
						"SELECT id, g, MIN(%s) OVER (PARTITION BY g) AS lo, "+
							"MAX(%s) OVER (PARTITION BY g) AS hi FROM mbtypes%s ORDER BY id",
						col.Name, col.Name, where))
					if err != nil {
						t.Fatalf("windowed MIN/MAX with an all-NULL partition: %v", err)
					}
					wmmAssertDeclaration(t, res.ColumnMetas, col, "lo", "hi")
					var sawNullGroup, sawValueGroup int
					for _, r := range res.Rows {
						g := r["g"].(int32)
						if g == 0 {
							sawNullGroup++
						} else {
							sawValueGroup++
						}
						mbAssertEqual(t, fmt.Sprintf("id %v group %d lo", r["id"], g),
							mmWiden(t, col.Type, r["lo"]), want[g][0])
						mbAssertEqual(t, fmt.Sprintf("id %v group %d hi", r["id"], g),
							mmWiden(t, col.Type, r["hi"]), want[g][1])
					}
					if sawNullGroup == 0 || sawValueGroup == 0 {
						t.Fatalf("the window saw %d all-NULL rows and %d valued rows — "+
							"both halves must be present or the entry proves nothing",
							sawNullGroup, sawValueGroup)
					}
				})
			}
		})
	}
}
