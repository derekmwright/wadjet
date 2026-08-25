package wadjet

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #585: a window's PARTITION BY / ORDER BY term that is QUALIFIED or an
// EXPRESSION was silently dropped, and the window degraded to ONE partition.
//
// `PARTITION BY p.g` missed the batch's bare `g`; `PARTITION BY id % 3` named
// a column nothing had computed. Both resolved to -1 through
// RecordBatch.ColumnIndex's exact-name match and were SKIPPED, so
// ROW_NUMBER() numbered straight through every group, SUM OVER returned the
// whole-table sum, and MIN OVER returned the whole-table minimum — with no
// error, on every function, for the most ordinary spelling a BI client emits.
//
// The reference is the engine's OWN answer to the same window written with a
// bare column key, which is the shape that always worked. That is the
// property the fix claims — a qualified reference and an expression name a
// value, and a window keyed on that value partitions by it — and it needs no
// per-function expected numbers hardcoded here, which is what let the
// original defect hide: every wrong answer LOOKED like a right one.
//
// The distributed half is internal/coordinator's
// TestWindowPartitionKeyTwoPath: the DAG computes an expression key in the
// WORKER, from the stage spec, which is a second implementation of this.

const wpkRows = 4000

// wpkOpen builds a fixture whose grouping columns are all derivable from id,
// so a query can spell the same partition three ways — bare, qualified, and
// computed — and demand one answer.
func wpkOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := mbSchema()
	if err := db.CreateTable(ctx, "mbtypes", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Row groups deliberately smaller than the fixture, so the window's input
	// arrives as MANY batches. A pre-window projection that reuses one
	// computed vector across batches reproduces #585's own symptom on the
	// retained batches, and a single-batch fixture cannot see it.
	ing := db.NewIngester("mbtypes", schema, nil, ingest.Config{
		MaxBufferRows: wpkRows + 1, RowGroupSize: 700,
	})
	if err := ing.Ingest(ctx, mbData(wpkRows)); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// wpkFuncs is every window function family, each written against a column the
// fixture carries. `%s` is the OVER clause's body.
var wpkFuncs = []struct {
	name string
	proj string
	// flatWithoutOrder marks a function whose answer is legitimately the
	// same in every row when the OVER clause has no ORDER BY: RANK and
	// DENSE_RANK are 1 for every row of every partition there. The
	// one-partition check below cannot say anything about those, and
	// asserting it anyway would fail a correct engine.
	flatWithoutOrder bool
}{
	{name: "row_number", proj: "ROW_NUMBER() OVER (%s)"},
	{name: "rank", proj: "RANK() OVER (%s)", flatWithoutOrder: true},
	{name: "dense_rank", proj: "DENSE_RANK() OVER (%s)", flatWithoutOrder: true},
	{name: "sum", proj: "SUM(c_i64) OVER (%s)"},
	{name: "count", proj: "COUNT(*) OVER (%s)"},
	{name: "min", proj: "MIN(c_i64) OVER (%s)"},
	{name: "lag", proj: "LAG(id) OVER (%s)"},
	{name: "lead", proj: "LEAD(id) OVER (%s)"},
	{name: "first_value", proj: "FIRST_VALUE(id) OVER (%s)"},
}

// TestWindowKeySpellingsAgree runs every function over three spellings of one
// partition — bare `g`, qualified `p.g`, computed `id % 3` (the fixture's own
// definition of g) — and requires all three to answer identically, with and
// without an ORDER BY inside the OVER.
func TestWindowKeySpellingsAgree(t *testing.T) {
	ctx := context.Background()
	db := wpkOpen(t, 0)

	for _, ord := range []struct {
		name    string
		suffix  string
		ordered bool
	}{
		{"no_order_by", "", false},
		{"with_order_by", " ORDER BY id", true},
		{"desc_nulls_first", " ORDER BY c_i64 DESC NULLS FIRST, id", true},
	} {
		ord := ord
		t.Run(ord.name, func(t *testing.T) {
			for _, fn := range wpkFuncs {
				fn := fn
				t.Run(fn.name, func(t *testing.T) {
					bare := wpkQuery(fn.proj, "PARTITION BY g"+ord.suffix)
					want := wpkRun(t, ctx, db, bare)
					if ord.ordered || !fn.flatWithoutOrder {
						wpkAssertPartitioned(t, fn.name, want)
					}
					for _, spelling := range []string{
						"PARTITION BY p.g" + ord.suffix,
						"PARTITION BY id % 3" + ord.suffix,
						"PARTITION BY CAST(id % 3 AS BIGINT)" + ord.suffix,
					} {
						got := wpkRun(t, ctx, db, wpkQuery(fn.proj, spelling))
						wpkAssertSame(t, spelling, got, want)
					}
				})
			}
		})
	}
}

// wpkQuery writes one window query. Every one is aliased `p` so a qualified
// spelling is legal, and ordered by id so the comparison is positional.
func wpkQuery(proj, over string) string {
	// 301 rows, so the three partitions are 101/100/100: an EQUAL split would
	// make COUNT(*) OVER constant for a correct engine too, and the
	// one-partition check below could not tell the two apart.
	return fmt.Sprintf("SELECT p.id, %s AS w FROM mbtypes p WHERE p.id < 301 ORDER BY p.id",
		fmt.Sprintf(proj, over))
}

// TestWindowQualifiedArgumentAfterJoin covers the sibling symptom #585's notes
// name: a QUALIFIED reference in the window's ARGUMENT resolved to no input
// vector, so the output column came back NULL in every row rather than wrong.
func TestWindowQualifiedArgumentAfterJoin(t *testing.T) {
	ctx := context.Background()
	db := wpkOpen(t, 0)

	bare := wpkRun(t, ctx, db,
		`SELECT p.id, MIN(c_i64) OVER (PARTITION BY g) AS w FROM mbtypes p WHERE p.id < 40 ORDER BY p.id`)
	qualified := wpkRun(t, ctx, db,
		`SELECT p.id, MIN(p.c_i64) OVER (PARTITION BY p.g) AS w FROM mbtypes p WHERE p.id < 40 ORDER BY p.id`)
	wpkAssertSame(t, "qualified argument", qualified, bare)
	for _, r := range bare {
		if r["w"] == nil {
			t.Fatalf("the fixture's own reference answers NULL, so this test proves nothing: %v", r)
		}
	}
}

// TestWindowNullPartitionKey pins PostgreSQL's rule that every NULL key forms
// ONE partition, through a computed key — the route where a NULL is produced
// by the pre-window projection rather than read out of the column.
func TestWindowNullPartitionKey(t *testing.T) {
	ctx := context.Background()
	db := wpkOpen(t, 0)

	// c_bool nulls on its own stride (every 23rd row). NULLIF turns the
	// remaining `false` into a second NULL source, so the computed key has a
	// null the stored column does not.
	rows := wpkRun(t, ctx, db,
		`SELECT p.id, COUNT(*) OVER (PARTITION BY NULLIF(p.c_bool, false)) AS n `+
			`FROM mbtypes p WHERE p.id < 300 ORDER BY p.id`)
	counts := map[int64]int{}
	for _, r := range rows {
		counts[wpkInt(t, r["n"])]++
	}
	if len(counts) < 2 {
		t.Fatalf("every row landed in one partition (%v) — the NULL key was not a key at all", counts)
	}
	// The NULL partition is one partition: every row whose key is NULL must
	// report the SAME count, and that count must be the number of such rows.
	nulls := 0
	for _, r := range rows {
		if r["n"] == nil {
			t.Fatalf("COUNT(*) OVER cannot be NULL: %v", r)
		}
	}
	trueRows := 0
	for i := 0; i < 300; i++ {
		if i%23 != 22 && i%3 == 0 {
			trueRows++
		}
	}
	nulls = 300 - trueRows
	if counts[int64(trueRows)] != trueRows || counts[int64(nulls)] != nulls {
		t.Errorf("partition sizes %v: want one partition of %d (key TRUE) and one of %d (key NULL, "+
			"PostgreSQL groups every NULL together)", counts, trueRows, nulls)
	}
}

// TestWindowMixedPartitionKeys covers a key LIST that mixes the three
// spellings, and two OVER clauses that share one computed key — the case a
// per-clause projection would compute twice, putting two columns of one name
// on the batch.
func TestWindowMixedPartitionKeys(t *testing.T) {
	ctx := context.Background()
	db := wpkOpen(t, 0)

	want := wpkRun(t, ctx, db,
		`SELECT p.id, ROW_NUMBER() OVER (PARTITION BY g, c_bool ORDER BY id) AS w `+
			`FROM mbtypes p WHERE p.id < 300 ORDER BY p.id`)
	got := wpkRun(t, ctx, db,
		`SELECT p.id, ROW_NUMBER() OVER (PARTITION BY p.g, id % 2 = 0 OR c_bool ORDER BY p.id) AS w `+
			`FROM mbtypes p WHERE p.id < 300 ORDER BY p.id`)
	if len(got) != len(want) {
		t.Fatalf("mixed key list returned %d rows, want %d", len(got), len(want))
	}
	wpkAssertPartitioned(t, "mixed keys", got)

	shared := wpkRun(t, ctx, db,
		`SELECT p.id, ROW_NUMBER() OVER (PARTITION BY id % 4 ORDER BY id) AS a, `+
			`COUNT(*) OVER (PARTITION BY id % 4) AS b FROM mbtypes p WHERE p.id < 300 ORDER BY p.id`)
	for _, r := range shared {
		if wpkInt(t, r["b"]) != 75 {
			t.Fatalf("two OVER clauses sharing one computed key: COUNT(*) = %v, want 75 "+
				"(300 rows / 4 partitions)", r["b"])
		}
	}
}

// TestWindowRowFieldPartitionKey covers `rw.f` over a ROW column: it LOOKS
// like a qualified reference and is not one, so dropping the qualifier would
// key on a column named `f` that does not exist.
func TestWindowRowFieldPartitionKey(t *testing.T) {
	ctx := context.Background()
	db := wpkOpen(t, 0)

	rows := wpkRun(t, ctx, db,
		`SELECT id, COUNT(*) OVER (PARTITION BY c_row.a) AS n FROM mbtypes WHERE id < 40 ORDER BY id`)
	// c_row.a is distinct per row except where c_row itself is NULL (every
	// 107th row, none below id 40), so every partition holds exactly one row.
	for _, r := range rows {
		if wpkInt(t, r["n"]) != 1 {
			t.Errorf("id=%v: COUNT(*) OVER (PARTITION BY c_row.a) = %v, want 1 — a ROW FIELD is "+
				"the key, not the whole input", r["id"], r["n"])
		}
	}
}

// TestWindowUnresolvableKeyIsRefused pins the half of #585 that is a POLICY
// rather than a repair: a key the engine cannot resolve fails the query.
// Answering it over one partition is answering a different question, and
// PostgreSQL refuses the same spelling ("column ... does not exist").
func TestWindowUnresolvableKeyIsRefused(t *testing.T) {
	ctx := context.Background()
	db := wpkOpen(t, 0)

	for _, sql := range []string{
		`SELECT id, ROW_NUMBER() OVER (PARTITION BY nosuchcol) AS w FROM mbtypes WHERE id < 10`,
		`SELECT id, ROW_NUMBER() OVER (ORDER BY nosuchcol) AS w FROM mbtypes WHERE id < 10`,
		`SELECT id, ROW_NUMBER() OVER (PARTITION BY g, nosuchcol) AS w FROM mbtypes WHERE id < 10`,
	} {
		if _, err := db.Query(ctx, sql); err == nil {
			t.Errorf("this query ANSWERED; an unresolvable window key must fail loudly rather "+
				"than degrade to one partition\n  SQL: %s", sql)
		} else if !strings.Contains(err.Error(), "nosuchcol") {
			t.Errorf("the refusal does not name the key that could not be resolved: %v\n  SQL: %s", err, sql)
		}
	}
}

// TestWindowKeySpellingsAgreeUnderSpill reruns the agreement over a 1 KiB
// budget, which pushes the window onto the external partition-at-a-time path
// (sorted columnar runs, k-way merge, partitionWalker). That walker resolves
// the key AGAIN, off the merged run's schema — a third resolution site, and
// the one whose old guard tested only the upper bound, so an unresolved key
// indexed Columns[-1] and panicked the task.
func TestWindowKeySpellingsAgreeUnderSpill(t *testing.T) {
	ctx := context.Background()
	plain := wpkOpen(t, 0)
	spilled := wpkOpen(t, 1024)

	for _, over := range []string{
		"PARTITION BY p.g ORDER BY p.id",
		"PARTITION BY id % 3 ORDER BY id",
		"PARTITION BY id % 3",
	} {
		over := over
		t.Run(over, func(t *testing.T) {
			sql := fmt.Sprintf(
				`SELECT p.id, ROW_NUMBER() OVER (%s) AS w FROM mbtypes p ORDER BY p.id LIMIT 500`, over)
			want := wpkRun(t, ctx, plain, sql)
			got := wpkRun(t, ctx, spilled, sql)
			wpkAssertPartitioned(t, "spilled "+over, got)
			wpkAssertSame(t, "spilled "+over, got, want)
		})
	}
}

func wpkRun(t *testing.T, ctx context.Context, db *DB, sql string) []map[string]any {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v\n  SQL: %s", err, sql)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("query returned no rows, so it gates nothing\n  SQL: %s", sql)
	}
	return res.Rows
}

// wpkAssertSame compares two answers positionally on (id, w). Both queries
// carry a total ORDER BY id, so the sequence is part of the answer.
func wpkAssertSame(t *testing.T, what string, got, want []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d", what, len(got), len(want))
	}
	for i := range got {
		if fmt.Sprint(got[i]["id"]) != fmt.Sprint(want[i]["id"]) ||
			fmt.Sprint(got[i]["w"]) != fmt.Sprint(want[i]["w"]) {
			t.Fatalf("%s: row %d is (id=%v, w=%v), the bare-column spelling of the same window "+
				"answers (id=%v, w=%v)", what, i, got[i]["id"], got[i]["w"], want[i]["id"], want[i]["w"])
		}
	}
}

// wpkAssertPartitioned fails when the window column is constant. Every query
// above partitions into at least three groups, so a constant column is the
// one-partition degradation and nothing else.
func wpkAssertPartitioned(t *testing.T, what string, rows []map[string]any) {
	t.Helper()
	seen := map[string]bool{}
	for _, r := range rows {
		seen[fmt.Sprint(r["w"])] = true
	}
	if len(seen) > 1 {
		return
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Fatalf("%s: every row answers w=%s — the window ran over ONE partition, which is #585's "+
		"signature and makes this comparison vacuous", what, strings.Join(keys, ","))
}

func wpkInt(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	t.Fatalf("expected an integer window value, got %#v (%T)", v, v)
	return 0
}

// --- A ROW field path as a window's argument or key (#603) ------------------
//
// `rw.f` parses to the same plansql.ColRef a table-qualified reference does,
// and the window path resolved it as one — dropping the `rw.` and looking up a
// column named `f`, which is a column of nothing. All four consumers answered
// SILENTLY:
//
//	SUM(rw.f) OVER ()                     NULL in every row
//	LAG(rw.f) OVER (ORDER BY id)          NULL in every row
//	ROW_NUMBER() OVER (ORDER BY rw.f)     numbered in input order, ORDER BY lost
//	COUNT(*) OVER (PARTITION BY rw.f)     one partition
//
// It is #585's seam because the repair is the same one: a field path is not a
// column reference, so it is MATERIALIZED as a synthetic pre-window column —
// here carrying the FIELD's declared type, which is what #568 settled for
// every other consumer (a field name alone types as STRING, and an INT64 field
// read through that is a wrong number, not a missing one).
//
// The reference is a FLAT column holding the same value in every row, compared
// with reflect.DeepEqual so a divergence of Go TYPE is visible and not only a
// divergence of rendering.

const wrfRows = 600

func wrfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "f_i64", Type: parquet.TypeInt64, Nullable: true},
			{Name: "f_bool", Type: parquet.TypeBool, Nullable: true},
			{Name: "f_str", Type: parquet.TypeString, Nullable: true},
			{Name: "f_f64", Type: parquet.TypeFloat64, Nullable: true},
		}},
		{Name: "xf_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "xf_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "xf_str", Type: parquet.TypeString, Nullable: true},
		{Name: "xf_f64", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// wrfData mirrors every ROW field into a flat column of the same type and the
// same value, including its NULLs — the comparison is only ground truth while
// the two spellings really name one value.
func wrfData() []map[string]any {
	rows := make([]map[string]any, wrfRows)
	for i := range rows {
		var i64 any = int64(i) * 7
		var b any = i%3 == 0
		var s any = fmt.Sprintf("f-%04d", i%9)
		var f any = float64(i) / 4
		if i%13 == 12 {
			i64, b, s, f = nil, nil, nil, nil
		}
		rows[i] = map[string]any{
			"id":      int64(i),
			"rw":      map[string]any{"f_i64": i64, "f_bool": b, "f_str": s, "f_f64": f},
			"xf_i64":  i64,
			"xf_bool": b,
			"xf_str":  s,
			"xf_f64":  f,
		}
	}
	return rows
}

func wrfOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := wrfSchema()
	if err := db.CreateTable(ctx, "winrowfld", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("winrowfld", schema, nil, ingest.Config{
		MaxBufferRows: wrfRows + 1, RowGroupSize: 250,
	})
	if err := ing.Ingest(ctx, wrfData()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestWindowOverRowFieldPath is #603's gate: every window consumer of a field
// path must answer what the same value in a flat column answers.
func TestWindowOverRowFieldPath(t *testing.T) {
	ctx := context.Background()
	db := wrfOpen(t)

	// %[1]s is the value expression: `rw.f_i64` or the flat `xf_i64`.
	shapes := []struct {
		name  string
		sql   string
		field string
	}{
		{"sum_over_empty", `SELECT id, SUM(%[1]s) OVER () AS w FROM winrowfld ORDER BY id`, "i64"},
		{"sum_over_partition", `SELECT id, SUM(%[1]s) OVER (PARTITION BY id %% 5) AS w FROM winrowfld ORDER BY id`, "i64"},
		{"count_over_partition_by_field", `SELECT id, COUNT(*) OVER (PARTITION BY %[1]s) AS w FROM winrowfld ORDER BY id`, "bool"},
		{"count_over_partition_by_str_field", `SELECT id, COUNT(*) OVER (PARTITION BY %[1]s) AS w FROM winrowfld ORDER BY id`, "str"},
		{"lag", `SELECT id, LAG(%[1]s) OVER (ORDER BY id) AS w FROM winrowfld ORDER BY id`, "i64"},
		{"lead", `SELECT id, LEAD(%[1]s) OVER (ORDER BY id) AS w FROM winrowfld ORDER BY id`, "str"},
		{"first_value", `SELECT id, FIRST_VALUE(%[1]s) OVER (PARTITION BY id %% 4 ORDER BY id) AS w FROM winrowfld ORDER BY id`, "str"},
		{"min", `SELECT id, MIN(%[1]s) OVER (PARTITION BY id %% 6) AS w FROM winrowfld ORDER BY id`, "i64"},
		{"max_float", `SELECT id, MAX(%[1]s) OVER (PARTITION BY id %% 6) AS w FROM winrowfld ORDER BY id`, "f64"},
		{"row_number_ordered_by_field", `SELECT id, ROW_NUMBER() OVER (ORDER BY %[1]s DESC NULLS FIRST, id) AS w FROM winrowfld ORDER BY id`, "i64"},
		{"rank_ordered_by_field", `SELECT id, RANK() OVER (PARTITION BY id %% 3 ORDER BY %[1]s, id) AS w FROM winrowfld ORDER BY id`, "str"},
	}
	for _, sh := range shapes {
		sh := sh
		t.Run(sh.name, func(t *testing.T) {
			want := wpkRun(t, ctx, db, fmt.Sprintf(sh.sql, "xf_"+sh.field))
			got := wpkRun(t, ctx, db, fmt.Sprintf(sh.sql, "rw.f_"+sh.field))
			if len(got) != len(want) {
				t.Fatalf("field path returned %d rows, the flat column %d", len(got), len(want))
			}
			nonNull := 0
			for i := range got {
				if !reflect.DeepEqual(got[i]["w"], want[i]["w"]) {
					t.Fatalf("row %d (id=%v): rw.f_%s answers %#v, the flat xf_%s holding the SAME "+
						"value answers %#v", i, want[i]["id"], sh.field, got[i]["w"], sh.field, want[i]["w"])
				}
				if want[i]["w"] != nil {
					nonNull++
				}
			}
			if nonNull == 0 {
				t.Fatalf("the flat-column reference is NULL in every row, so this comparison "+
					"cannot tell #603's all-NULL answer from a right one\n  SQL: %s",
					fmt.Sprintf(sh.sql, "xf_"+sh.field))
			}
		})
	}
}
