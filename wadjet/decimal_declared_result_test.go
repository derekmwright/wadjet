package wadjet

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The cross-scale DECIMAL pair fixture. The type-matrix table carries ONE
// DECIMAL column, and a column reconciled against itself agrees whatever rule
// is applied — two columns at DIFFERENT (p,s) is the only shape that can tell
// a common-type rule from a first-argument-wins one, and the only one that
// can tell a set operation whose arms AGREE from one whose arms do not (#542).
const ddrTable = "decdecl"

func ddrOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
	if err := db.CreateTable(ctx, ddrTable, schema, nil); err != nil {
		t.Fatal(err)
	}
	dec := func(v int64) parquet.Decimal128 {
		hi := int64(0)
		if v < 0 {
			hi = -1
		}
		return parquet.Decimal128{Hi: hi, Lo: uint64(v)}
	}
	rows := []map[string]any{
		// a = 12.75 (scale 2), b = 12.7500 / 12.7501 / 12.7499 (scale 4):
		// the ±1-ulp neighbourhood where a rounded comparison and an exact
		// one disagree.
		{"id": int64(1), "a": dec(1275), "b": dec(127500)},
		{"id": int64(2), "a": dec(1275), "b": dec(127501)},
		{"id": int64(3), "a": dec(1275), "b": dec(127499)},
		// "2.00" sorts above "10.0000" as TEXT and below it as a number.
		{"id": int64(4), "a": dec(200), "b": dec(100000)},
		{"id": int64(5), "a": dec(-1), "b": dec(-100)},
		{"id": int64(6), "b": dec(10000)},
		{"id": int64(7), "a": dec(1275)},
	}
	ing := db.NewIngester(ddrTable, schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func ddrQuery(t *testing.T, db *DB, sql string) *QueryResult {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return res
}

// TestDecimalChoiceExpressionsProjectExactly is #529 and the COALESCE half of
// #555, end to end.
//
// A projected GREATEST/LEAST over two DECIMAL columns could not RUN before
// ADR-0024: the registry declared the return type FLOAT64, the value arrived
// as the column's rendered text, and the #361 silent-write guard refused it —
// "cannot store string into FLOAT64 vector". COALESCE and CASE were the same
// defect under other names. The declaration is now the common DECIMAL type of
// the branches, and the value materializes into a DECIMAL vector at that
// scale, exactly: every digit either column holds is still there.
func TestDecimalChoiceExpressionsProjectExactly(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name  string
		sql   string
		scale int
		want  []string
	}{
		{
			name:  "greatest",
			sql:   "SELECT GREATEST(a, b) AS v FROM " + ddrTable + " ORDER BY id",
			scale: 4,
			// Row 3 is the proof the comparison is exact and not textual:
			// 12.75 > 12.7499 numerically, while "12.75" < "12.7499" as text.
			want: []string{"12.7500", "12.7501", "12.7500", "10.0000", "-0.0100", "1.0000", "12.7500"},
		},
		{
			name:  "least",
			sql:   "SELECT LEAST(a, b) AS v FROM " + ddrTable + " ORDER BY id",
			scale: 4,
			want:  []string{"12.7500", "12.7500", "12.7499", "2.0000", "-0.0100", "1.0000", "12.7500"},
		},
		{
			name:  "coalesce",
			sql:   "SELECT COALESCE(a, b) AS v FROM " + ddrTable + " ORDER BY id",
			scale: 4,
			want:  []string{"12.7500", "12.7500", "12.7500", "2.0000", "-0.0100", "1.0000", "12.7500"},
		},
		{
			name:  "case",
			sql:   "SELECT CASE WHEN id < 4 THEN a ELSE b END AS v FROM " + ddrTable + " ORDER BY id",
			scale: 4,
			want:  []string{"12.7500", "12.7500", "12.7500", "10.0000", "-0.0100", "1.0000", ""},
		},
		{
			// NULLIF mirrors argument 0 alone, so the output keeps a's own
			// (9,2) — the narrower rendering is the RIGHT one here.
			//
			// Rows 1 and 5 are NULL because the EQUALITY is exact: 12.75 at
			// scale 2 and 12.7500 at scale 4 are the same number, though not
			// the same text. Comparing the boxes as strings answered 12.75
			// there, which is what evalNullIf fixes.
			name:  "nullif",
			sql:   "SELECT NULLIF(a, b) AS v FROM " + ddrTable + " ORDER BY id",
			scale: 2,
			want:  []string{"", "12.75", "12.75", "2.00", "", "", "12.75"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d columns, want 1", len(res.ColumnMetas))
			}
			if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Scale != tc.scale {
				t.Errorf("declared %s(%d,%d), want DECIMAL scale %d",
					m.TypeID, m.Precision, m.Scale, tc.scale)
			}
			var got []string
			for _, row := range res.Rows {
				if row["v"] == nil {
					got = append(got, "")
					continue
				}
				got = append(got, fmt.Sprintf("%v", row["v"]))
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("values = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDecimalChoiceExpressionOverOneColumnKeepsItsScale is the control for
// the pair above: over a single DECIMAL column the common type IS that
// column's type, so the rendering must not widen.
func TestDecimalChoiceExpressionOverOneColumnKeepsItsScale(t *testing.T) {
	db := ddrOpen(t)
	res := ddrQuery(t, db, "SELECT GREATEST(a, a) AS v FROM "+ddrTable+" WHERE id = 1")
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Precision != 9 || m.Scale != 2 {
		t.Fatalf("declared %s(%d,%d), want DECIMAL(9,2)", m.TypeID, m.Precision, m.Scale)
	}
	if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != "12.75" {
		t.Errorf("value = %q, want %q", got, "12.75")
	}
}

// TestDecimalWireTypmodIsUnconstrainedForComputedResults is ADR-0024 item 5,
// which closes #587 and #542.
//
// PostgreSQL keeps a numeric's typmod for a BARE COLUMN REFERENCE and for
// nothing else — verified live against postgres:17-alpine's \gdesc. Before
// this, declaredWireUnconstrainedDecimal gated on proj.IsAgg alone, so a
// window function (which reaches the output projection as a bare reference to
// the Window operator's output column) and a computed expression both went
// out carrying a real (p,s), and a set operation did too however far its arms'
// declarations were apart.
//
// The corresponding wire-corpus entries in benchmarks/tpch/postgres_wire_test.go
// prove the same thing against a live server; this is the direct assertion on
// ColumnMeta, and it also covers the ZERO-ROW arm, where the answer comes from
// the plan alone.
func TestDecimalWireTypmodIsUnconstrainedForComputedResults(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		col  string
		want bool
	}{
		// The control: a bare column reference KEEPS its typmod.
		{"bare column", "SELECT a FROM " + ddrTable, "a", false},
		{"bare column, zero rows", "SELECT a FROM " + ddrTable + " WHERE id < 0", "a", false},
		{"a rename is still a bare reference", "SELECT a AS v FROM " + ddrTable, "v", false},

		// #587: a window function.
		{"windowed min", "SELECT MIN(a) OVER () AS v FROM " + ddrTable, "v", true},
		{"windowed min, zero rows", "SELECT MIN(a) OVER () AS v FROM " + ddrTable + " WHERE id < 0", "v", true},
		{"windowed first_value", "SELECT FIRST_VALUE(a) OVER (ORDER BY id) AS v FROM " + ddrTable, "v", true},
		{"windowed lag", "SELECT LAG(a) OVER (ORDER BY id) AS v FROM " + ddrTable, "v", true},

		// Computed expressions.
		{"greatest", "SELECT GREATEST(a, b) AS v FROM " + ddrTable, "v", true},
		{"coalesce", "SELECT COALESCE(a, b) AS v FROM " + ddrTable, "v", true},
		{"case", "SELECT CASE WHEN id < 4 THEN a ELSE b END AS v FROM " + ddrTable, "v", true},
		{"greatest, zero rows", "SELECT GREATEST(a, b) AS v FROM " + ddrTable + " WHERE id < 0", "v", true},

		// The aggregate half, which already held before ADR-0024.
		{"min", "SELECT MIN(a) AS v FROM " + ddrTable, "v", true},

		// #542: a set operation keeps the typmod only when every arm
		// carries the SAME one.
		{"set op, arms agree", "SELECT a AS v FROM " + ddrTable + " UNION ALL SELECT a FROM " + ddrTable, "v", false},
		{"set op, arms disagree", "SELECT a AS v FROM " + ddrTable + " UNION ALL SELECT b FROM " + ddrTable, "v", true},
		{"set op, arms disagree, INTERSECT", "SELECT a AS v FROM " + ddrTable + " INTERSECT SELECT b FROM " + ddrTable, "v", true},
		{"set op, arms disagree, EXCEPT", "SELECT a AS v FROM " + ddrTable + " EXCEPT SELECT b FROM " + ddrTable, "v", true},
		{"set op with ORDER BY, arms disagree",
			"SELECT a AS v FROM " + ddrTable + " UNION ALL SELECT b FROM " + ddrTable + " ORDER BY 1", "v", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			var found bool
			for _, m := range res.ColumnMetas {
				if m.Name != tc.col {
					continue
				}
				found = true
				if m.TypeID != parquet.TypeDecimal {
					t.Errorf("%s: declared %s, want DECIMAL", tc.col, m.TypeID)
				}
				if m.WireUnconstrained != tc.want {
					t.Errorf("%s: WireUnconstrained = %v, want %v", tc.col, m.WireUnconstrained, tc.want)
				}
			}
			if !found {
				t.Fatalf("column %q not in result: %v", tc.col, res.Columns)
			}
		})
	}
}

// TestDecimalComputedKeysAreNotTruncated covers the sites a computed DECIMAL
// reaches as a MATERIALIZED KEY rather than as an output column: a GROUP BY
// expression, a DISTINCT, an aggregate's derived input, an ORDER BY term, a
// window PARTITION BY / ORDER BY term, and a join condition. Each one
// allocates its own vector from the planner's declaration, and a DECIMAL
// vector with no scale TRUNCATES every value written into it — 12.75 and
// 12.7501 collapse into one key holding 12.
//
// The declaration and the vector are two separate plumbing steps and only the
// values can show they agree, which is why this asserts answers rather than
// metadata (ADR-0024 item 2). Without it the fix for #529 would have turned a
// loud refusal into a silent wrong answer at every one of these sites.
func TestDecimalComputedKeysAreNotTruncated(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"group by a computed decimal",
			"SELECT COALESCE(a, b) AS k, COUNT(*) AS n FROM " + ddrTable + " GROUP BY COALESCE(a, b) ORDER BY 1",
			"[map[k:-0.0100 n:1] map[k:1.0000 n:1] map[k:2.0000 n:1] map[k:12.7500 n:4]]"},
		{"distinct over a computed decimal",
			"SELECT DISTINCT GREATEST(a, b) AS g FROM " + ddrTable + " ORDER BY 1",
			"[map[g:-0.0100] map[g:1.0000] map[g:10.0000] map[g:12.7500] map[g:12.7501]]"},
		{"an aggregate over a computed decimal",
			"SELECT MAX(COALESCE(a, b)) AS m, MIN(GREATEST(a, b)) AS l FROM " + ddrTable,
			"[map[l:-0.0100 m:12.7500]]"},
		{"order by a computed decimal",
			"SELECT id FROM " + ddrTable + " ORDER BY GREATEST(a, b) DESC, id",
			"[map[id:2] map[id:1] map[id:3] map[id:7] map[id:4] map[id:6] map[id:5]]"},
		{"a window partitioned by a computed decimal",
			"SELECT id, MIN(a) OVER (PARTITION BY COALESCE(a, b)) AS w FROM " + ddrTable + " ORDER BY id",
			"[map[id:1 w:12.75] map[id:2 w:12.75] map[id:3 w:12.75] map[id:4 w:2.00] " +
				"map[id:5 w:-0.01] map[id:6 w:<nil>] map[id:7 w:12.75]]"},
		{"a window ordered by a computed decimal",
			"SELECT id, ROW_NUMBER() OVER (ORDER BY GREATEST(a, b), id) AS r FROM " + ddrTable + " ORDER BY id",
			"[map[id:1 r:4] map[id:2 r:7] map[id:3 r:5] map[id:4 r:3] map[id:5 r:1] map[id:6 r:2] map[id:7 r:6]]"},
		{"a join on a computed decimal",
			"SELECT x.id FROM " + ddrTable + " x JOIN " + ddrTable + " y " +
				"ON COALESCE(x.a, x.b) = COALESCE(y.a, y.b) AND x.id < y.id ORDER BY x.id",
			"[map[id:1] map[id:1] map[id:1] map[id:2] map[id:2] map[id:3]]"},
		{"a computed decimal in a filter",
			"SELECT COUNT(*) AS n FROM " + ddrTable + " WHERE GREATEST(a, b) = 12.7501",
			"[map[n:1]]"},
		{"a computed decimal through an IN subquery",
			"SELECT id FROM " + ddrTable + " WHERE GREATEST(a, b) IN " +
				"(SELECT GREATEST(a, b) FROM " + ddrTable + " WHERE id = 2) ORDER BY id",
			"[map[id:2]]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if got := fmt.Sprintf("%v", res.Rows); got != tc.want {
				t.Errorf("%s\n  got  %s\n  want %s", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalChoiceExpressionRefusesAValueWithNoCarrier is ADR-0024 item 4 on
// the path item 2 opened.
//
// GREATEST over DECIMAL(38,0) and DECIMAL(11,10) declares the common type
// DECIMAL(38,10) — the cap has already spent the integer digits the wide arm
// needs (#552's corner) — so a value with 38 integer digits has no Int128 at
// the output scale. exec.Project writes computed DECIMAL outputs through
// Vector.SetValueChecked, so that is a 22003 error; Vector.SetValue, the
// unchecked writer the comparison and ingest paths keep, would have stored
// the saturated end of the range and answered a plausible wrong number
// (#553).
func TestDecimalChoiceExpressionRefusesAValueWithNoCarrier(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "wide", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		{Name: "fine", Type: parquet.TypeDecimal, Precision: 11, Scale: 10, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "deccap", schema, nil); err != nil {
		t.Fatal(err)
	}
	// 10^30, which needs 31 integer digits: fine at DECIMAL(38,0), and past
	// the carrier once restated at scale 10 (41 digits).
	wide := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
	rows := []map[string]any{{
		"id":   int64(1),
		"wide": parquet.Decimal128{Hi: int64(new(big.Int).Rsh(wide, 64).Uint64()), Lo: new(big.Int).And(wide, new(big.Int).SetUint64(^uint64(0))).Uint64()},
		"fine": parquet.Decimal128{Lo: 1},
	}}
	ing := db.NewIngester("deccap", schema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = db.Query(ctx, "SELECT GREATEST(wide, fine) AS g FROM deccap")
	if err == nil {
		t.Fatal("a value with no Int128 at the declared scale was ANSWERED — the " +
			"saturating writer would return 1701411834604692317316873037.1588410572")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003 numeric_value_out_of_range: %v", got, err)
	}
}
