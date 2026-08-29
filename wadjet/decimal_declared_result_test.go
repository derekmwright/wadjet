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
		// A DECIMAL ARRAY, so element_at over a real container has somewhere
		// to be tested: arr[1] is a's value and arr[2] is 3.00 on every row.
		{Name: "arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeDecimal,
				Precision: 9, Scale: 2, Nullable: true}},
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
	arr := func(first int64, present bool) []any {
		if !present {
			return []any{nil, dec(300)}
		}
		return []any{dec(first), dec(300)}
	}
	rows := []map[string]any{
		// a = 12.75 (scale 2), b = 12.7500 / 12.7501 / 12.7499 (scale 4):
		// the ±1-ulp neighbourhood where a rounded comparison and an exact
		// one disagree.
		{"id": int64(1), "a": dec(1275), "b": dec(127500), "arr": arr(1275, true)},
		{"id": int64(2), "a": dec(1275), "b": dec(127501), "arr": arr(1275, true)},
		{"id": int64(3), "a": dec(1275), "b": dec(127499), "arr": arr(1275, true)},
		// "2.00" sorts above "10.0000" as TEXT and below it as a number.
		{"id": int64(4), "a": dec(200), "b": dec(100000), "arr": arr(200, true)},
		{"id": int64(5), "a": dec(-1), "b": dec(-100), "arr": arr(-1, true)},
		{"id": int64(6), "b": dec(10000), "arr": arr(0, false)},
		{"id": int64(7), "a": dec(1275), "arr": arr(1275, true)},
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

// TestDecimalWireTypmod is ADR-0024 item 5, which closes #587 and #542.
//
// The rule is PostgreSQL's select_common_typmod, not "computed means
// unconstrained": a numeric result KEEPS a type modifier when every input it
// is resolved from carries the same one, and carries none otherwise —
// verified live against 17.11's \gdesc. A bare column reference carries its
// column's; an aggregate, a window function, arithmetic, a CAST and every
// other function call carry none, and one of those anywhere in the fold makes
// the result unconstrained.
//
// Before this, declaredWireUnconstrainedDecimal gated on proj.IsAgg alone —
// so a window function (which reaches the output projection as a bare
// reference to the Window operator's output column) and a set operation went
// out carrying a real (p,s) whatever their arms said. The first fix for that
// over-corrected to "computed means unconstrained", which is wrong in the
// other direction: GREATEST(a, a) is numeric(9,2) on PostgreSQL, and a set
// operation over an AGGREGATE arm is plain numeric however well the widths
// line up.
//
// The corresponding wire-corpus entries in benchmarks/tpch/postgres_wire_test.go
// prove the same thing against a live server; this is the direct assertion on
// ColumnMeta, and it also covers the ZERO-ROW arm, where the answer comes from
// the plan alone.
func TestDecimalWireTypmod(t *testing.T) {
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

		// select_common_typmod KEEPS the modifier when every input agrees.
		// Each of these describes as numeric(9,2) or numeric(18,4) on live
		// PostgreSQL 17.11, not as plain numeric.
		{"greatest over one column", "SELECT GREATEST(a, a) AS v FROM " + ddrTable, "v", false},
		{"greatest of one argument", "SELECT GREATEST(a) AS v FROM " + ddrTable, "v", false},
		{"coalesce over one column", "SELECT COALESCE(a, a) AS v FROM " + ddrTable, "v", false},
		{"case whose branches are one column",
			"SELECT CASE WHEN id > 1 THEN a ELSE a END AS v FROM " + ddrTable, "v", false},
		{"least over one column", "SELECT LEAST(b, b) AS v FROM " + ddrTable, "v", false},
		// NULLIF folds argument 0 ALONE, the same candidate list its TYPE
		// resolution folds, so it keeps the first argument's modifier
		// whatever the second one carries.
		{"nullif keeps its first argument's modifier",
			"SELECT NULLIF(a, b) AS v FROM " + ddrTable, "v", false},
		{"nullif the other way round",
			"SELECT NULLIF(b, a) AS v FROM " + ddrTable, "v", false},
		// A NULL branch is where the TYPMOD fold parts company with the TYPE
		// fold: COALESCE(a, NULL) is numeric(9,2) as a TYPE (the NULL names
		// none, so it neither contributes nor blocks) and plain numeric on
		// the WIRE (an untyped NULL coerced into that type carries -1).
		// Verified live; the wire corpus entry CoalesceWithANullBranch is
		// the same assertion against the server.
		{"a NULL branch drops the modifier",
			"SELECT COALESCE(a, NULL) AS v FROM " + ddrTable, "v", true},

		// #587: a window function.
		{"windowed min", "SELECT MIN(a) OVER () AS v FROM " + ddrTable, "v", true},
		{"windowed min, zero rows", "SELECT MIN(a) OVER () AS v FROM " + ddrTable + " WHERE id < 0", "v", true},
		{"windowed first_value", "SELECT FIRST_VALUE(a) OVER (ORDER BY id) AS v FROM " + ddrTable, "v", true},
		{"windowed lag", "SELECT LAG(a) OVER (ORDER BY id) AS v FROM " + ddrTable, "v", true},

		// Computed expressions whose inputs carry DIFFERENT modifiers:
		// a is numeric(9,2) and b is numeric(18,4), so the fold has none.
		{"greatest across scales", "SELECT GREATEST(a, b) AS v FROM " + ddrTable, "v", true},
		{"coalesce across scales", "SELECT COALESCE(a, b) AS v FROM " + ddrTable, "v", true},
		{"case across scales", "SELECT CASE WHEN id < 4 THEN a ELSE b END AS v FROM " + ddrTable, "v", true},
		{"greatest across scales, zero rows",
			"SELECT GREATEST(a, b) AS v FROM " + ddrTable + " WHERE id < 0", "v", true},

		// The aggregate half, which already held before ADR-0024.
		{"min", "SELECT MIN(a) AS v FROM " + ddrTable, "v", true},

		// #542: a set operation keeps the typmod only when every arm
		// carries the SAME one.
		{"set op, arms agree", "SELECT a AS v FROM " + ddrTable + " UNION ALL SELECT a FROM " + ddrTable, "v", false},
		{"set op, arms disagree", "SELECT a AS v FROM " + ddrTable + " UNION ALL SELECT b FROM " + ddrTable, "v", true},
		{"set op, arms disagree, INTERSECT", "SELECT a AS v FROM " + ddrTable + " INTERSECT SELECT b FROM " + ddrTable, "v", true},
		{"set op, arms disagree, EXCEPT", "SELECT a AS v FROM " + ddrTable + " EXCEPT SELECT b FROM " + ddrTable, "v", true},
		// An arm that carries NO modifier makes the result unconstrained
		// however well the widths line up — the direction "the arms' (p,s)
		// disagree" alone cannot see.
		{"set op over an aggregate arm",
			"SELECT MIN(a) AS v FROM " + ddrTable + " UNION ALL SELECT a FROM " + ddrTable, "v", true},
		{"set op over a computed arm",
			"SELECT COALESCE(a, b) AS v FROM " + ddrTable + " UNION ALL SELECT b FROM " + ddrTable, "v", true},
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
		// GREATEST's winner is the WIDER column on every row, so both sides
		// render at scale 4 and a text-keyed membership set happens to
		// agree. COALESCE's winner is the NARROWER one, so the probe renders
		// "12.75" against a set holding "12.7500" — same number, two keys —
		// and the predicate answered zero rows where PostgreSQL answers
		// four. Both are kept: the pair is what shows the set is keyed by
		// VALUE and not by rendering (ADR-0012 item 8).
		{"a computed decimal through an IN subquery",
			"SELECT id FROM " + ddrTable + " WHERE GREATEST(a, b) IN " +
				"(SELECT GREATEST(a, b) FROM " + ddrTable + " WHERE id = 2) ORDER BY id",
			"[map[id:2]]"},
		{"an IN subquery whose two sides render at different scales",
			"SELECT id FROM " + ddrTable + " WHERE COALESCE(a, b) IN " +
				"(SELECT COALESCE(a, b) FROM " + ddrTable + " WHERE id = 2) ORDER BY id",
			"[map[id:1] map[id:2] map[id:3] map[id:7]]"},
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

// TestDecimalChoiceDeclinesOverAnUndeclaredProducer is the safety clause of
// ADR-0024 item 2, and it is the review finding the item-2 fold was missing.
//
// A branch that decided no type still PRODUCES a value at runtime, and a
// DECIMAL one arrives as text at ITS OWN scale. Folding only the branches
// that spoke declared `COALESCE(a, (SELECT MAX(b) FROM t))` numeric(9,2) —
// so the subquery's 12.7501 was TRUNCATED to 12.75 on the way into the output
// vector, and at the comparison sites the operand classified as nothing and
// GREATEST picked by BYTE order ("3.00" over "12.7501"). The fold now
// declines, which puts every such shape back exactly where it was before
// ADR-0024: a loud refusal, or the STRING fallback that renders the decimal
// text unchanged.
//
// A bare NULL is the exception and stays one: it names no type AND produces
// no value, which is SQL's `unknown`, so COALESCE(d, NULL) is numeric here as
// it is on PostgreSQL.
func TestDecimalChoiceDeclinesOverAnUndeclaredProducer(t *testing.T) {
	db := ddrOpen(t)
	sub := "(SELECT MAX(b) FROM " + ddrTable + ")"

	// A CASE falls back to STRING, which renders the branch's own text — so
	// row 6, where the subquery wins, keeps all four digits.
	res := ddrQuery(t, db, "SELECT CASE WHEN a IS NULL THEN "+sub+" ELSE a END AS c FROM "+ddrTable+" ORDER BY id")
	var got []string
	for _, r := range res.Rows {
		got = append(got, fmt.Sprintf("%v", r["c"]))
	}
	want := []string{"12.75", "12.75", "12.75", "2.00", "-0.01", "12.7501", "12.75"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("CASE over a scalar subquery = %v, want %v (row 6 is the subquery's own value, "+
			"which a fold to numeric(9,2) would have truncated to 12.75)", got, want)
	}

	// COALESCE and GREATEST have a numeric fallback rather than a string one,
	// so the same decline surfaces as the #361 store guard — loud, and the
	// answer this engine gave before a DECIMAL could decide anything.
	for _, sql := range []string{
		"SELECT COALESCE(a, " + sub + ") AS c FROM " + ddrTable,
		"SELECT GREATEST(a, " + sub + ") AS c FROM " + ddrTable,
		"SELECT LEAST(a, " + sub + ") AS c FROM " + ddrTable,
	} {
		if _, err := db.Query(context.Background(), sql); err == nil {
			t.Errorf("%s: answered — a DECIMAL fold over an operand with no declaration "+
				"silently truncates it to the fold's scale", sql)
		}
	}

	// The NULL exception.
	res = ddrQuery(t, db, "SELECT COALESCE(a, NULL) AS c FROM "+ddrTable+" WHERE id = 1")
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Scale != 2 {
		t.Errorf("COALESCE(a, NULL) declared %s(%d,%d), want DECIMAL(9,2)", m.TypeID, m.Precision, m.Scale)
	}
}

// TestNullifRefusesANonNumericLiteralBesideADecimal: NULLIF was the one boxed
// comparison site that did not run the refusal its siblings run. `=`,
// GREATEST, LEAST, IS DISTINCT FROM and simple CASE all raise 22P02 for a
// literal that names no number beside a DECIMAL column — PostgreSQL's answer
// — while NULLIF compared the two as boxes and returned every row.
func TestNullifRefusesANonNumericLiteralBesideADecimal(t *testing.T) {
	db := ddrOpen(t)
	_, err := db.Query(context.Background(), "SELECT NULLIF(a, 'abc') AS c FROM "+ddrTable)
	if err == nil {
		t.Fatal("NULLIF(decimal, 'abc') answered; PostgreSQL raises 22P02")
	}
	if got := sqlerr.StateOf(err); got != "22P02" {
		t.Errorf("SQLSTATE = %q, want 22P02 invalid_text_representation: %v", got, err)
	}
}

// TestDecimalDecidesThroughParensAndDerivedTables covers the two shapes that
// LOOK like a DECIMAL column and did not decide like one.
//
// `SELECT (a)` is a bare reference with parentheses. exec.Project resolves a
// copy's source by NAME and the name it holds is the parenthesized text, so
// nothing corrected the STRING fallback and the column went out as OID 25.
//
// `GREATEST(v, b)` over a DERIVED TABLE is #529 for every query that names
// its DECIMAL through a subquery: inputColTypes stops at the derived table's
// Project, so neither argument decided and GREATEST fell to FLOAT64 —
// "cannot store string into FLOAT64 vector", the exact failure ADR-0024's
// declaration layer exists to remove.
func TestDecimalDecidesThroughParensAndDerivedTables(t *testing.T) {
	db := ddrOpen(t)

	res := ddrQuery(t, db, "SELECT (a) AS v FROM "+ddrTable+" WHERE id = 1")
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Precision != 9 || m.Scale != 2 {
		t.Errorf("SELECT (a) declared %s(%d,%d), want DECIMAL(9,2)", m.TypeID, m.Precision, m.Scale)
	}
	if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != "12.75" {
		t.Errorf("SELECT (a) = %q, want 12.75", got)
	}

	res = ddrQuery(t, db,
		"SELECT GREATEST(v, b) AS g FROM (SELECT a AS v, b FROM "+ddrTable+") x ORDER BY g")
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Scale != 4 {
		t.Errorf("GREATEST over a derived table declared %s(%d,%d), want DECIMAL(18,4)",
			m.TypeID, m.Precision, m.Scale)
	}
	var got []string
	for _, r := range res.Rows {
		got = append(got, fmt.Sprintf("%v", r["g"]))
	}
	want := []string{"-0.0100", "1.0000", "10.0000", "12.7500", "12.7500", "12.7500", "12.7501"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("GREATEST over a derived table = %v, want %v", got, want)
	}
}

// TestDecimalBesideAnIntegerIsDataDependent records what the DECIMAL⊕INTEGER
// deferral actually costs, so nobody reads it as a stable refusal.
//
// PostgreSQL resolves GREATEST(numeric, integer) to numeric. Wadjet declares
// the INTEGER, because the alternative is worse: an integer box written into
// a DECIMAL vector is taken as ALREADY SCALED (ADR-0018 §4, the parquet
// ingest contract), so declaring DECIMAL would read GREATEST(a, 5) back as
// 0.05. The consequence is that the query's fate depends on the DATA — it
// answers wherever the integer wins every row and fails at the #361 store
// guard on the first row the decimal wins. That is a loud failure, never a
// wrong answer, and it is what TODO(#555) buys back when the store learns to
// scale an integer box.
func TestDecimalBesideAnIntegerIsDataDependent(t *testing.T) {
	db := ddrOpen(t)

	// Row 1 holds a = 12.75, so the integer 100 wins and the query answers —
	// as an INT64 column, which is already the wrong TYPE for PostgreSQL.
	res := ddrQuery(t, db, "SELECT GREATEST(a, 100) AS g FROM "+ddrTable+" WHERE id = 1")
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeInt64 {
		t.Errorf("GREATEST(a, 100) declared %s; the deferral declares the INTEGER "+
			"(TODO(#555) — PostgreSQL says numeric)", m.TypeID)
	}
	if got := fmt.Sprintf("%v", res.Rows[0]["g"]); got != "100" {
		t.Errorf("GREATEST(a, 100) = %q, want 100", got)
	}

	// The same expression with a small integer fails on the first row the
	// DECIMAL wins. Loud, and the same failure the whole family gave before
	// ADR-0024.
	if _, err := db.Query(context.Background(), "SELECT GREATEST(a, 1) AS g FROM "+ddrTable); err == nil {
		t.Error("GREATEST(a, 1) answered — the decimal wins on row 1, and an INT64 " +
			"declaration has nowhere to put its text")
	}
}

// TestDecimalChoiceOverAContainerElementComparesByValue is the runtime half of
// the review's P0-1: element_at lifts a value OUT of a container, so its kind
// is the container's ELEMENT kind. Without that arm the pair falls through to
// compare()'s byte order, and "2" sorts above "12.75" while 2 is below it.
//
// The arm has to be keyed on the COMPILED node: element_at compiles to
// expr.elementAtExpr (#607), so the arm the first attempt put under *FuncCall
// was dead code and every one of these was still ordered by bytes.
//
// The DECLARED type of such an expression is still undecided — the planner
// carries no element type for a top-level ARRAY column — so the fold declines
// and the projection keeps its pre-ADR-0024 STRING fallback, which renders
// the decimal text unchanged. What this pins is that the COMPARISON is exact
// wherever it runs.
func TestDecimalChoiceOverAContainerElementComparesByValue(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// arr[1] is a's value on every row and arr[2] is 3.00.
		{"a container element equals the column it was built from",
			"SELECT id FROM " + ddrTable + " WHERE element_at(arr, 1) = a ORDER BY id",
			"[map[id:1] map[id:2] map[id:3] map[id:4] map[id:5] map[id:7]]"},
		// GREATEST against a QUOTED literal, which PostgreSQL types FROM the
		// other operand. 2 wins outright on rows 5 and 6 and ties row 4's
		// 2.00; reading the pair as TEXT would have made "2" win everywhere,
		// because "2" sorts above "12.75".
		{"greatest over a container element and a quoted literal",
			"SELECT id FROM " + ddrTable + " WHERE GREATEST(element_at(arr, 1), '2') = '2' ORDER BY id",
			"[map[id:4] map[id:5] map[id:6]]"},
		// Against the WIDER column, where the two renderings differ in
		// length and byte order disagrees with numeric order.
		{"greatest over a container element and a wider decimal",
			"SELECT id FROM " + ddrTable + " WHERE GREATEST(element_at(arr, 1), b) > 5 ORDER BY id",
			"[map[id:1] map[id:2] map[id:3] map[id:4] map[id:7]]"},
		{"least over a container element and a wider decimal",
			"SELECT id FROM " + ddrTable + " WHERE LEAST(element_at(arr, 1), b) = element_at(arr, 1) ORDER BY id",
			"[map[id:1] map[id:2] map[id:4] map[id:5] map[id:7]]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if got := fmt.Sprintf("%v", res.Rows); got != tc.want {
				t.Errorf("%s\n  got  %s\n  want %s", tc.sql, got, tc.want)
			}
		})
	}
}

// TestNullifComparesByValueAgainstAnUndeclaredOperand is the review's second
// P0.
//
// NULLIF's candidate list is [0] — it mirrors argument 0 and nothing else —
// so the safety decline, which was computed over the CANDIDATES, never looked
// at argument 1. `NULLIF(a, (SELECT b …))` therefore declared numeric(9,2)
// from argument 0 alone, no decline fired, arms.order found no rule for a
// DECIMAL against an untypable operand, and compare() decided "12.75" is not
// "12.7500" by BYTES — answering the row where PostgreSQL answers NULL.
//
// Two fixes, and both are needed: expr.Ret.Resolve computes sawUnknown over
// EVERY evaluated argument (the candidate list is right for the TYPE and
// TYPMOD folds and wrong for this), and evalNullIf compares two numeric-text
// boxes as the numbers they name whenever either operand is DECLARED DECIMAL
// — never by sniffing the boxes, so a STRING column still compares as text
// (#504).
func TestNullifComparesByValueAgainstAnUndeclaredOperand(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// 12.75 = 12.7500, so rows 1, 2 and 3 are NULL; row 6's a is NULL;
		// row 7's a equals it too. Exactly PostgreSQL's answer.
		{"against a scalar subquery",
			"SELECT id FROM " + ddrTable + " WHERE NULLIF(a, (SELECT b FROM " + ddrTable +
				" WHERE id = 1)) IS NULL ORDER BY id",
			"[map[id:1] map[id:2] map[id:3] map[id:6] map[id:7]]"},
		// arr[1] IS a on every row, so every row is NULL.
		{"against a container element",
			"SELECT id FROM " + ddrTable + " WHERE NULLIF(a, element_at(arr, 1)) IS NULL ORDER BY id",
			"[map[id:1] map[id:2] map[id:3] map[id:4] map[id:5] map[id:6] map[id:7]]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if got := fmt.Sprintf("%v", res.Rows); got != tc.want {
				t.Errorf("%s\n  got  %s\n  want %s", tc.sql, got, tc.want)
			}
		})
	}

	// The PROJECTED form of the same shape is a loud refusal rather than an
	// answer: the fold declines (argument 1 names no type and produces a
	// value), so the call keeps nullif's FLOAT64 fallback and the decimal
	// text has nowhere to go. That is what this engine answered before
	// ADR-0024 too — a scalar subquery has no declaration for the planner to
	// read, which is the gap TODO(#555) closes.
	if _, err := db.Query(context.Background(),
		"SELECT NULLIF(a, (SELECT b FROM "+ddrTable+" WHERE id = 1)) AS c FROM "+ddrTable); err == nil {
		t.Error("a projected NULLIF over a scalar subquery answered; the fold must decline " +
			"while the operand has no declaration")
	}
}

// TestAggregateOverAChoiceCarriesNoTypmod: PostgreSQL gives every aggregate
// call typmod -1, and an aggregate whose ARGUMENT is a choice expression is
// still an aggregate call. The plan cannot always type one — aggSpecOutput
// Decimal declines a non-bare-ColRef input — so gating the mark on "the plan
// says this is DECIMAL" skipped exactly those, and the wire went out carrying
// the (p,s) the runtime schema had resolved. The mark is unconditional for an
// aggregate and for a window function now (ADR-0024 item 5).
func TestAggregateOverAChoiceCarriesNoTypmod(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		sql string
		col string
	}{
		{"SELECT MAX(COALESCE(a, a)) AS m FROM " + ddrTable, "m"},
		{"SELECT MIN(COALESCE(a, a)) AS m FROM " + ddrTable, "m"},
		{"SELECT SUM(COALESCE(a, b)) AS m FROM " + ddrTable, "m"},
		{"SELECT MAX(GREATEST(a, a)) AS m FROM " + ddrTable, "m"},
		{"SELECT MIN(CASE WHEN id > 1 THEN a ELSE a END) AS m FROM " + ddrTable, "m"},
	} {
		res := ddrQuery(t, db, tc.sql)
		var found bool
		for _, m := range res.ColumnMetas {
			if m.Name != tc.col {
				continue
			}
			found = true
			if !m.WireUnconstrained {
				t.Errorf("%s: WireUnconstrained = false; PostgreSQL declares every aggregate "+
					"call unconstrained numeric", tc.sql)
			}
		}
		if !found {
			t.Fatalf("%s: column %q not in result", tc.sql, tc.col)
		}
	}
}
