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
		// A genuine STRING column holding numeric-looking text: the other
		// half of the pair no BOX can tell apart (#504). A DECIMAL renders
		// as text and so does this, and only the DECLARATION says which of
		// them compares as a number and which as bytes.
		{Name: "s", Type: parquet.TypeString, Nullable: true},
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
		// s carries a's number in a DIFFERENT spelling on most rows, so the
		// text rule and the numeric one disagree about all but rows 1 and 4.
		{"id": int64(1), "a": dec(1275), "b": dec(127500), "arr": arr(1275, true), "s": "12.75"},
		{"id": int64(2), "a": dec(1275), "b": dec(127501), "arr": arr(1275, true), "s": "12.7500"},
		{"id": int64(3), "a": dec(1275), "b": dec(127499), "arr": arr(1275, true), "s": "abc"},
		// "2.00" sorts above "10.0000" as TEXT and below it as a number.
		{"id": int64(4), "a": dec(200), "b": dec(100000), "arr": arr(200, true), "s": "2.00"},
		{"id": int64(5), "a": dec(-1), "b": dec(-100), "arr": arr(-1, true), "s": "-0.0100"},
		{"id": int64(6), "b": dec(10000), "arr": arr(0, false), "s": "1"},
		{"id": int64(7), "a": dec(1275), "arr": arr(1275, true), "s": "12.750"},
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

// TestDecimalBesideAnIntegerIsNumeric is #695, and it replaces the pin that
// recorded the deferral this closes.
//
// PostgreSQL resolves every choice construct over a numeric and an integer —
// a column or a literal — to numeric, verified live on 17.11 for CASE,
// COALESCE, GREATEST, LEAST and NULLIF. Wadjet declared the INTEGER instead,
// because an integer box written into a DECIMAL vector is taken as ALREADY
// SCALED (ADR-0018 §4, the parquet ingest contract) and GREATEST(a, 5) would
// have read back as 0.05. The fate of the query then depended on the DATA: it
// answered wherever the integer won every row (`GREATEST(a, 100)`, as an INT64
// column) and failed at the #361 store guard on the first row the decimal won
// (`GREATEST(a, 1)`). TPC-H Q14 is that second shape and Q08 the first.
//
// The box is what closes it: a choice whose arms fold to a DECIMAL renders its
// chosen value as TEXT — the same box a DECIMAL column and exact arithmetic
// already hand over — so the store resolves it at the output vector's own
// scale through the checked parser rather than reinterpreting a carrier.
//
// The values below are PostgreSQL's, over the same seven rows (`\gdesc` and
// the value matrix are in the commit body). The one rendering difference is
// the one a finite carrier owes: PostgreSQL's numeric carries a per-VALUE
// scale and prints the integer branch as `100`, while a DECIMAL column has ONE
// scale and prints `100.00`. Same number, and the pg-oracle compares both
// sides canonicalised.
func TestDecimalBesideAnIntegerIsNumeric(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name    string
		sql     string
		prec    int
		scale   int
		want    []string
		wantErr bool
	}{
		{
			// The shape that used to answer, as an INT64 column: the integer
			// wins every row. TPC-H Q08's silent face.
			name: "greatest over a literal the decimal never beats",
			sql:  "SELECT GREATEST(a, 100) AS v FROM " + ddrTable + " ORDER BY id",
			prec: 9, scale: 2,
			// GREATEST ignores a NULL argument on PostgreSQL, so row 6
			// answers 100 rather than NULL.
			want: []string{"100.00", "100.00", "100.00", "100.00", "100.00", "100.00", "100.00"},
		},
		{
			// The shape that used to FAIL at the store guard on row 1. TPC-H
			// Q14's loud face.
			name: "greatest over a literal the decimal beats",
			sql:  "SELECT GREATEST(a, 1) AS v FROM " + ddrTable + " ORDER BY id",
			prec: 9, scale: 2,
			want: []string{"12.75", "12.75", "12.75", "2.00", "1.00", "1.00", "12.75"},
		},
		{
			name: "coalesce with zero",
			sql:  "SELECT COALESCE(a, 0) AS v FROM " + ddrTable + " ORDER BY id",
			prec: 9, scale: 2,
			want: []string{"12.75", "12.75", "12.75", "2.00", "-0.01", "0.00", "12.75"},
		},
		{
			// A FRACTIONAL literal, whose own declaration is FLOAT64: only
			// DeclType.Exact tells it from a float COLUMN, which would make
			// the whole construct float8 as it does on PostgreSQL.
			name: "case with a fractional literal branch",
			sql:  "SELECT CASE WHEN id > 4 THEN a ELSE 1.5 END AS v FROM " + ddrTable + " ORDER BY id",
			prec: 9, scale: 2,
			want: []string{"1.50", "1.50", "1.50", "1.50", "-0.01", "", "12.75"},
		},
		{
			// A literal FINER than the column widens the fold's scale, so
			// the column's own values gain digits rather than the literal
			// losing them.
			name: "case with a literal finer than the column",
			sql:  "SELECT CASE WHEN id > 4 THEN a ELSE 0.125 END AS v FROM " + ddrTable + " ORDER BY id",
			prec: 10, scale: 3,
			want: []string{"0.125", "0.125", "0.125", "0.125", "-0.010", "", "12.750"},
		},
		{
			// NULLIF mirrors argument 0 alone, so the fold is over `a` and
			// the output keeps its (9,2) — PostgreSQL's rule for the TYPE and
			// for the typmod both.
			name: "nullif against an integer literal",
			sql:  "SELECT NULLIF(a, 0) AS v FROM " + ddrTable + " ORDER BY id",
			prec: 9, scale: 2,
			want: []string{"12.75", "12.75", "12.75", "2.00", "-0.01", "", "12.75"},
		},
		{
			// An INTEGER COLUMN, which contributes its whole RANGE at scale 0
			// (19 digits for an INT64) rather than a spelling — so the fold
			// is DECIMAL(21,2), not (9,2).
			name: "least over an integer column",
			sql:  "SELECT LEAST(a, id) AS v FROM " + ddrTable + " ORDER BY id",
			prec: 21, scale: 2,
			want: []string{"1.00", "2.00", "3.00", "2.00", "-0.01", "6.00", "7.00"},
		},
		{
			name: "greatest over an integer column",
			sql:  "SELECT GREATEST(a, id) AS v FROM " + ddrTable + " ORDER BY id",
			prec: 21, scale: 2,
			want: []string{"12.75", "12.75", "12.75", "4.00", "5.00", "6.00", "12.75"},
		},
		{
			// Three alternatives at three widths, one of them an integer
			// literal: the fold takes the widest scale and the widest
			// integer part, so no branch loses a digit.
			name: "a three-way mix of two decimals and a literal",
			sql: "SELECT CASE WHEN id < 3 THEN a WHEN id < 5 THEN b ELSE 0 END AS v FROM " +
				ddrTable + " ORDER BY id",
			prec: 18, scale: 4,
			want: []string{"12.7500", "12.7500", "12.7499", "10.0000", "0.0000", "0.0000", "0.0000"},
		},
		{
			// TPC-H Q14's expression, whose THEN branch is exact arithmetic
			// over two DECIMAL columns: (9,2) x (18,4) is DECIMAL(28,6), and
			// the literal ELSE folds in at scale 0.
			name: "the Q14 shape",
			sql: "SELECT CASE WHEN id > 3 THEN a * b ELSE 0 END AS v FROM " +
				ddrTable + " ORDER BY id",
			prec: 28, scale: 6,
			want: []string{"0.000000", "0.000000", "0.000000", "20.000000", "0.000100", "", ""},
		},
		{
			// The control that must NOT move: a FLOAT column beside a DECIMAL
			// is double precision on PostgreSQL and stays FLOAT64 here, so
			// the exact-numeric rule cannot be reading "any number".
			name: "an integer beside an integer stays integer",
			sql:  "SELECT COALESCE(id, 0) AS v FROM " + ddrTable + " ORDER BY id",
			want: []string{"1", "2", "3", "4", "5", "6", "7"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			m := res.ColumnMetas[0]
			if tc.prec == 0 {
				if m.TypeID == parquet.TypeDecimal {
					t.Errorf("declared %s(%d,%d), want a non-DECIMAL",
						m.TypeID, m.Precision, m.Scale)
				}
			} else if m.TypeID != parquet.TypeDecimal || m.Precision != tc.prec || m.Scale != tc.scale {
				t.Errorf("declared %s(%d,%d), want DECIMAL(%d,%d)",
					m.TypeID, m.Precision, m.Scale, tc.prec, tc.scale)
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

// TestDecimalChoiceUnderAnAggregateOverADerivedTable is TPC-H Q08's exact
// shape, and it is the half of #695 the type fold alone does not reach.
//
// Q08's CASE branch is a bare reference to `volume`, a column a DERIVED TABLE
// computes (`l_extendedprice * (1 - l_discount)`). The SELECT list has
// resolved through a derived table since #529 (emittedColDecls), but the
// AGGREGATE's own pre-projection was still built from inputColDecls, which
// stops at the subquery's Project — so `volume` decided nothing, the CASE
// declared its integer ELSE, and the branch's DECIMAL text met an INT64
// vector at the #361 store guard on the first row the branch fired. The
// aggregate input, the SELECT list and the plan-declared schema now answer
// from one walk.
//
// Values are PostgreSQL's over the same seven rows.
func TestDecimalChoiceUnderAnAggregateOverADerivedTable(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name  string
		sql   string
		prec  int
		scale int
		want  string
	}{
		{
			name: "the Q08 numerator over a computed derived column",
			sql: "SELECT SUM(CASE WHEN s = '12.75' THEN volume ELSE 0 END) AS v FROM " +
				"(SELECT s, a * b AS volume FROM " + ddrTable + ") x",
			prec: 38, scale: 6, want: "162.562500",
		},
		{
			// A plain RENAME through the derived table, which failed for the
			// same reason: the walk stopped at the Project, not at the
			// arithmetic.
			name: "the same over a renamed derived column",
			sql: "SELECT SUM(CASE WHEN s = '12.75' THEN v ELSE 0 END) AS v FROM " +
				"(SELECT s, a AS v FROM " + ddrTable + ") x",
			prec: 38, scale: 2, want: "12.75",
		},
		{
			name: "greatest over a computed derived column",
			sql: "SELECT SUM(GREATEST(volume, 0)) AS v FROM " +
				"(SELECT a * b AS volume FROM " + ddrTable + ") x",
			prec: 38, scale: 6, want: "507.687600",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			m := res.ColumnMetas[0]
			if m.TypeID != parquet.TypeDecimal || m.Precision != tc.prec || m.Scale != tc.scale {
				t.Errorf("%s\n  declared %s(%d,%d), want DECIMAL(%d,%d)",
					tc.sql, m.TypeID, m.Precision, m.Scale, tc.prec, tc.scale)
			}
			if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != tc.want {
				t.Errorf("%s\n  = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalBesideAFloatStaysDoublePrecision is the boundary of the rule
// above, and the reason a numeric literal needed its own carrier rather than
// being read off its declared type: `CASE … THEN d ELSE 1.5 END` and
// `CASE … THEN d ELSE f END` both have a FLOAT64-declared branch, and only one
// of them is numeric on PostgreSQL.
func TestDecimalBesideAFloatStaysDoublePrecision(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ddrfloat", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		// Row 1 is NULL in the DECIMAL arm, so every construct below answers
		// from the FLOAT one and the query runs; row 2 is the shape the
		// residual below is about.
		{"id": int64(1), "f": 2.5},
		{"id": int64(2), "a": parquet.Decimal128{Lo: 1275}, "f": 20.5},
	}
	ing := db.NewIngester("ddrfloat", schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"SELECT COALESCE(a, f) AS v FROM ddrfloat WHERE id = 1",
		"SELECT GREATEST(a, f) AS v FROM ddrfloat WHERE id = 1",
		"SELECT CASE WHEN id = 2 THEN a ELSE f END AS v FROM ddrfloat WHERE id = 1",
	} {
		res := ddrQuery(t, db, sql)
		if m := res.ColumnMetas[0]; m.TypeID == parquet.TypeDecimal {
			t.Errorf("%s declared %s(%d,%d); PostgreSQL says double precision",
				sql, m.TypeID, m.Precision, m.Scale)
		}
		if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != "2.5" {
			t.Errorf("%s = %q, want 2.5", sql, got)
		}
	}

	// The FLOAT half, which was TODO(#555) until #724: a row where the
	// DECIMAL arm wins boxes its rendered TEXT, and a FLOAT64 vector could
	// not take it — the #361 guard refused the store, loudly, for a pair
	// whose declared type was already right.
	//
	// choice_decimal.go reads that text as the float the call's type names
	// now, the mirror of what #695 built for the integer half, so the row
	// answers. Row 2 holds numeric(9,2) 12.75 beside double 20.5.
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{"SELECT COALESCE(a, f) AS v FROM ddrfloat WHERE id = 2", "12.75"},
		{"SELECT GREATEST(a, f) AS v FROM ddrfloat WHERE id = 2", "20.5"},
		{"SELECT LEAST(a, f) AS v FROM ddrfloat WHERE id = 2", "12.75"},
		{"SELECT CASE WHEN id = 2 THEN a ELSE f END AS v FROM ddrfloat WHERE id = 2", "12.75"},
	} {
		res := ddrQuery(t, db, tc.sql)
		if m := res.ColumnMetas[0]; m.TypeID == parquet.TypeDecimal {
			t.Errorf("%s declared %s(%d,%d); PostgreSQL says double precision",
				tc.sql, m.TypeID, m.Precision, m.Scale)
		}
		got, ok := res.Rows[0]["v"].(float64)
		if !ok {
			t.Fatalf("%s answered %#v (%T), want a float64 — the DECIMAL arm's text has to be "+
				"read as the double the call declares", tc.sql, res.Rows[0]["v"], res.Rows[0]["v"])
		}
		if fmt.Sprintf("%v", got) != tc.want {
			t.Errorf("%s = %v, want %s (PostgreSQL 17)", tc.sql, got, tc.want)
		}
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

// TestNullifOverAStringColumnStillComparesAsText is #504's rule at the site
// the previous fix over-reached into.
//
// The numeric fallback evalNullIf gained is for a DECIMAL against an operand
// this layer CANNOT CLASSIFY — a scalar subquery, a container element. Gated
// on "either side is DECIMAL" it also fired for a DECIMAL against a genuine
// TEXT column, and then NULLIF read "12.7500" as a number where `=`,
// GREATEST, CASE … WHEN and IS DISTINCT FROM all read it as bytes: one
// commit, five sites, two answers for one question.
//
// The assertion is that INTERNAL agreement, not a fixed row set: whatever
// `s = a` decides, NULLIF must decide the same way, in both argument orders.
func TestNullifOverAStringColumnStillComparesAsText(t *testing.T) {
	db := ddrOpen(t)
	rowsOf := func(sql string) string {
		return fmt.Sprintf("%v", ddrQuery(t, db, sql).Rows)
	}
	eq := rowsOf("SELECT id FROM " + ddrTable + " WHERE s = a ORDER BY id")
	// s = a matches only where the two RENDERINGS are byte-identical:
	// "12.75" on row 1 and "2.00" on row 4. Not row 2 ("12.7500"), not row 5
	// ("-0.0100"), not row 7 ("12.750").
	if eq != "[map[id:1] map[id:4]]" {
		t.Fatalf("s = a matched %s — this test's premise is that a STRING column "+
			"compares AS TEXT (#504); if that changed, every site below changes with it", eq)
	}
	for _, sql := range []string{
		"SELECT id FROM " + ddrTable + " WHERE NULLIF(s, a) IS NULL AND s IS NOT NULL ORDER BY id",
		"SELECT id FROM " + ddrTable + " WHERE NULLIF(a, s) IS NULL AND a IS NOT NULL ORDER BY id",
	} {
		if got := rowsOf(sql); got != eq {
			t.Errorf("%s\n  got  %s\n  want %s (the rows `s = a` matches — NULLIF must read a "+
				"STRING column the way every sibling site reads it)", sql, got, eq)
		}
	}
}

// TestDecimalChoiceOverAnIntegerEXPRESSION is #695's review finding, and it is
// the shape the first cut got wrong: the DECLARED side of the fold
// (expr.declFixedPoint) takes any arm whose DeclType is INT32/INT64, while the
// COMPILED side (expr.decimalChoiceArm) classified arms by NODE KIND and knew
// only a constant, a bare column and exact arithmetic. A CAST to an integer
// and a nested choice over integers are neither, so the runtime fold declined,
// the integer box was never rendered as text, and it met the DECIMAL vector the
// PLAN had already allocated: 22003 "integer value 7 reached a DECIMAL(scale 2)
// column as a raw unscaled carrier" for a value PostgreSQL answers — on both
// paths, and on the parent commit these queries answered.
//
// Two changes, and the second is the one that makes it stay fixed. The
// classification learned the missing kinds (a CAST to an integer, a nested
// choice, a registry function declared integer), and the STORE stopped
// depending on the classification being complete: an integer box from an
// EXPRESSION is a value at scale 0, never ADR-0018 §4's encoded carrier, so
// batch.Vector.SetComputedChecked scales it instead of refusing it. A kind this
// layer has not learned now costs a narrower declared TYPE, not the query.
//
// Values are PostgreSQL 17.11's over the same seven rows.
func TestDecimalChoiceOverAnIntegerEXPRESSION(t *testing.T) {
	db := ddrOpen(t)
	intExpr := "CASE WHEN id = 99 THEN a ELSE CAST(id AS BIGINT) END"
	for _, tc := range []struct {
		name string
		sql  string
		col  string
		want []string
	}{
		{"projection", "SELECT " + intExpr + " AS v FROM " + ddrTable + " WHERE id = 1",
			"v", []string{"1.00"}},
		{"a nested choice of integers",
			"SELECT COALESCE(a, CASE WHEN id = 1 THEN 1 ELSE 2 END) AS v FROM " + ddrTable + " WHERE id = 6",
			"v", []string{"2.00"}},
		{"a CAST beside an empty aggregate",
			"SELECT COALESCE(MAX(a), CAST(0 AS BIGINT)) AS v FROM " + ddrTable + " WHERE id < 0",
			"v", []string{"0.00"}},
		{"a nested choice beside an empty aggregate",
			"SELECT COALESCE(SUM(a), CASE WHEN 1=1 THEN 0 ELSE 1 END) AS v FROM " + ddrTable + " WHERE id < 0",
			"v", []string{"0.00"}},
		{"greatest over a CAST", "SELECT GREATEST(a, CAST(id AS BIGINT)) AS v FROM " + ddrTable + " WHERE id = 1",
			"v", []string{"12.75"}},
		{"a GROUP BY key", "SELECT " + intExpr + " AS v FROM " + ddrTable + " GROUP BY 1 ORDER BY 1",
			"v", []string{"1.00", "2.00", "3.00", "4.00", "5.00", "6.00", "7.00"}},
		{"an ORDER BY key", "SELECT id FROM " + ddrTable + " ORDER BY " + intExpr + " LIMIT 2",
			"id", []string{"1", "2"}},
		{"an aggregate input", "SELECT MAX(" + intExpr + ") AS v FROM " + ddrTable,
			"v", []string{"7.00"}},
		{"a DISTINCT key", "SELECT DISTINCT " + intExpr + " AS v FROM " + ddrTable + " ORDER BY 1",
			"v", []string{"1.00", "2.00", "3.00", "4.00", "5.00", "6.00", "7.00"}},
		{"a window PARTITION key",
			"SELECT COUNT(*) OVER (PARTITION BY " + intExpr + ") AS v FROM " + ddrTable + " WHERE id = 1",
			"v", []string{"1"}},
		{"a set-operation arm",
			"SELECT " + intExpr + " AS v FROM " + ddrTable + " WHERE id = 1" +
				" UNION ALL SELECT a FROM " + ddrTable + " WHERE id = 4",
			"v", []string{"1.00", "2.00"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			var got []string
			for _, r := range res.Rows {
				got = append(got, fmt.Sprintf("%v", r[tc.col]))
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("%s\n  got  %v\n  want %v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalChoiceOverAHighScaleColumn is the other half of the same review
// finding, and it is why the p>38 ADJUSTMENT of ADR-0024 item 3 is NOT applied
// to a choice.
//
// `GREATEST(numeric(38,30), bigint)` raised 22003 before: the integer arm's box
// met the DECIMAL vector, exactly as above. The reduction rule would also have
// "fixed" it — intDigits 19 at scale 30 is 49, and item 3 gives (38,19) — but
// applying it to a CHOICE is unsound, because a choice's result IS a stored
// value: over `GREATEST(numeric(38,0), numeric(11,10))` the same rule reduces
// the scale to 6 and silently truncates the second column's 0.0000000001.
// Item 7's reasoning governs a choice, not item 3's, so the type keeps its
// scale and a value with no carrier is a per-value 22003 at the store — which
// is what lets every value that FITS answer, PostgreSQL's answer included.
func TestDecimalChoiceOverAHighScaleColumn(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "w", Type: parquet.TypeDecimal, Precision: 38, Scale: 30, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "wscale", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("wscale", schema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(5), "w": "1.5"},
		{"id": int64(7), "w": "2.25"},
	}); err != nil {
		t.Fatal(err)
	}
	// A second table whose integer genuinely has NO carrier at scale 30:
	// 10^9 restated there is 10^39, past the Int128. It is the row that makes
	// this test able to fail — with item 3's adjustment the type would be
	// (38,19), the value would fit, and the refusal below would disappear
	// along with the truncation the adjustment causes elsewhere.
	if err := db.CreateTable(ctx, "wscale2", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing2 := db.NewIngester("wscale2", schema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ing2.Ingest(ctx, []map[string]any{{"id": int64(1000000000), "w": "1.5"}}); err != nil {
		t.Fatal(err)
	}
	if err := ing2.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		// PostgreSQL answers 5 and 7 — the bigint wins both rows.
		{"SELECT GREATEST(w, id) AS v FROM wscale ORDER BY id",
			[]string{"5.000000000000000000000000000000", "7.000000000000000000000000000000"}},
		{"SELECT LEAST(w, id) AS v FROM wscale ORDER BY id",
			[]string{"1.500000000000000000000000000000", "2.250000000000000000000000000000"}},
	} {
		res := ddrQuery(t, db, tc.sql)
		if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Scale != 30 {
			t.Errorf("%s declared %s(%d,%d), want DECIMAL scale 30 — the column's own scale, "+
				"never reduced: a choice returns a STORED value", tc.sql, m.TypeID, m.Precision, m.Scale)
		}
		var got []string
		for _, r := range res.Rows {
			got = append(got, fmt.Sprintf("%v", r["v"]))
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s\n  got  %v\n  want %v", tc.sql, got, tc.want)
		}
	}

	// The value with no carrier at the declared scale is a LOUD per-value
	// 22003, never a silently reduced scale. PostgreSQL answers 1000000000
	// here; wadjet's 128-bit carrier cannot hold it at scale 30, and item 7's
	// position is that the error is the honest answer.
	_, err = db.Query(ctx, "SELECT GREATEST(w, id) AS v FROM wscale2")
	if err == nil {
		t.Error("a value with no Int128 at the declared scale was ANSWERED — if the " +
			"(p,s) rule changed, check that it did not also truncate a stored value " +
			"(TestDecimalChoiceExpressionRefusesAValueWithNoCarrier is the other half)")
	} else if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003 numeric_value_out_of_range: %v", got, err)
	}
}

// TestNullifTypesOverBothArgumentsWhenOnlyTheSecondIsDecimal is the review's
// P2: PostgreSQL resolves NULLIF's TYPE with select_common_type over BOTH
// arguments while the TYPMOD comes from argument 0 alone, and wadjet folded
// both questions over the typmod list. `NULLIF(0, numeric(9,2))` therefore
// declared INT64 where PostgreSQL says numeric.
//
// The widening is deliberately narrow — it fires only when the candidate list's
// answer is NOT a DECIMAL and another argument decided one. Folding every
// argument would widen `NULLIF(a, b)` to (18,4) for a result that is always
// a's value, which the corpus pins the other way, and would re-open the
// Guessed/Decided contract of #331/#333.
func TestNullifTypesOverBothArgumentsWhenOnlyTheSecondIsDecimal(t *testing.T) {
	db := ddrOpen(t)
	res := ddrQuery(t, db, "SELECT NULLIF(0, a) AS v FROM "+ddrTable+" WHERE id = 1")
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeDecimal || m.Precision != 9 || m.Scale != 2 {
		t.Errorf("NULLIF(0, a) declared %s(%d,%d), want DECIMAL(9,2); PostgreSQL says numeric",
			m.TypeID, m.Precision, m.Scale)
	}
	if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != "0.00" {
		t.Errorf("NULLIF(0, a) = %q, want 0.00", got)
	}
	// The control that must NOT move: over two DECIMALs the fold stays on
	// argument 0 alone, so the output keeps a's own (9,2).
	res = ddrQuery(t, db, "SELECT NULLIF(a, b) AS v FROM "+ddrTable+" WHERE id = 2")
	if m := res.ColumnMetas[0]; m.Precision != 9 || m.Scale != 2 {
		t.Errorf("NULLIF(a, b) declared (%d,%d), want (9,2) — the result is argument 0's value",
			m.Precision, m.Scale)
	}
}

// TestWideNumericLiteralInAChoiceStaysFloat corrects a record. #695's first
// pass described the wide-literal deferral as "a FLOAT64 declaration and the
// #361 store refusal", i.e. loud. It is not loud: the whole expression declares
// FLOAT64 and ANSWERS a rounded double, so
// `GREATEST(numeric(18,4), 493827160549382.7160549350)` comes back as
// 4.938271605493827e+14 where PostgreSQL answers the literal exactly.
//
// The cause is the box: compileLit puts a float64 in it past a double's ~17
// significant digits, and a choice hands over whatever box the winning arm
// produced. Arithmetic is exact for the same literal because it reads Lit.Text
// (ADR-0012 item 6). Closing it means giving the choice constructs an
// exact-text path for a constant arm; until then the deferral is a SILENT loss
// of digits, recorded here rather than described as something safer.
// TODO(#555): this pin flips when the choice path reads the literal's text.
func TestWideNumericLiteralInAChoiceStaysFloat(t *testing.T) {
	db := ddrOpen(t)
	res := ddrQuery(t, db,
		"SELECT GREATEST(b, 493827160549382.7160549350) AS v FROM "+ddrTable+" WHERE id = 1")
	m := res.ColumnMetas[0]
	if m.TypeID == parquet.TypeDecimal {
		t.Fatalf("GREATEST over a wide literal declared %s(%d,%d) — the exact-text path has "+
			"landed, so delete this pin and assert 493827160549382.7160549350",
			m.TypeID, m.Precision, m.Scale)
	}
	if got := fmt.Sprintf("%v", res.Rows[0]["v"]); got != "4.938271605493827e+14" {
		t.Errorf("value = %q, want the rounded double 4.938271605493827e+14 "+
			"(PostgreSQL answers 493827160549382.7160549350)", got)
	}
}

// TestNullifTypmodFollowsArgumentZeroNotTheFoldedType is the wire-only defect
// #695's re-review found, and it exists only because the TYPE fold learned to
// widen: `NULLIF(i64, d92)` now DECLARES DECIMAL(21,2) (PostgreSQL types it
// numeric), and its TYPMOD folds over argument 0 alone — a bare INT64 column,
// which carries no numeric modifier. Those two answers disagree, PostgreSQL
// sends typmod -1, and wadjet sent numeric(21,2). On the parent the column
// declared INT64, so there was no numeric modifier to get wrong.
//
// The guard is general rather than NULLIF-shaped: pgTypeMod sends the DECLARED
// (p,s) whenever the fold says "keeps", so a fold answering a modifier the
// column does not declare puts a number on the wire that came from nowhere.
// projectionKeepsTypmod compares the two and declares unconstrained when they
// differ.
//
// Every row verified live against PostgreSQL 17.11 with \gdesc.
func TestNullifTypmodFollowsArgumentZeroNotTheFoldedType(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		sql string
		// wantUnconstrained is PostgreSQL's answer: true where \gdesc says
		// plain `numeric`, false where it names (p,s).
		wantUnconstrained   bool
		wantPrec, wantScale int
	}{
		// INT-FIRST: argument 0 carries no numeric modifier, so the result
		// carries none — whatever the TYPE fold resolved.
		{"SELECT NULLIF(id, a) AS v FROM " + ddrTable + " WHERE id = 1", true, 21, 2},
		{"SELECT NULLIF(0, a) AS v FROM " + ddrTable + " WHERE id = 1", true, 9, 2},
		// DECIMAL-FIRST: argument 0 carries its own, and NULLIF is the one
		// construct that keeps it (GREATEST/COALESCE over the same pair drop
		// to -1 because they fold every argument).
		{"SELECT NULLIF(a, id) AS v FROM " + ddrTable + " WHERE id = 1", false, 9, 2},
		{"SELECT NULLIF(a, b) AS v FROM " + ddrTable + " WHERE id = 2", false, 9, 2},
		// The controls: these fold every argument, so a non-DECIMAL one makes
		// them disagree and drop the modifier on both engines.
		{"SELECT COALESCE(id, a) AS v FROM " + ddrTable + " WHERE id = 1", true, 21, 2},
		{"SELECT GREATEST(id, a) AS v FROM " + ddrTable + " WHERE id = 1", true, 21, 2},
	} {
		res := ddrQuery(t, db, tc.sql)
		m := res.ColumnMetas[0]
		if m.TypeID != parquet.TypeDecimal || m.Precision != tc.wantPrec || m.Scale != tc.wantScale {
			t.Errorf("%s declared %s(%d,%d), want DECIMAL(%d,%d)",
				tc.sql, m.TypeID, m.Precision, m.Scale, tc.wantPrec, tc.wantScale)
		}
		if m.WireUnconstrained != tc.wantUnconstrained {
			t.Errorf("%s: WireUnconstrained = %v, want %v — the modifier a column carries "+
				"must be the one it declares", tc.sql, m.WireUnconstrained, tc.wantUnconstrained)
		}
	}
}
