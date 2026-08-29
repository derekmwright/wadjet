package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The cross-WIDTH KEY fixture (#615, #650, #663).
//
// A comparison (`WHERE a.x = b.y`) resolves its operand pair to a common type
// before comparing; a hash KEY was built from each side's OWN storage
// encoding. ADR-0023's invariant is that the two agree — "compares equal" and
// "keys alike" have to name one relation — so every ordered pair of numeric
// widths in a JOIN, a semi/anti join or a set-operation dedup is a place they
// could part company, and every one of them did.
//
// PostgreSQL is the authority (ADR-0012), and it uses TWO ladders here, which
// is the fact this fixture exists to hold:
//
//	OPERATOR resolution (a join / semi-join key, read off EXPLAIN VERBOSE on
//	postgres:17.11 over this exact schema):
//	    int4 = int8       -> int8            (no cast on either side)
//	    int  = float4     -> ((int)::float8) = float4, i.e. float8
//	    int  = numeric    -> ((int)::numeric) = numeric
//	    float4 = float8   -> float48eq, i.e. float8
//	    numeric = float4  -> float4 = ((numeric)::float8), i.e. float8
//	    numeric = float8  -> float8 = ((numeric)::float8), i.e. float8
//	    numeric = numeric -> numeric, exact, at either scale
//
//	SET-OPERATION resolution (`select_common_type`, pg_typeof over the union):
//	    int4 ∪ int8       -> bigint
//	    int  ∪ numeric    -> numeric
//	    int  ∪ float4     -> real            <- NOT float8
//	    numeric ∪ float4  -> real            <- NOT float8
//	    float4 ∪ float8   -> double precision
//	    numeric ∪ float8  -> double precision
//
// The two ladders disagree exactly where float4 meets an exact type: a JOIN
// widens both to float8, a set operation NARROWS the exact side to real. That
// is not a wadjet choice to make — `real` is a PREFERRED type of the numeric
// category for `select_common_type` and merely a resolvable one for an
// operator — so this fixture pins both, and the engine has to run both.
const nwkTable = "numwidth"

func nwkSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "w_key", Type: parquet.TypeInt64},
		{Name: "w_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "w_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "w_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "w_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "w_d2", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "w_d4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
}

// nwkCols is every numeric column, in the order the matrices below walk them.
func nwkCols() []string { return []string{"w_i32", "w_i64", "w_f32", "w_f64", "w_d2", "w_d4"} }

// nwkData is ten rows chosen so that EVERY ordered pair of the six columns
// has both matches and non-matches, and so that no pair's answer survives
// keying at the NARROW side's width.
//
//	0  the trivially-equal row: 2 in all six columns.
//	1  0.1 — exactly representable in NONE of them. w_f32 holds float32(0.1)
//	   and w_f64 holds 0.1, which are different float8 numbers, so `f32 = f64`
//	   must DROP this row while `f64 = d2` (0.1 = 0.10::float8) keeps it.
//	2  12.75 — exact in every carrier, at two different DECIMAL scales
//	   (12.75 and 12.7500). The row that proves a DECIMAL key is compared at
//	   the common scale rather than at either side's own (#474's shape as a
//	   JOIN key).
//	3  16777217 = 2^24+1, the first integer float32 cannot follow. `i32 = f32`
//	   must drop it (the int widens to float8 and 16777216 ≠ 16777217) while
//	   `i32 = f64` and `i32 = d4` keep it — the same row, three answers.
//	4  9007199254740993 = 2^53+1 beside the float8 9007199254740992.0.
//	   PostgreSQL compares `bigint = double precision` AS float8, so it says
//	   these are EQUAL. That answer is surprising and it is PostgreSQL's; see
//	   TestNumericWidthLossyPairMatchesPostgres, which exists so nobody
//	   "fixes" it into an exact int64 comparison.
//	5  a NEGATIVE row (-20), because a key encoding that drops or misorders
//	   the sign is invisible over a non-negative fixture.
//	6  all NULL, so LEFT JOIN padding, NOT IN's three-valued rule and the
//	   set operations' NULL-is-a-value rule each have a row to answer about.
//	7  zero, the value every "read the constant as the type's zero" defect
//	   lands on.
//	8  w_d2 12.75 beside w_d4 12.7501: two DECIMALs that agree at scale 2 and
//	   differ at scale 4, so a key built at min(scale) merges them.
//	9  int32's maximum beside int64's 2^31, so `i32 = i64` is not the
//	   identity over this fixture.
//
// The DECIMAL columns are boxed as UNSCALED int64 — the parquet writer's
// verbatim input (ADR-0018's writer corollary) — because the float and string
// boxes both route through strconv.ParseFloat and would decide the fixture's
// exactness for it.
func nwkData() []map[string]any {
	f32 := func(v float32) any { return v }
	row := func(key int64, i32, i64, ff32, ff64, d2, d4 any) map[string]any {
		return map[string]any{
			"w_key": key, "w_i32": i32, "w_i64": i64,
			"w_f32": ff32, "w_f64": ff64, "w_d2": d2, "w_d4": d4,
		}
	}
	return []map[string]any{
		row(0, int32(2), int64(2), f32(2), float64(2), int64(200), int64(20000)),
		row(1, int32(3), int64(3), f32(0.1), float64(0.1), int64(10), int64(1000)),
		row(2, int32(12), int64(12), f32(12.75), float64(12.75), int64(1275), int64(127500)),
		row(3, int32(16777217), int64(16777217), f32(16777216), float64(16777217), nil, int64(167772170000)),
		row(4, nil, int64(9007199254740993), nil, float64(9007199254740992), nil, nil),
		row(5, int32(-20), int64(-20), f32(-20), float64(-20), int64(-2000), int64(-200000)),
		row(6, nil, nil, nil, nil, nil, nil),
		row(7, int32(0), int64(0), f32(0), float64(0), int64(0), int64(0)),
		row(8, int32(13), int64(13), f32(12.75), float64(12.7501), int64(1275), int64(127501)),
		row(9, int32(2147483647), int64(2147483648), f32(1.5), float64(1.5), int64(150), int64(15000)),
	}
}

// nwkInnerWant is PostgreSQL 17.11's answer for
//
//	SELECT COUNT(*) FROM numwidth a JOIN numwidth b ON a.<x> = b.<y>
//
// over nwkData, for all 36 ordered pairs, taken live. It is the OPERATOR
// ladder above: the diagonal is each type's own width (w_f32 = w_f32 is 10,
// not 6, because float4 = float4 compares at float4 width and 12.75 = 12.75
// twice over rows 2 and 8), and every off-diagonal entry is the common type's.
var nwkInnerWant = map[string]int{
	"w_i32=w_i32": 8, "w_i32=w_i64": 7, "w_i32=w_f32": 3, "w_i32=w_f64": 4, "w_i32=w_d2": 3, "w_i32=w_d4": 4,
	"w_i64=w_i32": 7, "w_i64=w_i64": 9, "w_i64=w_f32": 3, "w_i64=w_f64": 5, "w_i64=w_d2": 3, "w_i64=w_d4": 4,
	"w_f32=w_i32": 3, "w_f32=w_i64": 3, "w_f32=w_f32": 10, "w_f32=w_f64": 6, "w_f32=w_d2": 8, "w_f32=w_d4": 6,
	"w_f64=w_i32": 4, "w_f64=w_i64": 5, "w_f64=w_f32": 6, "w_f64=w_f64": 9, "w_f64=w_d2": 7, "w_f64=w_d4": 8,
	"w_d2=w_i32": 3, "w_d2=w_i64": 3, "w_d2=w_f32": 8, "w_d2=w_f64": 7, "w_d2=w_d2": 9, "w_d2=w_d4": 7,
	"w_d4=w_i32": 4, "w_d4=w_i64": 4, "w_d4=w_f32": 6, "w_d4=w_f64": 8, "w_d4=w_d2": 7, "w_d4=w_d4": 8,
}

// nwkShapeWant is PostgreSQL 17.11's answer for the other six shapes, over
// the 30 CROSS pairs (the diagonal is covered by nwkInnerWant and by the
// same-type gates that already exist).
//
//	left   SELECT COUNT(*) FROM numwidth a LEFT JOIN numwidth b ON a.x = b.y
//	in     SELECT COUNT(*) FROM numwidth a WHERE a.x IN (SELECT b.y FROM numwidth b)
//	notIn  ... WHERE a.x NOT IN (SELECT b.y FROM numwidth b WHERE b.y IS NOT NULL)
//	exists ... WHERE EXISTS (SELECT 1 FROM numwidth b WHERE a.x = b.y)
//	isect  SELECT COUNT(*) FROM (SELECT a.x FROM numwidth a INTERSECT SELECT b.y FROM numwidth b) s
//	except SELECT COUNT(*) FROM (SELECT a.x FROM numwidth a EXCEPT    SELECT b.y FROM numwidth b) s
//
// notIn deliberately excludes the subquery's NULLs: with them, PostgreSQL's
// three-valued rule answers 0 for every one of the thirty pairs and the entry
// cannot tell a right key from a wrong one. The NULL-bearing form is asserted
// separately by TestNumericWidthNotInIsThreeValued, which is the shape that
// keeps the rule.
//
// isect and except count DISTINCT values and treat NULL as a value equal to
// itself, which is why several of them sit one above the IN-subquery entry
// beside them.
type nwkShape struct{ left, in, notIn, exists, isect, except int }

var nwkShapeWant = map[string]nwkShape{
	"w_i32/w_i64": {10, 7, 1, 7, 8, 1},
	"w_i32/w_f32": {10, 3, 5, 3, 5, 4},
	"w_i32/w_f64": {10, 4, 4, 4, 5, 4},
	"w_i32/w_d2":  {10, 3, 5, 3, 4, 5},
	"w_i32/w_d4":  {10, 4, 4, 4, 5, 4},
	"w_i64/w_i32": {10, 7, 2, 7, 8, 2},
	"w_i64/w_f32": {10, 3, 6, 3, 5, 5},
	"w_i64/w_f64": {10, 5, 4, 5, 6, 4},
	"w_i64/w_d2":  {10, 3, 6, 3, 4, 6},
	"w_i64/w_d4":  {10, 4, 5, 4, 5, 5},
	"w_f32/w_i32": {10, 3, 5, 3, 5, 3},
	"w_f32/w_i64": {10, 3, 5, 3, 5, 3},
	"w_f32/w_f64": {10, 6, 2, 6, 6, 2},
	"w_f32/w_d2":  {12, 6, 2, 6, 7, 1},
	"w_f32/w_d4":  {10, 6, 2, 6, 8, 0},
	"w_f64/w_i32": {10, 4, 5, 4, 5, 5},
	"w_f64/w_i64": {10, 5, 4, 5, 6, 4},
	"w_f64/w_f32": {11, 5, 4, 5, 6, 4},
	"w_f64/w_d2":  {11, 6, 3, 6, 7, 3},
	"w_f64/w_d4":  {10, 8, 1, 8, 9, 1},
	"w_d2/w_i32":  {10, 3, 4, 3, 4, 3},
	"w_d2/w_i64":  {10, 3, 4, 3, 4, 3},
	"w_d2/w_f32":  {12, 6, 1, 6, 7, 0},
	"w_d2/w_f64":  {10, 7, 0, 7, 7, 0},
	"w_d2/w_d4":   {10, 7, 0, 7, 7, 0},
	"w_d4/w_i32":  {10, 4, 4, 4, 5, 4},
	"w_d4/w_i64":  {10, 4, 4, 4, 5, 4},
	"w_d4/w_f32":  {11, 5, 3, 5, 8, 1},
	"w_d4/w_f64":  {10, 8, 0, 8, 9, 0},
	"w_d4/w_d2":   {11, 6, 2, 6, 7, 2},
}

// nwkPairs runs one SQL per ordered column pair on one arm and reports the
// pairs whose answer differs from PostgreSQL's, sorted, so a failure names
// the whole class rather than the first member of it.
func nwkPairs(t *testing.T, ctx context.Context, arm string, run func(string) (int, error),
	sqlFor func(x, y string) string, want func(x, y string) int) {
	t.Helper()
	var bad []string
	for _, x := range nwkCols() {
		for _, y := range nwkCols() {
			if sql := sqlFor(x, y); sql != "" {
				got, err := run(sql)
				if err != nil {
					bad = append(bad, fmt.Sprintf("%s = %s: %v (PostgreSQL: %d)", x, y, err, want(x, y)))
					continue
				}
				if w := want(x, y); got != w {
					bad = append(bad, fmt.Sprintf("%s = %s: %d, want %d", x, y, got, w))
				}
			}
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		t.Errorf("%s: %d of the width pairs disagree with PostgreSQL 17.11:\n  %s",
			arm, len(bad), strings.Join(bad, "\n  "))
	}
}

// TestNumericWidthJoinKeysMatchPostgres is the umbrella gate for #615: every
// ordered pair of numeric widths as an equi-join key, on both execution paths.
func TestNumericWidthJoinKeysMatchPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, arm := range []struct {
		name string
		dag  bool
	}{{"single", false}, {"dag", true}} {
		run := nwkRunner(t, ctx, single, coord, arm.dag)
		t.Run(arm.name+"/Inner", func(t *testing.T) {
			nwkPairs(t, ctx, arm.name, run,
				func(x, y string) string {
					return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.%s = b.%s",
						nwkTable, nwkTable, x, y)
				},
				func(x, y string) int { return nwkInnerWant[x+"="+y] })
		})
		for _, shape := range []struct {
			name string
			sql  func(x, y string) string
			want func(nwkShape) int
		}{
			{"LeftJoin", func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a LEFT JOIN %s b ON a.%s = b.%s",
					nwkTable, nwkTable, x, y)
			}, func(s nwkShape) int { return s.left }},
			{"InSubquery", func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a WHERE a.%s IN (SELECT b.%s FROM %s b)",
					nwkTable, x, y, nwkTable)
			}, func(s nwkShape) int { return s.in }},
			{"NotInNonNull", func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a WHERE a.%s NOT IN "+
					"(SELECT b.%s FROM %s b WHERE b.%s IS NOT NULL)", nwkTable, x, y, nwkTable, y)
			}, func(s nwkShape) int { return s.notIn }},
			{"Exists", func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a WHERE EXISTS "+
					"(SELECT 1 FROM %s b WHERE a.%s = b.%s)", nwkTable, nwkTable, x, y)
			}, func(s nwkShape) int { return s.exists }},
			{"Intersect", func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT a.%s AS v FROM %s a "+
					"INTERSECT SELECT b.%s FROM %s b) s", x, nwkTable, y, nwkTable)
			}, func(s nwkShape) int { return s.isect }},
			{"Except", func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT a.%s AS v FROM %s a "+
					"EXCEPT SELECT b.%s FROM %s b) s", x, nwkTable, y, nwkTable)
			}, func(s nwkShape) int { return s.except }},
		} {
			t.Run(arm.name+"/"+shape.name, func(t *testing.T) {
				nwkPairs(t, ctx, arm.name, run,
					func(x, y string) string {
						if x == y {
							return ""
						}
						return shape.sql(x, y)
					},
					func(x, y string) int { return shape.want(nwkShapeWant[x+"/"+y]) })
			})
		}
	}
}

// nwkRunner returns "run this SQL on this arm and give me the single scalar",
// with a REFUSAL reported as an error rather than a fatal: a wrong answer and
// a panic are both findings here, and one pair failing must not hide the
// other thirty-five.
func nwkRunner(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator, dag bool) func(string) (int, error) {
	t.Helper()
	return func(sql string) (int, error) {
		var (
			res *oracle.Result
			err error
		)
		if dag {
			res, err = tmdRunDAG(ctx, coord, sql)
		} else {
			res, err = tmdRunSingle(ctx, single, sql)
		}
		if err != nil {
			return 0, err
		}
		if len(res.Rows) != 1 {
			return 0, fmt.Errorf("%d rows, want 1", len(res.Rows))
		}
		v, ok := res.Rows[0]["n"]
		if !ok {
			return 0, fmt.Errorf("no column n in %v", res.Rows[0])
		}
		n, ok := v.(int64)
		if !ok {
			return 0, fmt.Errorf("count cell %#v (%T) is not an int64", v, v)
		}
		return int(n), nil
	}
}

// TestNumericWidthChainAndExpressionKeys is the shape #615 was filed with —
// an `int = float/decimal` key inside a THREE-relation chain, which took the
// inlineIntProbe fast path and panicked there rather than in the semi/anti
// probe — plus the two shapes beside it that a per-pair resolution has to get
// right: a second chain whose two links resolve to DIFFERENT common types
// (DECIMAL then FLOAT64), and a join key that is an EXPRESSION.
//
// Every expectation is PostgreSQL 17.11's over the numwidth fixture.
func TestNumericWidthChainAndExpressionKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	cases := []struct {
		name string
		sql  string
		want int
	}{
		// The two links resolve to two DIFFERENT types — a.w_i32 = b.w_f64 is
		// float8 and b.w_d2 = c.w_i64 is numeric — so one list of key types
		// per JOIN, not one per query, is the thing being asserted.
		{"ChainIntFloatThenDecimalInt", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.w_i32 = b.w_f64
			   JOIN %s c ON b.w_d2 = c.w_i64`, nwkTable, nwkTable, nwkTable), 3},
		{"ChainIntDecimalThenFloatFloat", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.w_i64 = b.w_d4
			   JOIN %s c ON b.w_f32 = c.w_f64`, nwkTable, nwkTable, nwkTable), 3},
		// An EXPRESSION key. PostgreSQL resolves it exactly as it resolves a
		// column one — `((a.w_i32 + 0))::double precision = b.w_f64` off
		// EXPLAIN VERBOSE — so the answer is the column key's answer.
		//
		// These two ALREADY passed before this fix, and they are here as the
		// other half of it: an expression is not a bare-column equality, so
		// liftInnerJoinOnResiduals moves it OUT of the ON clause into a
		// filter above a cross join, where the COMPARATOR evaluates it — and
		// the comparator was always the half that widened correctly. They
		// pin that the key path now agrees with the path that was already
		// right, which is the whole of ADR-0023 stated as a query, and they
		// fail the day a widening leaks into the comparator instead.
		{"ExpressionKeyIntPlusZeroAgainstFloat", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.w_i32 + 0 = b.w_f64`,
			nwkTable, nwkTable), 4},
		{"ExpressionKeyDecimalPlusZeroAgainstFloat", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.w_d2 + 0 = b.w_f64`,
			nwkTable, nwkTable), 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				got, err := nwkRunner(t, ctx, single, coord, arm.dag)(c.sql)
				if err != nil {
					t.Errorf("%s: %s: %v (PostgreSQL 17.11 answers %d)", arm.name, c.sql, err, c.want)
					continue
				}
				if got != c.want {
					t.Errorf("%s: %s = %d, want %d (PostgreSQL 17.11)", arm.name, c.sql, got, c.want)
				}
			}
		})
	}
}

// TestNumericWidthLossyPairMatchesPostgres pins the one answer in this family
// that looks like a defect and is not.
//
// `bigint = double precision` is resolved by PostgreSQL to float8 on BOTH
// sides, so 9007199254740993 (2^53+1, exact as a bigint) and the float8
// 9007199254740992.0 ARE EQUAL there — the bigint rounds on the way into the
// comparison and the two land on the same double. An engine that "fixed" this
// by comparing the pair exactly as int64 would answer 0 rows and would be
// wrong: it would also then have to explain why `WHERE a.w_i64 = b.w_f64`
// says something different from `a JOIN b ON a.w_i64 = b.w_f64`, which is
// the whole of #615 pointing the other way.
//
// Row 4 of numwidth exists for this and nothing else.
func TestNumericWidthLossyPairMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// The JOIN and the WHERE spelling of one predicate, which must agree —
	// ADR-0023's invariant, stated as a query.
	for _, c := range []struct {
		name string
		sql  string
	}{
		{"Join", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.w_i64 = b.w_f64 WHERE a.w_key = 4`,
			nwkTable, nwkTable)},
		{"Where", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a, %s b WHERE a.w_i64 = b.w_f64 AND a.w_key = 4`,
			nwkTable, nwkTable)},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				got, err := nwkRunner(t, ctx, single, coord, arm.dag)(c.sql)
				if err != nil {
					t.Errorf("%s: %s: %v", arm.name, c.sql, err)
					continue
				}
				if got != 1 {
					t.Errorf("%s: %s = %d, want 1 — PostgreSQL 17.11 compares "+
						"bigint = double precision AS float8, so 9007199254740993 and "+
						"9007199254740992.0 are EQUAL. This is not a rounding bug to fix.",
						arm.name, c.sql, got)
				}
			}
		})
	}
}

// TestNumericWidthNotInIsThreeValued is the NOT-IN entry the matrix cannot
// carry: with the subquery's NULLs left IN, PostgreSQL's three-valued rule
// answers 0 for every pair, so the entry cannot tell a right key from a wrong
// one — which is exactly why it belongs here, separately, rather than as a
// column of nwkShapeWant that would pass for the wrong reason.
//
// The rule is the join key's business as well as the anti join's: a widened
// key must not turn a NULL into a value, and must not lose one either.
//
// It passed before this fix too — a NULL-bearing build empties a null-aware
// anti join before any key is probed, which is why the panic never reached it
// — so it is a non-regression pin, not a fix proof. The proof for these pairs
// is the NotInNonNull column of nwkShapeWant, which failed on 26 of 30.
func TestNumericWidthNotInIsThreeValued(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, arm := range []struct {
		name string
		dag  bool
	}{{"single", false}, {"dag", true}} {
		run := nwkRunner(t, ctx, single, coord, arm.dag)
		nwkPairs(t, ctx, arm.name, run,
			func(x, y string) string {
				if x == y {
					return ""
				}
				return fmt.Sprintf(
					"SELECT COUNT(*) AS n FROM %s a WHERE a.%s NOT IN (SELECT b.%s FROM %s b)",
					nwkTable, x, y, nwkTable)
			},
			func(x, y string) int { return 0 })
	}
}

// TestNumericWidthShuffleJoinKeysMatchPostgres is the same matrix over the
// DAG's OTHER join shape: an exchange-repartition join, where the two sides
// are hash-partitioned on their own key columns before either one is read.
//
// It is a separate gate because the partition hash is a separate encoder
// (worker.hashRowsIntoPartitions) from the join key, and it fails
// differently: a cross-width pair routed at each column's own width sends
// equal values to different partitions, so the join downstream never sees
// them together and drops the pair with no error at all. Fixing the key alone
// would leave that hole, and the broadcast-join gate above cannot see it —
// a broadcast build reaches every task, so it has no partition to get wrong.
func TestNumericWidthShuffleJoinKeysMatchPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	// A ten-row build side is broadcast by every byte-based rule, so the
	// threshold is pinned at one byte rather than the fixture being grown:
	// what this gate needs is the exchange, not the size.
	coord.config.BroadcastBytesOverride = 1

	run := nwkRunner(t, ctx, nil, coord, true)

	// The join stage really is a shuffle one. Asserted rather than assumed,
	// by the artifact only a repartition leaves behind: a gate that silently
	// kept taking the broadcast path would pass for the reason the broadcast
	// gate already passes, and this whole file would then test one shape
	// twice.
	t.Run("PlanIsAShuffleJoin", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.w_i32 = b.w_f64",
			nwkTable, nwkTable)
		if _, err := run(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		infos, err := infra.store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
		if err != nil {
			t.Fatalf("listing scratch: %v", err)
		}
		for _, info := range infos {
			if strings.Contains(info.Key, "exchange-repartition") {
				return
			}
		}
		t.Fatalf("no exchange-repartition output among %d scratch objects — the fixture "+
			"did not route to a shuffle join, so this gate is testing the broadcast path twice",
			len(infos))
	})

	t.Run("Inner", func(t *testing.T) {
		nwkPairs(t, ctx, "dag-shuffle", run,
			func(x, y string) string {
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.%s = b.%s",
					nwkTable, nwkTable, x, y)
			},
			func(x, y string) int { return nwkInnerWant[x+"="+y] })
	})
	t.Run("LeftJoin", func(t *testing.T) {
		nwkPairs(t, ctx, "dag-shuffle", run,
			func(x, y string) string {
				if x == y {
					return ""
				}
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a LEFT JOIN %s b ON a.%s = b.%s",
					nwkTable, nwkTable, x, y)
			},
			func(x, y string) int { return nwkShapeWant[x+"/"+y].left })
	})
	t.Run("InSubquery", func(t *testing.T) {
		nwkPairs(t, ctx, "dag-shuffle", run,
			func(x, y string) string {
				if x == y {
					return ""
				}
				return fmt.Sprintf("SELECT COUNT(*) AS n FROM %s a WHERE a.%s IN (SELECT b.%s FROM %s b)",
					nwkTable, x, y, nwkTable)
			},
			func(x, y string) int { return nwkShapeWant[x+"/"+y].in })
	})
}
