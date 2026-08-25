package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The cross-scale set-operation fixture (#533).
//
// A DECIMAL value is an UNSCALED integer plus the column's declared SCALE
// (ADR-0018 §4), and on the stage DAG the two travel apart: each arm's task
// writes its own .wshf file carrying its own scale in the header, and the
// downstream task that reads several such files writes ONE file under the
// schema of the first batch it saw. Nothing reconciled the arms, because a
// TypeID comparison calls DECIMAL(9,2) and DECIMAL(18,4) the same type — so
// the wider arm's unscaled integer was read at the narrower arm's scale and
// every value from it came back 100x too large, silently.
//
// The fixture is split deliberately into two families of DECIMAL columns:
//
//   - e2/e4/ew hold the SAME NUMBERS at scale 2, 4 and 10. Every value is a
//     whole number of hundredths, so the single-process path — which boxes
//     each row as its rendered text and re-reads it at the result schema's
//     scale — is EXACT for them at any of the three scales. They are what
//     the TWO-PATH gate compares, and they isolate #533 cleanly: the DAG's
//     defect moves them by a power of ten while the single-process path is
//     right, in either arm order.
//
//   - u4 holds values whose 3rd and 4th fractional digits are non-zero. The
//     single-process path TRUNCATES those under a scale-2 first arm — a
//     different defect, in a different component, fixed separately as #532 —
//     so they are asserted on the STAGE DAG alone, against exact big.Rat
//     expectations computed from this generator and verified against live
//     postgres:17-alpine.
//
// It rides along in tmdTables() rather than standing up a third cluster, the
// way dtpTable and ketTable already do, and no type-matrix corpus entry names
// this table.
const sodTable = "setopdec"

func sodSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "e2", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "e4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "ew", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "u4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "i8", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f8", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// sodRow is one fixture row in HUNDREDTHS for the e-family (so the same
// number can be restated at each of the three scales), plus u4's own unscaled
// value at scale 4 and the two non-DECIMAL arms.
type sodRow struct {
	id                         int64
	e2, e4, ew                 int64 // hundredths; nil-ness in the flags below
	u4                         int64 // unscaled at scale 4
	i8                         int64
	f8                         float64
	e2Nil, e4Nil, ewNil, u4Nil bool
	i8Nil, f8Nil               bool
}

// sodRows is chosen so the e2 and e4 arms OVERLAP without either containing
// the other, so NULL appears on each side and on both, and so a leading-digit
// trap is present ("2.00" sorts above "10.0000" as text, below it as a
// number).
func sodRows() []sodRow {
	return []sodRow{
		{id: 1, e2: 1275, e4: 1275, ew: 1275, u4: 127501, i8: 1, f8: 12.75},
		{id: 2, e2: 1275, e4: 300, ew: 300, u4: 127499, i8: 2, f8: 0.5},
		{id: 3, e2: -1, e4: -1, ew: -1, u4: -1, i8: -1, f8: -0.5},
		{id: 4, e2: 0, e4: 0, ew: 0, u4: 0, i8: 0, f8: 0},
		{id: 5, e2: 200, e4: 1000, ew: 1000, u4: 100001, i8: 10, f8: 2},
		{id: 6, e2: 100, e4: 100, ew: 100, u4: 1, i8: 3, f8: 1},
		{id: 7, e2Nil: true, e4: 100, ewNil: true, u4: 10000, i8: 4, f8: 1.5},
		{id: 8, e2: 300, e4Nil: true, ew: 300, u4Nil: true, i8: 5, f8: -2.5},
		{id: 9, e2Nil: true, e4Nil: true, ewNil: true, u4Nil: true, i8Nil: true, f8Nil: true},
	}
}

// sodDec renders an unscaled int64 as the two halves parquet.Decimal128
// carries, sign-extended the way two's complement requires.
func sodDec(v int64) parquet.Decimal128 {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	return parquet.Decimal128{Hi: hi, Lo: uint64(v)}
}

func sodData() []map[string]any {
	src := sodRows()
	rows := make([]map[string]any, 0, len(src))
	for _, r := range src {
		m := map[string]any{"id": r.id}
		if !r.e2Nil {
			m["e2"] = sodDec(r.e2)
		}
		if !r.e4Nil {
			m["e4"] = sodDec(r.e4 * 100) // hundredths -> scale 4
		}
		if !r.ewNil {
			m["ew"] = sodDec(r.ew * 100000000) // hundredths -> scale 10
		}
		if !r.u4Nil {
			m["u4"] = sodDec(r.u4)
		}
		if !r.i8Nil {
			m["i8"] = r.i8
		}
		if !r.f8Nil {
			m["f8"] = r.f8
		}
		rows = append(rows, m)
	}
	return rows
}

// sodExpect is the fixture generator restated as exact rationals: column name
// to the multiset of values it holds, NULLs counted separately. Everything the
// gates assert is derived from this, so a test cannot agree with a wrong
// engine by copying its output.
func sodExpect(col string) (vals []*big.Rat, nulls int) {
	hundredth := big.NewRat(1, 100)
	tenThousandth := big.NewRat(1, 10000)
	for _, r := range sodRows() {
		var v int64
		var isNil bool
		unit := hundredth
		switch col {
		case "e2":
			v, isNil = r.e2, r.e2Nil
		case "e4":
			v, isNil = r.e4, r.e4Nil
		case "ew":
			v, isNil = r.ew, r.ewNil
		case "u4":
			v, isNil, unit = r.u4, r.u4Nil, tenThousandth
		case "i8":
			v, isNil, unit = r.i8, r.i8Nil, big.NewRat(1, 1)
		default:
			panic("sodExpect: unknown column " + col)
		}
		if isNil {
			nulls++
			continue
		}
		vals = append(vals, new(big.Rat).Mul(new(big.Rat).SetInt64(v), unit))
	}
	return vals, nulls
}

// sodRats reads one result column as exact rationals. A DECIMAL comes back as
// its rendered text, so the number is recovered without going through
// float64 — a comparison that rounded to float64 would call 12.7501 and
// 12.75009999 the same value, which is the whole point of an exact carrier.
func sodRats(t *testing.T, arm string, rows []map[string]any, col string) (vals []*big.Rat, nulls int) {
	t.Helper()
	for _, r := range rows {
		v, present := r[col]
		if !present {
			t.Fatalf("%s: result row has no column %q (has %v)", arm, col, sodRowKeys(r))
		}
		if v == nil {
			nulls++
			continue
		}
		rat, ok := sodRat(v)
		if !ok {
			t.Fatalf("%s: column %q holds %#v (%T), which is not a number", arm, col, v, v)
		}
		vals = append(vals, rat)
	}
	return vals, nulls
}

func sodRat(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case string:
		return new(big.Rat).SetString(n)
	case int64:
		return new(big.Rat).SetInt64(n), true
	case int32:
		return new(big.Rat).SetInt64(int64(n)), true
	case float64:
		r := new(big.Rat).SetFloat64(n)
		return r, r != nil
	}
	return nil, false
}

func sodRowKeys(r map[string]any) []string {
	out := make([]string, 0, len(r))
	for k := range r {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sodSortRats orders a multiset so two of them can be compared element by
// element regardless of the order the engine emitted them in.
func sodSortRats(v []*big.Rat) []*big.Rat {
	out := append([]*big.Rat(nil), v...)
	sort.Slice(out, func(i, j int) bool { return out[i].Cmp(out[j]) < 0 })
	return out
}

func sodRatsEqual(a, b []*big.Rat) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := sodSortRats(a), sodSortRats(b)
	for i := range as {
		if as[i].Cmp(bs[i]) != 0 {
			return false
		}
	}
	return true
}

func sodShow(v []*big.Rat, nulls int) string {
	out := make([]string, 0, len(v)+1)
	for _, r := range sodSortRats(v) {
		out = append(out, r.FloatString(10))
	}
	return fmt.Sprintf("%v +%d NULL", out, nulls)
}

// sodDistinct collapses a multiset the way a set operation's membership rule
// does: by VALUE, so 12.75 at scale 2 and 12.7500 at scale 4 are one member.
func sodDistinct(v []*big.Rat) []*big.Rat {
	var out []*big.Rat
	for _, r := range sodSortRats(v) {
		if len(out) > 0 && out[len(out)-1].Cmp(r) == 0 {
			continue
		}
		out = append(out, r)
	}
	return out
}

// TestSetOpDecimalScaleTwoPath holds the single-process engine and the stage
// DAG to the same NUMBERS for a set operation whose arms are DECIMAL columns
// of DIFFERENT declared scale, and holds both to what the fixture generator
// says those numbers are.
//
// The comparison is on VALUES, not on rendered text, because the two paths
// legitimately RENDER a widened DECIMAL differently until #532 lands on this
// branch too: a wadjet DECIMAL column has one declared scale, so 12.75 from
// the narrow arm prints as "12.7500" under a scale-4 result and as "12.75"
// under a scale-2 one. Same number either way — and the defect this gate is
// for moves the number, by a factor of 100 or 10^6.
func TestSetOpDecimalScaleTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	e2, e2Nulls := sodExpect("e2")
	e4, e4Nulls := sodExpect("e4")
	ew, ewNulls := sodExpect("ew")

	cat := func(parts ...[]*big.Rat) []*big.Rat {
		var out []*big.Rat
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	// INTERSECT and EXCEPT decide membership by equality, and a NULL is a
	// member like any other that matches a NULL on the other side — the rule
	// PostgreSQL applies and the one the fixture's NULL rows exercise.
	intersect := func(a, b []*big.Rat) []*big.Rat {
		var out []*big.Rat
		for _, x := range sodDistinct(a) {
			for _, y := range b {
				if x.Cmp(y) == 0 {
					out = append(out, x)
					break
				}
			}
		}
		return out
	}
	except := func(a, b []*big.Rat) []*big.Rat {
		var out []*big.Rat
		for _, x := range sodDistinct(a) {
			found := false
			for _, y := range b {
				if x.Cmp(y) == 0 {
					found = true
					break
				}
			}
			if !found {
				out = append(out, x)
			}
		}
		return out
	}
	nullIn := func(n int) int {
		if n > 0 {
			return 1
		}
		return 0
	}

	for _, tc := range []struct {
		name      string
		sql       string
		wantVals  []*big.Rat
		wantNulls int
		// dagOnly runs the stage-DAG arm alone, for a shape whose
		// single-process answer is wrong for a DIFFERENT, separately-tracked
		// reason. Both arms are still held to the generator; what is skipped
		// is only the arms-agree comparison, which would report the OTHER
		// defect and hide this one. Every entry says which issue.
		dagOnly string
	}{
		// Two arms, narrow first: the shape the issue reports. Every value
		// from the scale-4 arm came back 100x too large.
		{"union_all_narrow_first",
			"SELECT e2 AS v FROM %[1]s UNION ALL SELECT e4 FROM %[1]s",
			cat(e2, e4), e2Nulls + e4Nulls, ""},
		// The same two arms the other way round, where the corruption runs
		// the other direction (a scale-2 arm read at scale 4, 100x too
		// small).
		{"union_all_wide_first",
			"SELECT e4 AS v FROM %[1]s UNION ALL SELECT e2 FROM %[1]s",
			cat(e4, e2), e2Nulls + e4Nulls, ""},
		// Three arms at (9,2), (18,4) and (38,10). This parses left-deep,
		// so the outer union's first arm is ITSELF a union — the shape whose
		// reconciled type the enclosing operation used to report as unknown.
		{"union_all_three_arms",
			"SELECT e2 AS v FROM %[1]s UNION ALL SELECT e4 FROM %[1]s UNION ALL SELECT ew FROM %[1]s",
			cat(e2, e4, ew), e2Nulls + e4Nulls + ewNulls, ""},
		{"union_all_three_arms_widest_first",
			"SELECT ew AS v FROM %[1]s UNION ALL SELECT e2 FROM %[1]s UNION ALL SELECT e4 FROM %[1]s",
			cat(ew, e2, e4), e2Nulls + e4Nulls + ewNulls, ""},
		// The DISTINCT forms. Their dedup key must agree with the
		// comparator: two values `=` calls equal produce ONE key, so 12.75
		// at scale 2 and 12.7500 at scale 4 are one member.
		//
		// The stage DAG keys these through the COLUMNAR encoding — a
		// GroupByAll aggregate over the concatenation, whose DECIMAL arm is
		// batch.AppendDecimalKey (#474) — so it is held to the generator
		// here. The single-process path keys a boxed row with
		// fmt.Sprintf("%v", ...), where a DECIMAL is its RENDERED TEXT and
		// "12.75" and "12.7500" are two keys for one number; that is #499,
		// a different component and a separate fix (physical.setOpKeyer).
		// These four move back to the arms-agree comparison the moment it
		// lands — the four UNION ALL entries above already run both arms and
		// are what proves the DAG's VALUES are right for the same fixture.
		{"union_distinct",
			"SELECT e2 AS v FROM %[1]s UNION SELECT e4 FROM %[1]s",
			sodDistinct(cat(e2, e4)), nullIn(e2Nulls + e4Nulls), "#499"},
		{"union_distinct_three_arms",
			"SELECT e2 AS v FROM %[1]s UNION SELECT e4 FROM %[1]s UNION SELECT ew FROM %[1]s",
			sodDistinct(cat(e2, e4, ew)), nullIn(e2Nulls + e4Nulls + ewNulls), "#499"},
		{"intersect",
			"SELECT e2 AS v FROM %[1]s INTERSECT SELECT e4 FROM %[1]s",
			intersect(e2, e4), nullIn(e2Nulls) * nullIn(e4Nulls), "#499"},
		{"except",
			"SELECT e2 AS v FROM %[1]s EXCEPT SELECT e4 FROM %[1]s",
			except(e2, e4), 0, "#499"},
		{"except_reversed",
			"SELECT e4 AS v FROM %[1]s EXCEPT SELECT e2 FROM %[1]s",
			except(e4, e2), 0, "#499"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, sodTable)
			arms := []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}}
			if tc.dagOnly != "" {
				arms = arms[1:]
				t.Logf("stage-DAG arm only: the single-process answer for this shape is wrong "+
					"for a separate reason (%s), so comparing the arms would report that instead", tc.dagOnly)
			}
			var got [2][]*big.Rat
			var gotNulls [2]int
			for i, arm := range arms {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				got[i], gotNulls[i] = sodRats(t, arm.name, rows, "v")
				if !sodRatsEqual(got[i], tc.wantVals) || gotNulls[i] != tc.wantNulls {
					t.Errorf("%s: %s\n  got  %s\n  want %s", arm.name, sql,
						sodShow(got[i], gotNulls[i]), sodShow(tc.wantVals, tc.wantNulls))
				}
			}
			if len(arms) == 2 && (!sodRatsEqual(got[0], got[1]) || gotNulls[0] != gotNulls[1]) {
				t.Errorf("the two paths disagree on %s\n  single-process %s\n  stage DAG      %s",
					sql, sodShow(got[0], gotNulls[0]), sodShow(got[1], gotNulls[1]))
			}
		})
	}
}

// TestSetOpDecimalScaleOnTheDAG asserts the stage DAG's own answers, with the
// coordinator's local fast path disabled, for the cases the single-process
// path cannot yet be held to.
//
// u4 carries digits the narrow arm's scale does not have, and the
// single-process adapter re-reads every row's rendered text at the FIRST
// arm's scale — so it truncates 12.7501 to 12.75 there. That is a separate
// defect in a separate component (#532); this gate is about the DAG, so it
// compares the DAG against the fixture generator directly rather than against
// an arm that is known to be wrong for these values.
//
// Every expectation below was checked against live postgres:17-alpine over
// the same numbers.
func TestSetOpDecimalScaleOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)

	e2, e2Nulls := sodExpect("e2")
	u4, u4Nulls := sodExpect("u4")
	i8, i8Nulls := sodExpect("i8")

	run := func(t *testing.T, sql string) []map[string]any {
		t.Helper()
		res, err := tmdRunDAG(ctx, coord, sql)
		if err != nil {
			t.Fatalf("stage DAG refused %q: %v", sql, err)
		}
		return res.Rows
	}
	check := func(t *testing.T, sql string, wantVals []*big.Rat, wantNulls int) {
		t.Helper()
		gotVals, gotNulls := sodRats(t, "dag", run(t, sql), "v")
		if !sodRatsEqual(gotVals, wantVals) || gotNulls != wantNulls {
			t.Errorf("%s\n  got  %s\n  want %s", sql, sodShow(gotVals, gotNulls), sodShow(wantVals, wantNulls))
		}
	}

	t.Run("union_all_keeps_the_wider_arms_digits", func(t *testing.T) {
		check(t, fmt.Sprintf("SELECT e2 AS v FROM %[1]s UNION ALL SELECT u4 FROM %[1]s", sodTable),
			append(append([]*big.Rat(nil), e2...), u4...), e2Nulls+u4Nulls)
	})

	t.Run("union_distinct_does_not_merge_two_different_numbers", func(t *testing.T) {
		// 12.7501 and 12.7499 are one unit of the last place either side of
		// 12.7500, and 12.75 is in the narrow arm: three distinct members
		// that a truncating union collapses into one.
		all := append(append([]*big.Rat(nil), e2...), u4...)
		check(t, fmt.Sprintf("SELECT e2 AS v FROM %[1]s UNION SELECT u4 FROM %[1]s", sodTable),
			sodDistinct(all), 1)
	})

	t.Run("intersect_matches_only_the_equal_values", func(t *testing.T) {
		// The only values the two arms share are the ones u4 holds at whole
		// hundredths: 0.0000 and 1.0000.
		want := []*big.Rat{big.NewRat(0, 1), big.NewRat(1, 1)}
		check(t, fmt.Sprintf("SELECT e2 AS v FROM %[1]s INTERSECT SELECT u4 FROM %[1]s", sodTable), want, 1)
	})

	t.Run("decimal_arm_with_an_integer_arm", func(t *testing.T) {
		// PostgreSQL resolves `numeric UNION ALL bigint` to numeric, so the
		// integers keep their VALUE: 1 is 1.0000, not 0.0001. Reading an
		// integer box as an unscaled carrier is the same class of mistake as
		// reading one arm's unscaled carrier at the other's scale.
		check(t, fmt.Sprintf("SELECT e2 AS v FROM %[1]s UNION ALL SELECT i8 FROM %[1]s", sodTable),
			append(append([]*big.Rat(nil), e2...), i8...), e2Nulls+i8Nulls)
		check(t, fmt.Sprintf("SELECT i8 AS v FROM %[1]s UNION ALL SELECT e2 FROM %[1]s", sodTable),
			append(append([]*big.Rat(nil), i8...), e2...), e2Nulls+i8Nulls)
	})

	t.Run("order_by_limit_over_the_widened_output", func(t *testing.T) {
		// A sort over the union sees the coerced values, so the smallest
		// three are the smallest three NUMBERS. Under the defect the wider
		// arm's rows were 100x their real size and the order was a different
		// order entirely.
		all := sodSortRats(append(append([]*big.Rat(nil), e2...), u4...))
		sql := fmt.Sprintf(
			"SELECT v FROM (SELECT e2 AS v FROM %[1]s UNION ALL SELECT u4 FROM %[1]s) t "+
				"WHERE v IS NOT NULL ORDER BY v LIMIT 3", sodTable)
		gotVals, _ := sodRats(t, "dag", run(t, sql), "v")
		if len(gotVals) != 3 {
			t.Fatalf("%s returned %d rows, want 3", sql, len(gotVals))
		}
		for i, want := range all[:3] {
			if gotVals[i].Cmp(want) != 0 {
				t.Errorf("%s\n  row %d = %s, want %s (full order %s)",
					sql, i, gotVals[i].FloatString(10), want.FloatString(10), sodShow(all, 0))
			}
		}
	})

	t.Run("an_arm_that_ends_in_a_join", func(t *testing.T) {
		// A join arm reaches the union stage through its own materialized
		// output, which is the path a broadcast or probe-split join takes;
		// the coercion rides the union arm's fragment either way.
		sql := fmt.Sprintf(
			"SELECT e2 AS v FROM %[1]s UNION ALL SELECT a.u4 FROM %[1]s a JOIN %[1]s b ON a.id = b.id", sodTable)
		check(t, sql, append(append([]*big.Rat(nil), e2...), u4...), e2Nulls+u4Nulls)
	})
}

// TestSetOpDecimalOverflowIsAnError holds a set operation whose widening has
// no exact carrier to an ERROR rather than a wrapped number.
//
// DECIMAL(38,10) alongside DECIMAL(38,2) needs 36 integer digits at scale 10,
// which is 46 — past the 38 an Int128 holds. ADR-0012 item 9 settled what
// happens then for SUM, and the reason is identical here: a wrapped value is
// a different number wearing the right type, and nothing downstream can see
// that it is wrong.
func TestSetOpDecimalOverflowIsAnError(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	sql := fmt.Sprintf("SELECT wide AS v FROM %[1]s UNION ALL SELECT huge FROM %[1]s", sodOvfTable)
	res, err := tmdRunDAG(ctx, coord, sql)
	if err == nil {
		t.Fatalf("%s answered %v; a value with no exact DECIMAL(38,10) carrier must fail the query, "+
			"not come back wrapped", sql, res.Rows)
	}
	t.Logf("refused, as it must be: %v", err)
}

// The overflow fixture: one column whose values need every one of its 36
// integer digits, and one at a scale that forces those digits to move.
const sodOvfTable = "setopdecovf"

func sodOvfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "wide", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "huge", Type: parquet.TypeDecimal, Precision: 38, Scale: 2, Nullable: true},
	}}
}

func sodOvfData() []map[string]any {
	// 10^37 as an unscaled DECIMAL(38,2) is 10^35 whole units; restated at
	// scale 10 it needs 10^45, which no Int128 holds.
	huge, _ := new(big.Int).SetString("10000000000000000000000000000000000000", 10)
	return []map[string]any{
		{"id": int64(1), "wide": sodDec(12345), "huge": sodBigDec(huge)},
	}
}

func sodBigDec(v *big.Int) parquet.Decimal128 {
	lo := new(big.Int).And(v, new(big.Int).SetUint64(^uint64(0)))
	hi := new(big.Int).Rsh(v, 64)
	return parquet.Decimal128{Hi: hi.Int64(), Lo: lo.Uint64()}
}
