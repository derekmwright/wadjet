package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
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
		// f4 mirrors f8's values as a REAL. Every one of them is a binary
		// fraction (12.75, 0.5, -0.5, 0, 2, 1, 1.5, -2.5), so float32 and
		// float64 hold the identical number and a value comparison across the
		// two paths is exact — what differs between float4 and float8 here is
		// the declared TYPE, which is the point of the rung (#541 follow-up).
		{Name: "f4", Type: parquet.TypeFloat32, Nullable: true},
		// rd carries e2's DECIMAL(9,2) values as a ROW FIELD. A field path is
		// not the bare reference it looks like — nothing downstream resolves
		// one by name (ADR-0022) — so `rd.d` is the set-operation arm shape
		// that reaches the type walk needing a FIELD's declaration rather
		// than a column's (#551's class, one level in).
		{Name: "rd", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		}},
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
			m["rd"] = map[string]any{"d": sodDec(r.e2)}
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
			m["f4"] = float32(r.f8)
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
	case float32:
		// A FLOAT32 result column boxes as a float32 (Vector.GetValue), and
		// widening it to float64 here is exact — the rational is the number
		// the float32 holds, not a re-rounding of it.
		r := new(big.Rat).SetFloat64(float64(n))
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
	i8, i8Nulls := sodExpect("i8")

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
		// A DECIMAL arm beside an INTEGER one. `numeric ∪ bigint` is numeric
		// in PostgreSQL and an integer is a VALUE at scale 0, so 1 is 1.00 —
		// not 0.01, which is what reading the integer box as the DECIMAL's
		// already-scaled carrier gives (ADR-0018 §4's ingest rule applied
		// where it does not belong). These two were asserted on the stage DAG
		// alone because the single-process adapter did not reconcile arms of
		// different TypeID at all (#541); it now resolves the type through
		// the same setOpWiden / setOpDecimalTarget the DAG uses and MOVES
		// each arm's boxes into it, so the arms-agree comparison is the proof.
		{"decimal_arm_with_an_integer_arm",
			"SELECT e2 AS v FROM %[1]s UNION ALL SELECT i8 FROM %[1]s",
			cat(e2, i8), e2Nulls + i8Nulls, ""},
		{"integer_arm_with_a_decimal_arm",
			"SELECT i8 AS v FROM %[1]s UNION ALL SELECT e2 FROM %[1]s",
			cat(i8, e2), e2Nulls + i8Nulls, ""},
		// The DISTINCT forms. Their dedup key must agree with the
		// comparator: two values `=` calls equal produce ONE key, so 12.75
		// at scale 2 and 12.7500 at scale 4 are one member.
		//
		// The stage DAG keys these through the COLUMNAR encoding — a
		// GroupByAll aggregate over the concatenation, whose DECIMAL arm is
		// batch.AppendDecimalKey (#474). The single-process path used to key
		// a boxed row with fmt.Sprintf("%v", ...), where a DECIMAL is its
		// RENDERED TEXT and "12.75" and "12.7500" are two keys for one
		// number, so these five ran on the DAG alone (#499). They are on
		// BOTH arms now: physical.setOpKeyer keys through the same encoding
		// the DAG does, and the arms-agree comparison is what proves it.
		{"union_distinct",
			"SELECT e2 AS v FROM %[1]s UNION SELECT e4 FROM %[1]s",
			sodDistinct(cat(e2, e4)), nullIn(e2Nulls + e4Nulls), ""},
		{"union_distinct_three_arms",
			"SELECT e2 AS v FROM %[1]s UNION SELECT e4 FROM %[1]s UNION SELECT ew FROM %[1]s",
			sodDistinct(cat(e2, e4, ew)), nullIn(e2Nulls + e4Nulls + ewNulls), ""},
		{"intersect",
			"SELECT e2 AS v FROM %[1]s INTERSECT SELECT e4 FROM %[1]s",
			intersect(e2, e4), nullIn(e2Nulls) * nullIn(e4Nulls), ""},
		{"except",
			"SELECT e2 AS v FROM %[1]s EXCEPT SELECT e4 FROM %[1]s",
			except(e2, e4), 0, ""},
		{"except_reversed",
			"SELECT e4 AS v FROM %[1]s EXCEPT SELECT e2 FROM %[1]s",
			except(e4, e2), 0, ""},
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

// TestSetOpFloat32ArmsTwoPath holds both paths to PostgreSQL's float4 rung.
//
// float4 and float8 are BOTH preferred types of PostgreSQL's numeric category,
// so a real beats every exact type it meets and only a double precision beats
// it. Verified live on postgres:17-alpine with pg_typeof over the union
// itself, in both arm orders:
//
//	real ∪ integer / bigint  → real
//	real ∪ numeric(9,2)      → real
//	real ∪ double precision  → double precision
//
// Sharing one ladder rung with float8 made `real ∪ anything exact` answer
// double precision, which re-renders a real's 0.1 as 0.10000000149011612.
// This gate is on BOTH arms because the two paths reach the rung by different
// machinery — the DAG CASTs each arm's projection (setOpCastExpr, now with a
// REAL destination) while the single-process adapter rewrites the boxes
// (coerceSetOpArmRows) — so a rung added to one and not the other is exactly
// the drift #541 asked to make impossible.
//
// f4 mirrors f8's values, all binary fractions, so the NUMBERS are identical
// under either float width and any disagreement here is the type resolution
// and nothing else. The rendering difference the rung really costs is asserted
// on values in wadjet/setop_type_reconciliation_test.go, which has a real
// holding 0.1.
func TestSetOpFloat32ArmsTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// The f4 column restated exactly: f8's values, which float32 holds
	// without rounding.
	var f4 []*big.Rat
	var f4Nulls int
	for _, r := range sodRows() {
		if r.f8Nil {
			f4Nulls++
			continue
		}
		f4 = append(f4, new(big.Rat).SetFloat64(float64(float32(r.f8))))
	}
	e2, e2Nulls := sodExpect("e2")
	i8, i8Nulls := sodExpect("i8")
	f8, f8Nulls := f4, f4Nulls // same numbers, wider column

	// An arm the union resolves to REAL is held to the float32 rounding of
	// its values, which is what PostgreSQL answers too: e2's -0.01 has no
	// float32, and `numeric(9,2) '-0.01' UNION ALL real` prints -0.01 on
	// postgres:17 because that is the shortest decimal that reads back as
	// float32(-0.01). Comparing against the exact rational instead would
	// demand a precision neither engine claims.
	asReal := func(vals []*big.Rat) []*big.Rat {
		out := make([]*big.Rat, 0, len(vals))
		for _, v := range vals {
			f, _ := v.Float32()
			out = append(out, new(big.Rat).SetFloat64(float64(f)))
		}
		return out
	}
	e2AsReal := asReal(e2)
	i8AsReal := asReal(i8)

	cat := func(a, b []*big.Rat) []*big.Rat {
		return append(append([]*big.Rat(nil), a...), b...)
	}

	for _, tc := range []struct {
		name      string
		sql       string
		wantVals  []*big.Rat
		wantNulls int
	}{
		{"real_with_bigint", "SELECT f4 AS v FROM %[1]s UNION ALL SELECT i8 FROM %[1]s",
			cat(f4, i8AsReal), f4Nulls + i8Nulls},
		{"bigint_with_real", "SELECT i8 AS v FROM %[1]s UNION ALL SELECT f4 FROM %[1]s",
			cat(i8AsReal, f4), f4Nulls + i8Nulls},
		{"real_with_decimal", "SELECT f4 AS v FROM %[1]s UNION ALL SELECT e2 FROM %[1]s",
			cat(f4, e2AsReal), f4Nulls + e2Nulls},
		{"decimal_with_real", "SELECT e2 AS v FROM %[1]s UNION ALL SELECT f4 FROM %[1]s",
			cat(e2AsReal, f4), f4Nulls + e2Nulls},
		// The one rung that DOES widen: only float8 outranks float4.
		{"real_with_double", "SELECT f4 AS v FROM %[1]s UNION ALL SELECT f8 FROM %[1]s",
			cat(f4, f8), f4Nulls + f8Nulls},
		{"double_with_real", "SELECT f8 AS v FROM %[1]s UNION ALL SELECT f4 FROM %[1]s",
			cat(f8, f4), f4Nulls + f8Nulls},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, sodTable)
			sv, sn := sodRats(t, "single", dtpRun(t, ctx, single, coord, sql, false), "v")
			dv, dn := sodRats(t, "dag", dtpRun(t, ctx, single, coord, sql, true), "v")
			for _, arm := range []struct {
				name  string
				vals  []*big.Rat
				nulls int
			}{{"single", sv, sn}, {"dag", dv, dn}} {
				if !sodRatsEqual(arm.vals, tc.wantVals) || arm.nulls != tc.wantNulls {
					t.Errorf("%s: %s\n  got  %s\n  want %s", arm.name, sql,
						sodShow(arm.vals, arm.nulls), sodShow(tc.wantVals, tc.wantNulls))
				}
			}
			if !sodRatsEqual(sv, dv) || sn != dn {
				t.Errorf("the two paths disagree on %s\n  single-process %s\n  stage DAG      %s",
					sql, sodShow(sv, sn), sodShow(dv, dn))
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

	t.Run("decimal_arm_with_an_integer_arm_against_the_wide_arm", func(t *testing.T) {
		// PostgreSQL resolves `numeric UNION ALL bigint` to numeric, so the
		// integers keep their VALUE: 1 is 1.0000, not 0.0001. Reading an
		// integer box as an unscaled carrier is the same class of mistake as
		// reading one arm's unscaled carrier at the other's scale.
		//
		// The e2 pair moved to TestSetOpDecimalScaleTwoPath when the
		// single-process path learned to reconcile arm TYPES (#541). This one
		// stays here because u4 carries digits the narrow arm's scale does not
		// have, which is #532's territory rather than this issue's.
		check(t, fmt.Sprintf("SELECT u4 AS v FROM %[1]s UNION ALL SELECT i8 FROM %[1]s", sodTable),
			append(append([]*big.Rat(nil), u4...), i8...), u4Nulls+i8Nulls)
		check(t, fmt.Sprintf("SELECT i8 AS v FROM %[1]s UNION ALL SELECT u4 FROM %[1]s", sodTable),
			append(append([]*big.Rat(nil), i8...), u4...), u4Nulls+i8Nulls)
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

// The overflow fixture: columns whose widening has no exact Int128, in the
// two shapes that reach it.
//
//   - wide/huge: DECIMAL(38,10) beside DECIMAL(38,2). 36 integer digits at
//     scale 10 is 46 digits, past the 38 an Int128 holds.
//   - d380/d1110: DECIMAL(38,0) holding 10^30 beside DECIMAL(11,10). The
//     integer part is ALREADY near the carrier's limit, so any scale at all
//     on the other arm pushes it out. That is the shape showing the 38-digit
//     cap is a RANGE REDUCTION, not only a rounding rule (#552).
const sodOvfTable = "setopdecovf"

func sodOvfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "wide", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "huge", Type: parquet.TypeDecimal, Precision: 38, Scale: 2, Nullable: true},
		{Name: "d380", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		{Name: "d1110", Type: parquet.TypeDecimal, Precision: 11, Scale: 10, Nullable: true},
	}}
}

func sodOvfData() []map[string]any {
	// 10^37 as an unscaled DECIMAL(38,2) is 10^35 whole units; restated at
	// scale 10 it needs 10^45, which no Int128 holds.
	huge, _ := new(big.Int).SetString("10000000000000000000000000000000000000", 10)
	// 10^30: a value both engines hold comfortably at scale 0, and neither
	// this carrier nor a 38-digit declaration holds at scale 10.
	e30, _ := new(big.Int).SetString("1000000000000000000000000000000", 10)
	return []map[string]any{
		{"id": int64(1), "wide": sodDec(12345), "huge": sodBigDec(huge),
			"d380": sodBigDec(e30), "d1110": sodDec(10000000001)},
		{"id": int64(2), "wide": sodDec(1), "huge": sodDec(1),
			"d380": sodDec(7), "d1110": sodDec(20000000000)},
	}
}

func sodBigDec(v *big.Int) parquet.Decimal128 {
	lo := new(big.Int).And(v, new(big.Int).SetUint64(^uint64(0)))
	hi := new(big.Int).Rsh(v, 64)
	return parquet.Decimal128{Hi: hi.Int64(), Lo: lo.Uint64()}
}

// The join-arm fixture (#551): the SAME column name at two different (p,s),
// in two tables, so a set-operation arm that ends in a JOIN of them reaches
// the type walk with a name the two sides disagree about.
const (
	sodJoinA = "setopdecja"
	sodJoinB = "setopdecjb"
)

func sodJoinSchema(precision, scale int) parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "dx", Type: parquet.TypeDecimal, Precision: precision, Scale: scale, Nullable: true},
	}}
}

// sodJoinData is four rows of the SAME NUMBER at whichever scale the column
// declares, so any difference in the answer is the scale being misread and
// nothing else.
func sodJoinData(unscaled int64) []map[string]any {
	rows := make([]map[string]any, 0, 4)
	for i := 1; i <= 4; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "dx": sodDec(unscaled)})
	}
	return rows
}

// The unconstrained-DECIMAL fixture: a column carrying #458's sentinel,
// Precision 0, which is NOT a declaration — taken at face value it would build
// an output vector at scale 0 and read every value back a hundredfold out.
// It is what gives the plan-time refusal an end-to-end witness.
const sodUncTable = "setopdecunc"

func sodUncSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "u", Type: parquet.TypeDecimal, Precision: 0, Scale: 2, Nullable: true},
	}}
}

func sodUncData() []map[string]any {
	return []map[string]any{{"id": int64(1), "u": sodDec(1275)}}
}

// TestSetOpUnresolvableDecimalArmIsRefused is the SQL witness for the plan-time
// refusal — the thing an assertion over a hand-built arm state cannot be.
//
// A set operation MOVES every arm into one DECIMAL(p,s). Where an arm's type
// or its (p,s) cannot be resolved there is nothing to move it to, and leaving
// it as written is a SILENT WRONG ANSWER: each arm writes its own .wshf at its
// own scale and the stage that reads several of them takes the first header's.
// ADR-0012 item 12 calls refusing "the honest interim", and these are the two
// shapes that reach it.
//
// The single-process path ANSWERS both — it re-reads each row's rendered text
// under a max(scale) fallback, which moves no value there — so this is a
// refusal PostgreSQL and one wadjet path both answer. It is recorded as such
// in ADR-0012 item 12 rather than hidden.
func TestSetOpUnresolvableDecimalArmIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name, sql, wantIn string
	}{
		// No (p,s): the arm IS typed DECIMAL and carries the #458
		// "unconstrained" sentinel, so setOpDecimalTarget declines.
		{"an_unconstrained_decimal_arm",
			fmt.Sprintf("SELECT u AS v FROM %[1]s UNION ALL SELECT e2 FROM %[2]s", sodUncTable, sodTable),
			`result column "v" is DECIMAL in arm 1 (u)`},
		{"an_unconstrained_decimal_arm_second",
			fmt.Sprintf("SELECT e2 AS v FROM %[2]s UNION ALL SELECT u FROM %[1]s", sodUncTable, sodTable),
			`result column "v" is DECIMAL in arm 2 (u)`},
		// No TYPE at all, beside a DECIMAL arm: a ROW field path over a JOIN.
		// The join's output names its ROW column `a.rd` while the SELECT list
		// wrote `rd.d`, so the field cannot be resolved for this arm — and a
		// DECIMAL sibling makes "leave it alone" the wrong answer.
		{"an_untyped_arm_beside_a_decimal_arm",
			fmt.Sprintf("SELECT rd.d AS v FROM %[1]s a JOIN %[2]s b ON a.id = b.id UNION ALL SELECT e4 FROM %[1]s",
				sodTable, sodJoinB),
			`result column "v" is DECIMAL in one arm`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The control: the single-process engine answers, so the refusal
			// is about what the DAG can RECONCILE and not about the query
			// being illegal.
			if _, err := tmdRunSingle(ctx, single, tc.sql); err != nil {
				t.Fatalf("the single-process arm is the control and must answer %q: %v", tc.sql, err)
			}
			res, err := tmdRunDAG(ctx, coord, tc.sql)
			if err == nil {
				t.Fatalf("the stage DAG answered %v.\nIf the arm walk now resolves this shape, the "+
					"refusal is no longer its answer: move the query into the two-path gate above "+
					"and assert its values.", res.Rows)
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Errorf("the refusal must name the column and localize the arm.\n  got:  %v\n  want it to contain: %s",
					err, tc.wantIn)
			}
			t.Logf("refused, as it must be: %v", err)
		})
	}
}

// TestSetOpDecimalJoinArmsTwoPath is the gate that replaced #551's pin.
//
// An arm that ends in a JOIN used to reach the type walk through
// inputColTypes / inputColDecimal, which merge the two sides and DELETE any
// name they disagree about. For a TypeID that is right — two tables genuinely
// have two `dx` columns, and picking a side would answer about the wrong one.
// For a set operation it threw away the one fact being reconciled, so `dx`
// resolved to DECIMAL with no (p,s), setOpDecimalTarget declined, and both
// arms kept their own scale: the wider arm's unscaled integer read at the
// narrower arm's scale, 100x out, silently.
//
// The writer's scale check never caught it: each arm's task writes its OWN
// file, internally consistent, and the reinterpretation happens in the
// downstream stage that reads several files and takes the first header's
// scale — upstream of any writer.
//
// setOpArmDecls keeps a PER-SIDE view instead: the projection names the column
// qualified (`a.dx`), so each side's columns are keyed under its own relation
// names and the two are told apart. The gate is the ARMS-AGREE comparison,
// which is the fix's proof, plus both arms held to the fixture generator so
// they cannot agree by being wrong together.
func TestSetOpDecimalJoinArmsTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// Both join tables hold the SAME NUMBER — 12.75 at scale 2 in A and
	// 12.7500 at scale 4 in B — over ids 1..4, so any difference in the
	// answer is the scale being misread and nothing else.
	dx := big.NewRat(1275, 100)
	rep := func(n int) []*big.Rat {
		out := make([]*big.Rat, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, dx)
		}
		return out
	}
	e4, e4Nulls := sodExpect("e4")

	for _, tc := range []struct {
		name      string
		sql       string
		wantVals  []*big.Rat
		wantNulls int
	}{
		// The issue's own query: four rows through the join arm at (9,2),
		// four direct at (18,4). The DAG answered 12.75 four times then
		// 1275.00 four times; SUM(v) was 51.51 against PostgreSQL's 102.00.
		{"join_arm_then_direct",
			"SELECT v FROM (SELECT a.dx AS v FROM %[1]s a JOIN %[2]s b ON a.id = b.id " +
				"UNION ALL SELECT dx FROM %[2]s) t ORDER BY v",
			rep(8), 0},
		// The same two arms the other way round, where the misreading runs
		// the other direction.
		{"direct_then_join_arm",
			"SELECT v FROM (SELECT dx AS v FROM %[2]s " +
				"UNION ALL SELECT a.dx FROM %[1]s a JOIN %[2]s b ON a.id = b.id) t ORDER BY v",
			rep(8), 0},
		// The join arm names the WIDE side, so the coercion lands on the
		// other arm instead. Both directions have to work, or the fix is an
		// accident of which side the merge happened to keep.
		{"join_arm_names_the_wide_side",
			"SELECT b.dx AS v FROM %[1]s a JOIN %[2]s b ON a.id = b.id UNION ALL SELECT dx FROM %[1]s",
			rep(8), 0},
		// A three-way join: the qualified name has to survive a nested join,
		// where the inner join's own merged view is one of the two sides.
		{"three_way_join",
			"SELECT c.dx AS v FROM %[1]s a JOIN %[2]s b ON a.id = b.id JOIN %[2]s c ON a.id = c.id " +
				"UNION ALL SELECT dx FROM %[1]s",
			rep(8), 0},
		// The join sits inside a DERIVED TABLE inside the arm, so the walk
		// has to descend the arm's Project and find the join under it.
		{"join_inside_a_derived_table",
			"SELECT v FROM (SELECT a.dx AS v FROM %[1]s a JOIN %[2]s b ON a.id = b.id) s " +
				"UNION ALL SELECT dx FROM %[2]s",
			rep(8), 0},
		// A LEFT JOIN with the DECIMAL on the NULLABLE side: `a.id = b.id + 2`
		// matches only a.id 3 and 4, so two rows carry b.dx and two carry the
		// null padding — through the coercion, which must move a value and
		// leave a NULL alone.
		{"left_join_with_the_decimal_on_the_nullable_side",
			"SELECT b.dx AS v FROM %[1]s a LEFT JOIN %[2]s b ON a.id = b.id + 2 " +
				"UNION ALL SELECT dx FROM %[1]s",
			rep(6), 2},
		// One side of the join is a DERIVED TABLE. Its Project emits BARE
		// names, so before the scope qualifiers the merge deleted the
		// contested `dx`, `s.dx` resolved to nothing, and the arm came back
		// with no type at all — past the (p,s) refusal and straight into the
		// silent reinterpretation. SUM over this answered 5151.0000 against
		// PostgreSQL's 102.0000.
		{"a_derived_table_on_one_side_of_the_join",
			"SELECT s.dx AS v FROM (SELECT id, dx FROM %[2]s) s JOIN %[1]s a ON s.id = a.id " +
				"UNION ALL SELECT dx FROM %[1]s",
			rep(8), 0},
		// A CTE names its scope in a different place (Node.CTEName on the
		// subtree root, where a derived alias is stamped on the scans), so it
		// is asserted separately.
		{"a_cte_on_one_side_of_the_join",
			"WITH c AS (SELECT id, dx FROM %[2]s) " +
				"SELECT c.dx AS v FROM c JOIN %[1]s a ON c.id = a.id UNION ALL SELECT dx FROM %[1]s",
			rep(8), 0},
		// BOTH sides derived: neither contributes a bare name the merge can
		// keep, so both qualifiers have to answer.
		{"both_sides_of_the_join_derived",
			"SELECT s.dx AS v FROM (SELECT id, dx FROM %[2]s) s JOIN (SELECT id, dx FROM %[1]s) a " +
				"ON s.id = a.id UNION ALL SELECT dx FROM %[1]s",
			rep(8), 0},
		// The same join arm NESTED in an enclosing union, which is the shape
		// where a wrong scale would be reconciled a second time and hidden.
		{"a_derived_side_join_arm_inside_an_enclosing_union",
			"SELECT v FROM (SELECT s.dx AS v FROM (SELECT id, dx FROM %[2]s) s JOIN %[1]s a " +
				"ON s.id = a.id UNION ALL SELECT dx FROM %[1]s) t UNION ALL SELECT dx FROM %[1]s",
			rep(12), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, sodJoinA, sodJoinB)
			sodAssertBothPaths(t, ctx, single, coord, sql, tc.wantVals, tc.wantNulls)
		})
	}

	// A SELF-join of one table under two aliases, where both sides carry
	// EVERY name: the qualified keys must name the right side's column and
	// the bare merge must still be the answer for an unqualified reference.
	//
	// The join is `a.id = b.id + 1`, NOT `a.id = b.id`. An equi-join of a
	// table to itself on the same key pairs every row with itself, so `a.e2`
	// and `b.e2` are the SAME multiset and the assertion holds whichever side
	// the arm read — a gate that cannot fail. Shifting by one drops id 1 from
	// the left side and id 9 from the right, so the two spellings name
	// different multisets and reading the wrong one is visible.
	//
	// What this pair gates is the VALUES a qualified reference resolves to
	// end to end, not the (p,s) resolution: one table has one scale, so the
	// type answer is the same either way. The (p,s) half is the ja/jb cases
	// above, where the two sides genuinely disagree.
	t.Run("self_join_of_one_table_under_two_aliases", func(t *testing.T) {
		shifted, shiftedNulls := sodPickE2(func(id int64) bool { return id >= 2 })
		sql := fmt.Sprintf(
			"SELECT a.e2 AS v FROM %[1]s a JOIN %[1]s b ON a.id = b.id + 1 UNION ALL SELECT e4 FROM %[1]s",
			sodTable)
		sodAssertBothPaths(t, ctx, single, coord, sql,
			append(append([]*big.Rat(nil), shifted...), e4...), shiftedNulls+e4Nulls)
	})

	// The same shape naming the OTHER side, which under `a.id = b.id + 1`
	// carries ids 1..8 — the multiset the case above excludes.
	t.Run("self_join_naming_the_other_side", func(t *testing.T) {
		shifted, shiftedNulls := sodPickE2(func(id int64) bool { return id <= 8 })
		sql := fmt.Sprintf(
			"SELECT b.e2 AS v FROM %[1]s a JOIN %[1]s b ON a.id = b.id + 1 UNION ALL SELECT e4 FROM %[1]s",
			sodTable)
		sodAssertBothPaths(t, ctx, single, coord, sql,
			append(append([]*big.Rat(nil), shifted...), e4...), shiftedNulls+e4Nulls)
	})
}

// sodPickE2 restates the fixture's e2 column over the rows a predicate keeps,
// as exact rationals.
func sodPickE2(keep func(id int64) bool) (vals []*big.Rat, nulls int) {
	for _, r := range sodRows() {
		if !keep(r.id) {
			continue
		}
		if r.e2Nil {
			nulls++
			continue
		}
		vals = append(vals, new(big.Rat).Mul(new(big.Rat).SetInt64(r.e2), big.NewRat(1, 100)))
	}
	return vals, nulls
}

// sodAssertBothPaths runs one query on both execution paths, holds each to the
// fixture generator, and then holds the two to EACH OTHER. The arms-agree
// comparison is the part that proves a fix; the generator comparison is what
// stops the two arms from agreeing by being wrong together.
func sodAssertBothPaths(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator,
	sql string, wantVals []*big.Rat, wantNulls int) {
	t.Helper()
	var got [2][]*big.Rat
	var gotNulls [2]int
	for i, arm := range []struct {
		name string
		dag  bool
	}{{"single", false}, {"dag", true}} {
		rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
		got[i], gotNulls[i] = sodRats(t, arm.name, rows, "v")
		if !sodRatsEqual(got[i], wantVals) || gotNulls[i] != wantNulls {
			t.Errorf("%s: %s\n  got  %s\n  want %s", arm.name, sql,
				sodShow(got[i], gotNulls[i]), sodShow(wantVals, wantNulls))
		}
	}
	if !sodRatsEqual(got[0], got[1]) || gotNulls[0] != gotNulls[1] {
		t.Errorf("the two paths disagree on %s\n  single-process %s\n  stage DAG      %s",
			sql, sodShow(got[0], gotNulls[0]), sodShow(got[1], gotNulls[1]))
	}
}

// TestSetOpDecimalCapIsARangeReduction pins what the 38-digit cap costs
// (#552): a value both arms hold before the union becomes a hard failure
// after it, and neither a filter nor a LIMIT above the union rescues it,
// because the coercion runs in the arm's own fragment ahead of both.
//
// This is the honest side of a 128-bit carrier — ADR-0012 item 9 already
// settled that a value with no exact carrier is an error rather than a wrapped
// number. It is pinned so the cost is a recorded, tested position instead of a
// surprise, and so a future widening of the carrier shows up as this test
// failing.
func TestSetOpDecimalCapIsARangeReduction(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// d380 is DECIMAL(38,0) holding 10^30; d1110 is DECIMAL(11,10). The output
	// type is 38 integer digits plus scale 10 = 48, capped to DECIMAL(38,10),
	// and 10^30 at scale 10 needs 10^40.
	union := fmt.Sprintf("SELECT d380 AS v FROM %[1]s UNION ALL SELECT d1110 FROM %[1]s", sodOvfTable)
	// A numeric LITERAL arm reaches the same cap, and it is a NEW trigger: the
	// literal's own scale enters the common type, so a union that answered
	// float8 before the arm was typed numeric now resolves DECIMAL(38,10) and
	// 10^30 no longer fits. PostgreSQL answers all four rows — its numeric is
	// unbounded and never reaches this — so the cost of the 38-digit carrier
	// is now reachable from a query that names no wide column at all.
	litUnion := fmt.Sprintf("SELECT d380 AS v FROM %[1]s UNION ALL SELECT 0.1234567890 FROM %[1]s", sodOvfTable)
	for _, tc := range []struct{ name, sql string }{
		{"bare", union},
		{"a_numeric_literal_arm_supplies_the_scale", litUnion},
		{"a_literal_arm_filtered_below_the_bad_row",
			fmt.Sprintf("SELECT v FROM (%s) t WHERE v < 100", litUnion)},
		// A predicate that excludes the offending row does not save it: the
		// post-filter runs on the union stage, after the arm's coercion.
		{"filtered_below_the_bad_row", fmt.Sprintf("SELECT v FROM (%s) t WHERE v < 100", union)},
		// Nor does a LIMIT, which is its own Singleton stage further down.
		{"limited", fmt.Sprintf("SELECT v FROM (%s) t LIMIT 1", union)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The single-process arm ANSWERS all three — it is not subject to
			// the cap, because it never resolves a common type at all (#541),
			// and its answer for the 10^30 row is its own defect (#553).
			if _, err := tmdRunSingle(ctx, single, tc.sql); err != nil {
				t.Logf("single-process also refused: %v", err)
			}
			res, err := tmdRunDAG(ctx, coord, tc.sql)
			if err == nil {
				t.Errorf("the stage DAG answered %v.\nIf the carrier widened, or the coercion moved "+
					"below the post-filter, #552 is fixed: delete this entry and assert the values "+
					"against PostgreSQL, which answers all four rows.", res.Rows)
				return
			}
			if !strings.Contains(err.Error(), "numeric field overflow") {
				t.Errorf("expected the overflow refusal, got: %v", err)
			}
			t.Logf("refused, as the cap requires (#552): %v", err)
		})
	}
}

// TestSetOpDerivedTableArmsTwoPath is the gate that replaced #554's pin.
//
// setOpArmProjection took each arm's source expression from its SELECT list as
// written, and for a derived-table arm that is the name the SUBQUERY exposes —
// which the arm's MATERIALIZED output does not carry, because walkStages emits
// no stage for a Project. A rename resolved through resolveOutputRenameSource
// (#490); a COMPUTED alias had nothing to resolve to and the task failed loud
// with `column "x" does not exist in the input schema`, and even the rename
// case reached the union with no DECIMAL (p,s) — inputColDecls stops AT the
// derived table's Project — so its two arms wrote two scales into one file.
//
// setOpArmDecls descends INTO the Project, which is what the nested
// set-operation arm already did one level up, and setOpArmComputedSource
// rewrites a forwarded computed alias into the expression that builds it.
func TestSetOpDerivedTableArmsTwoPath(t *testing.T) {
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
	i8, i8Nulls := sodExpect("i8")
	cat := func(parts ...[]*big.Rat) []*big.Rat {
		var out []*big.Rat
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	// i8 + 1, the computed derived-table arm's values.
	i8Plus1 := make([]*big.Rat, 0, len(i8))
	for _, v := range i8 {
		i8Plus1 = append(i8Plus1, new(big.Rat).Add(v, big.NewRat(1, 1)))
	}
	// The rows the id filters keep, restated from the generator.
	pick := func(col string, keep func(id int64) bool) (vals []*big.Rat, nulls int) {
		for _, r := range sodRows() {
			if !keep(r.id) {
				continue
			}
			var v int64
			var isNil bool
			switch col {
			case "e2":
				v, isNil = r.e2, r.e2Nil
			case "e4":
				v, isNil = r.e4, r.e4Nil
			}
			if isNil {
				nulls++
				continue
			}
			vals = append(vals, new(big.Rat).Mul(new(big.Rat).SetInt64(v), big.NewRat(1, 100)))
		}
		return vals, nulls
	}
	lo5, lo5Nulls := pick("e2", func(id int64) bool { return id < 5 })
	le3, le3Nulls := pick("e4", func(id int64) bool { return id <= 3 })

	for _, tc := range []struct {
		name      string
		sql       string
		wantVals  []*big.Rat
		wantNulls int
	}{
		// The issue's own query: a bare rename in each arm.
		{"a_rename_in_each_arm",
			"SELECT x AS v FROM (SELECT e2 AS x FROM %[1]s) a UNION ALL " +
				"SELECT y FROM (SELECT e4 AS y FROM %[1]s) b",
			cat(e2, e4), e2Nulls + e4Nulls},
		// The same two arms the other way round: the wide arm first, so the
		// coercion lands on the other one.
		{"the_reverse_arm_order",
			"SELECT y AS v FROM (SELECT e4 AS y FROM %[1]s) b UNION ALL " +
				"SELECT x FROM (SELECT e2 AS x FROM %[1]s) a",
			cat(e4, e2), e2Nulls + e4Nulls},
		// A COMPUTED derived column, which is the shape #554 filed: `x` names
		// no column of the arm's stream at all, so the reference has to be
		// rewritten into `i8 + 1`.
		{"a_computed_derived_column",
			"SELECT x AS v FROM (SELECT i8 + 1 AS x FROM %[1]s) a UNION ALL " +
				"SELECT y FROM (SELECT e4 AS y FROM %[1]s) b",
			cat(i8Plus1, e4), i8Nulls + e4Nulls},
		// A derived table inside a derived table: the rename chain resolves
		// level by level, and the walk has to descend both Projects.
		{"a_nested_derived_table",
			"SELECT x AS v FROM (SELECT z AS x FROM (SELECT e2 AS z FROM %[1]s) i) a UNION ALL " +
				"SELECT y FROM (SELECT e4 AS y FROM %[1]s) b",
			cat(e2, e4), e2Nulls + e4Nulls},
		// A FILTER and a LIMIT inside the derived tables. The LIMIT is wider
		// than its input on purpose: `LIMIT 3` with no ORDER BY does not say
		// WHICH three rows, and a gate must not depend on that.
		{"a_filter_and_a_limit_inside_the_derived_table",
			"SELECT x AS v FROM (SELECT e2 AS x FROM %[1]s WHERE id < 5) a UNION ALL " +
				"SELECT y FROM (SELECT e4 AS y FROM %[1]s WHERE id <= 3 LIMIT 9) b",
			cat(lo5, le3), lo5Nulls + le3Nulls},
		// A CTE arm, referenced ONCE. A CTE reaches the arm walk as a derived
		// table with a named scope, so it is the same resolution — but it is
		// asserted separately because the scope name is what
		// derivedScopeBareName has to strip.
		{"a_cte_arm_referenced_once",
			"WITH c AS (SELECT e2 AS x FROM %[1]s) SELECT x AS v FROM c UNION ALL SELECT e4 FROM %[1]s",
			cat(e2, e4), e2Nulls + e4Nulls},
		// A ROW FIELD PATH arm. `rd.d` names no column of anything — nothing
		// downstream resolves a field path by name (ADR-0022) — so every
		// lookup missed and the arm reached the union untyped, which beside a
		// DECIMAL column is the same reinterpretation channel one level in.
		// The FIELD's own declaration answers, and the spec carries the type
		// because a field path is MATERIALIZED the way a computed expression
		// is.
		{"a_row_field_path_arm",
			"SELECT rd.d AS v FROM %[1]s UNION ALL SELECT e4 FROM %[1]s",
			cat(e2, e4), e2Nulls + e4Nulls},
		{"a_row_field_path_arm_second",
			"SELECT e4 AS v FROM %[1]s UNION ALL SELECT rd.d FROM %[1]s",
			cat(e4, e2), e2Nulls + e4Nulls},
		// The same field path through a DERIVED TABLE, which is the shape
		// that was SILENT: the derived table's Project types its output from
		// the field, and nothing downstream could see the scale was wrong.
		{"a_row_field_path_through_a_derived_table",
			"SELECT x AS v FROM (SELECT rd.d AS x FROM %[1]s) a UNION ALL SELECT e4 FROM %[1]s",
			cat(e2, e4), e2Nulls + e4Nulls},
		// A nested SET OPERATION behind a derived table. setOpArmProjection
		// reads a nested operation through its own result names when the
		// operation IS the arm; a derived table around it put a Project in
		// between, the walk answered nothing, all three files kept their own
		// scales and the shuffle writer refused the query.
		{"a_nested_set_operation_behind_a_derived_table",
			"SELECT v FROM (SELECT e2 AS v FROM %[1]s UNION ALL SELECT e4 FROM %[1]s) t " +
				"UNION ALL SELECT ew FROM %[1]s",
			cat(e2, e4, ew), e2Nulls + e4Nulls + ewNulls},
		// The same CTE feeding BOTH arms. Pinned as a residual until #660:
		// walkStages' CTE dedup gives the second reference a `cte-alias`
		// phantom, and flattenCTEAliases rewrote Dependencies without
		// UnionArms[i].DepStage, so the plan was REFUSED (`arm 1 names
		// producer "cte-alias-1" but Dependencies[1] is "scan-0"`). Asserted
		// on both arms now.
		{"a_cte_referenced_by_both_arms",
			"WITH c AS (SELECT e2 AS x FROM %[1]s) SELECT x AS v FROM c UNION ALL SELECT x FROM c",
			cat(e2, e2), e2Nulls * 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, sodTable)
			sodAssertBothPaths(t, ctx, single, coord, sql, tc.wantVals, tc.wantNulls)
		})
	}

	// The residuals, pinned rather than claimed covered. Both are the STAGE
	// WIRING between an arm and the stage that produces it — the union arm
	// names one producer and the stage's Dependencies name another — not the
	// arm projection this test is about, and each fails the day it starts
	// working.
	for _, tc := range []struct{ name, issue, sql string }{
		// #656's family: an ORDER BY inside a derived-table arm makes the arm
		// a merge_sort producer, which the union stage's dependency list does
		// not name. Distinct from the Filter/Project placement #656 fixed —
		// this is the set-operation emitter's own arm→producer wiring.
		{"an_order_by_inside_a_derived_table_arm", "#656",
			"SELECT x AS v FROM (SELECT e2 AS x FROM %[1]s) a UNION ALL " +
				"SELECT y FROM (SELECT e4 AS y FROM %[1]s ORDER BY id LIMIT 3) b"},
		// A field path whose ROW column was RENAMED by a derived table. The
		// arm's stream carries `rd`, the SELECT list wrote `x.d`, and the
		// rename resolver cannot map the two: `x` qualifies a ROW COLUMN, not
		// a relation, so derivedScopeBareName does not see it. Pre-existing
		// and LOUD — the task refuses a projection of a column that is not
		// there — which is why it is pinned rather than left to be found.
		{"a_field_path_whose_row_column_was_renamed", "the derived-table rename resolver (#554's family)",
			"SELECT x.d AS v FROM (SELECT rd AS x FROM %[1]s) s UNION ALL SELECT e4 FROM %[1]s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, sodTable)
			if _, err := tmdRunSingle(ctx, single, sql); err != nil {
				t.Fatalf("the single-process arm is the control and must answer %q: %v", sql, err)
			}
			if _, err := tmdRunDAG(ctx, coord, sql); err == nil {
				t.Errorf("the stage DAG now executes %q, so %s is FIXED. Delete this pin and "+
					"assert the values on both arms instead.", sql, tc.issue)
				return
			} else {
				t.Logf("known divergence, NOT gated (%s): %v", tc.issue, err)
			}
		})
	}
}

// TestSetOpNumericLiteralArmTwoPath holds a numeric LITERAL arm to the type
// PostgreSQL gives it (#665).
//
// PostgreSQL's literal rule, verified live against 17.11 with pg_typeof:
// a constant is NUMERIC when it carries a decimal point OR an exponent
// (`1.23456`, `1.`, `1e2`, `1.5e1`, `1.5e-2` are all numeric — there is no
// float8 constant syntax), and an INTEGER constant is integer, then bigint,
// then numeric once no integer type holds it. wadjet's declared-type layer
// answered float8 for every literal with a point and for every integer too
// wide for int64, so `SELECT d FROM t UNION ALL SELECT 1.23456 FROM t`
// resolved double precision and every value in the union went through a float.
//
// The fixture rows are restricted to id <> 3 so every value in play is a
// BINARY FRACTION (12.75, 2, 1, 3, 0 and the literals). The single-process
// path still resolves this union to float8 — its arm schema is the one the
// pipeline actually built, and the literal's vector is float8 until the
// declared-type layer's literal case lands — so a value that is not a binary
// fraction would report the float's expansion rather than a disagreement
// about the number. -0.01, the one such value in the fixture, is id 3.
//
// The DAG-only assertions below are the ones a value comparison across the
// two paths cannot make: that the union's result column IS numeric, and that
// a literal no float64 holds comes back EXACT.
func TestSetOpNumericLiteralArmTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// e2 and f8 over the rows the filter keeps, and the row count the literal
	// arm therefore produces.
	var e2, f8, i8Plus1, e2PlusHalf []*big.Rat
	e2Nulls, f8Nulls, kept := 0, 0, 0
	i8Nulls := 0
	for _, r := range sodRows() {
		if r.id == 3 {
			continue
		}
		kept++
		if r.e2Nil {
			e2Nulls++
		} else {
			e2 = append(e2, new(big.Rat).Mul(new(big.Rat).SetInt64(r.e2), big.NewRat(1, 100)))
		}
		if r.f8Nil {
			f8Nulls++
		} else {
			f8 = append(f8, new(big.Rat).SetFloat64(r.f8))
		}
		// The two COMPUTED arms below, as exact rationals: an integer
		// expression and a decimal one, neither of which is a passthrough the
		// worker can type from its input.
		if r.i8Nil {
			i8Nulls++
		} else {
			i8Plus1 = append(i8Plus1, new(big.Rat).Add(new(big.Rat).SetInt64(r.i8), big.NewRat(1, 1)))
		}
		if !r.e2Nil {
			e2PlusHalf = append(e2PlusHalf,
				new(big.Rat).Add(new(big.Rat).Mul(new(big.Rat).SetInt64(r.e2), big.NewRat(1, 100)),
					big.NewRat(1, 2)))
		}
	}
	lit := func(num, den int64) []*big.Rat {
		out := make([]*big.Rat, 0, kept)
		for i := 0; i < kept; i++ {
			out = append(out, big.NewRat(num, den))
		}
		return out
	}
	cat := func(parts ...[]*big.Rat) []*big.Rat {
		var out []*big.Rat
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}

	for _, tc := range []struct {
		name      string
		sql       string
		wantVals  []*big.Rat
		wantNulls int
		// wantDecimalOnTheDAG is false only where PostgreSQL resolves a
		// FLOAT: a float8 column arm beats numeric, because float8 is the
		// preferred type of the numeric category.
		wantDecimalOnTheDAG bool
	}{
		{"a_fractional_literal_arm",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 1.5 FROM %[1]s WHERE id <> 3",
			cat(e2, lit(3, 2)), e2Nulls, true},
		{"the_literal_arm_first",
			"SELECT 1.5 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT e2 FROM %[1]s WHERE id <> 3",
			cat(lit(3, 2), e2), e2Nulls, true},
		// A NEGATIVE literal is numeric too: the parser makes the sign a
		// unary operator, and reading only the unsigned spelling would let
		// the sign decide the column's type.
		{"a_negative_literal_arm",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT -1.5000 FROM %[1]s WHERE id <> 3",
			cat(e2, lit(-3, 2)), e2Nulls, true},
		// A TRAILING POINT is a decimal point: `1.` is numeric.
		{"a_trailing_point_literal_arm",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 1. FROM %[1]s WHERE id <> 3",
			cat(e2, lit(1, 1)), e2Nulls, true},
		// An EXPONENT literal is numeric in PostgreSQL, with or without a
		// decimal point — this row asserted float8 before, which was wrong
		// about PostgreSQL, not about wadjet.
		{"an_exponent_literal_arm",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 1.5e1 FROM %[1]s WHERE id <> 3",
			cat(e2, lit(15, 1)), e2Nulls, true},
		{"an_exponent_literal_arm_without_a_point",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 1e2 FROM %[1]s WHERE id <> 3",
			cat(e2, lit(100, 1)), e2Nulls, true},
		// 125e-3 is 0.125, a binary fraction, so the single-process arm's
		// float carries it exactly and the two paths can be compared on
		// VALUES. 1.5e-2 is not one — it is asserted exactly on the DAG
		// below, where the literal never becomes a float at all.
		{"a_negative_exponent_literal_arm",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 125e-3 FROM %[1]s WHERE id <> 3",
			cat(e2, lit(1, 8)), e2Nulls, true},
		// An INTEGER literal is on the integer rung: `numeric UNION ALL
		// integer` is numeric, and the integer's value is at scale 0.
		{"an_integer_literal_arm",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 7 FROM %[1]s WHERE id <> 3",
			cat(e2, lit(7, 1)), e2Nulls, true},
		// …until no integer type holds it, at which point PostgreSQL types
		// the constant numeric. 2^64 is past bigint and IS exact in float64,
		// so the two paths can be compared on values here; a 21-digit
		// constant float64 rounds is asserted on the DAG below.
		{"an_integer_literal_too_wide_for_bigint",
			"SELECT e2 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 18446744073709551616 FROM %[1]s WHERE id <> 3",
			cat(e2, sodRepeatRat(t, "18446744073709551616", kept)), e2Nulls, true},
		// A COMPUTED INTEGER arm against a COMPUTED DECIMAL one — the seam
		// #555's exact literal arms opened with the set-operation landing.
		// Both arms are expressions, so neither is a DirectCopy the worker
		// types from its input: the integer arm's DECLARED spec is what
		// builds its vector, and declaring the reconciled DECIMAL there made
		// the checked writer refuse the int box ("integer value 2 reached a
		// DECIMAL(scale 4) column as a raw unscaled carrier") before
		// DecimalCoerce could convert it. PostgreSQL:
		// `SELECT 1::bigint + 1 UNION ALL SELECT 12.75::numeric(9,2) + 0.5`
		// is numeric and moves no value (verified on 17.11).
		{"a_computed_integer_arm_against_a_computed_decimal_arm",
			"SELECT i8 + 1 AS v FROM %[1]s WHERE id <> 3 UNION ALL " +
				"SELECT e2 + 0.5 FROM %[1]s WHERE id <> 3",
			cat(i8Plus1, e2PlusHalf), i8Nulls + e2Nulls, true},
		// A FLOAT column arm beats the literal.
		{"a_float_column_arm_beats_the_literal",
			"SELECT f8 AS v FROM %[1]s WHERE id <> 3 UNION ALL SELECT 1.5 FROM %[1]s WHERE id <> 3",
			cat(f8, lit(3, 2)), f8Nulls, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, sodTable)
			sodAssertBothPaths(t, ctx, single, coord, sql, tc.wantVals, tc.wantNulls)

			dagRows := dtpRun(t, ctx, single, coord, sql, true)
			isDecimal := false
			for _, r := range dagRows {
				if v, ok := r["v"]; ok && v != nil {
					_, isDecimal = v.(string)
					break
				}
			}
			if isDecimal != tc.wantDecimalOnTheDAG {
				t.Errorf("the stage DAG's result column is decimal=%v, want %v — %s\n"+
					"a DECIMAL boxes as its rendered text and a float8 as a float64, so this is "+
					"the union's resolved TYPE and not only its values",
					isDecimal, tc.wantDecimalOnTheDAG, sql)
			}
		})
	}

	// The values #665 is really about: a literal no float64 holds. The arm's
	// projection is REWRITTEN to the literal's plain text, so the evaluator
	// never folds it into a float and the DECIMAL vector parses the digits
	// the query wrote. Declaring DECIMAL over the float box would have been
	// an exact TYPE on an already-rounded number — worse than the float8
	// column the arm had before.
	//
	// The single-process path is NOT held to these: it builds the literal
	// arm's vector from the declared-type layer, which still answers float8
	// for a fractional literal everywhere outside a set-operation arm.
	for _, tc := range []struct{ name, lit string }{
		// 1234567890123456.78 is 18 significant digits; float64 carries ~16
		// and rounds the last two to .80.
		{"seventeen_significant_digits", "1234567890123456.78"},
		// 20 fractional digits, all significant: the float is 0.1.
		{"twenty_fractional_digits", "0.10000000000000000001"},
		// An exponent form whose expansion float64 cannot hold either.
		{"an_exponent_form_no_float_holds", "1.2345678901234567890e2"},
		// A negative exponent whose expansion is not a binary fraction.
		{"a_negative_exponent_no_float_holds", "1.5e-2"},
		// A 21-digit integer constant, which PostgreSQL types numeric.
		{"an_integer_wider_than_bigint", "123456789012345678901"},
	} {
		t.Run("exact_on_the_dag_"+tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(
				"SELECT ew AS v FROM %[1]s WHERE id = 1 UNION ALL SELECT %[2]s FROM %[1]s WHERE id = 1",
				sodTable, tc.lit)
			got, _ := sodRats(t, "dag", dtpRun(t, ctx, single, coord, sql, true), "v")
			want := []*big.Rat{big.NewRat(1275, 100), sodRat1(t, tc.lit)}
			if !sodRatsEqual(got, want) {
				t.Errorf("%s\n  got  %s\n  want %s", sql, sodShow(got, 0), sodShow(want, 0))
			}
		})
	}
}

// sodRat1 reads a decimal literal's SPELLING as an exact rational, so the
// expectation is the digits the query wrote and not a float's reading of them.
func sodRat1(t *testing.T, lit string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(lit)
	if !ok {
		t.Fatalf("test bug: %q is not a number", lit)
	}
	return r
}

func sodRepeatRat(t *testing.T, lit string, n int) []*big.Rat {
	t.Helper()
	r := sodRat1(t, lit)
	out := make([]*big.Rat, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, r)
	}
	return out
}
