package physical

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decDecls is a scan's catalog annotation with two DECIMAL columns at
// DIFFERENT (p,s) — the shape a single-column fixture cannot test, because a
// column reconciled against itself agrees whatever the rule.
func decDecls() colDecls {
	return colDecls{
		types: map[string]parquet.TypeID{
			"a":     parquet.TypeDecimal,
			"b":     parquet.TypeDecimal,
			"wide":  parquet.TypeDecimal,
			"nops":  parquet.TypeDecimal,
			"i32":   parquet.TypeInt32,
			"i64":   parquet.TypeInt64,
			"f64":   parquet.TypeFloat64,
			"txt":   parquet.TypeString,
			"stamp": parquet.TypeTimestamp,
		},
		dec: map[string]logical.DecimalMeta{
			"a":    {Precision: 9, Scale: 2},
			"b":    {Precision: 18, Scale: 4},
			"wide": {Precision: 38, Scale: 10},
			// The #458 "unconstrained" sentinel: a DECIMAL nothing could
			// put a (p,s) on.
			"nops": {},
		},
	}
}

// TestDecimalColumnReferenceDecidesItsOwnType is the structural fix of
// ADR-0024 item 2 and the root of #529/#555/#587: before it,
// colRefDeclaredType answered Undecided for every DECIMAL column because the
// declared-type layer was a bare TypeID with nowhere to put (p,s). Everything
// downstream then fell to its non-DECIMAL default.
func TestDecimalColumnReferenceDecidesItsOwnType(t *testing.T) {
	decls := decDecls()
	for _, tc := range []struct {
		name  string
		col   string
		want  expr.DeclType
		wantC expr.Confidence
	}{
		{"narrow", "a", expr.DeclDecimal(9, 2), expr.Decided},
		{"wider scale", "b", expr.DeclDecimal(18, 4), expr.Decided},
		{"the full carrier width", "wide", expr.DeclDecimal(38, 10), expr.Decided},
		// Precision 0 is a sentinel, not a declaration: taken at face value
		// the output vector would be built at scale 0 (#458).
		{"an unconstrained DECIMAL still declines", "nops", expr.DeclType{}, expr.Undecided},
		{"a name no scan carries", "missing", expr.DeclType{}, expr.Undecided},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, c := colRefDeclaredType(&plansql.ColRef{Column: tc.col}, decls)
			if got != tc.want || c != tc.wantC {
				t.Errorf("colRefDeclaredType(%q) = (%v, %s), want (%v, %s)",
					tc.col, got, c, tc.want, tc.wantC)
			}
		})
	}
}

// TestDecimalChoiceExpressionsDeclareTheCommonType covers ADR-0024 item 2's
// second half: every construct that CHOOSES BETWEEN its operands —
// CASE/COALESCE/NULLIF/IFNULL/IF/GREATEST/LEAST (IF is registered but the
// recursive-descent parser does not accept `IF(...)` as a call, so it is
// covered by the registry's own declaration rather than here) — declares a DECIMAL wide
// enough for all of them, through the same batch.DecimalCommon a set
// operation reconciles its arms with.
//
// Before this, GREATEST(a, b) declared FLOAT64 and the query could not run at
// all ("cannot store string into FLOAT64 vector", #529); COALESCE(a, b) was
// the same defect under another name (#555).
func TestDecimalChoiceExpressionsDeclareTheCommonType(t *testing.T) {
	decls := decDecls()
	for _, tc := range []struct {
		name  string
		sql   string
		want  expr.DeclType
		wantC expr.Confidence
	}{
		// (9,2) beside (18,4): scale 4 (nothing may be truncated) with the
		// widest integer part, 14 — DECIMAL(18,4).
		{"greatest", "GREATEST(a, b)", expr.DeclDecimal(18, 4), expr.Decided},
		{"least", "LEAST(a, b)", expr.DeclDecimal(18, 4), expr.Decided},
		{"coalesce", "COALESCE(a, b)", expr.DeclDecimal(18, 4), expr.Decided},
		{"ifnull", "IFNULL(a, b)", expr.DeclDecimal(18, 4), expr.Decided},
		// NULLIF mirrors argument 0 alone, which is PostgreSQL's rule too.
		{"nullif mirrors its first argument", "NULLIF(a, b)", expr.DeclDecimal(9, 2), expr.Decided},
		{"searched case", "CASE WHEN i64 > 0 THEN a ELSE b END", expr.DeclDecimal(18, 4), expr.Decided},
		{"simple case", "CASE i64 WHEN 1 THEN a ELSE b END", expr.DeclDecimal(18, 4), expr.Decided},
		{"case with one branch", "CASE WHEN i64 > 0 THEN a END", expr.DeclDecimal(9, 2), expr.Decided},
		// Three widths at once: scale 10, integer part 28 — exactly 38.
		{"three widths", "COALESCE(a, b, wide)", expr.DeclDecimal(38, 10), expr.Decided},
		{"a NULL branch decides nothing", "COALESCE(a, NULL)", expr.DeclDecimal(9, 2), expr.Decided},
		{"one column repeated", "GREATEST(a, a)", expr.DeclDecimal(9, 2), expr.Decided},

		// TODO(#555): the mixed shapes PostgreSQL answers numeric/float8 for.
		// They keep the FIRST NON-DECIMAL decider, which is exactly what
		// they declared before a DECIMAL reference could decide anything —
		// a loud mismatch at the store, never a silent rescale. An integer
		// box written into a DECIMAL vector is taken as ALREADY SCALED
		// (ADR-0018 §4), so declaring DECIMAL here would read
		// GREATEST(i64, a) = 5 back as 0.05.
		{"a DECIMAL beside an integer defers", "GREATEST(a, i64)",
			expr.Decl(parquet.TypeInt64), expr.Decided},
		{"a DECIMAL beside a float defers", "COALESCE(a, f64)",
			expr.Decl(parquet.TypeFloat64), expr.Decided},
		{"a DECIMAL beside a string defers", "COALESCE(a, txt)",
			expr.Decl(parquet.TypeString), expr.Decided},
		// An UNCONSTRAINED DECIMAL decides nothing and yet PRODUCES a value
		// — at its own scale, which nobody here knows. Folding it away and
		// declaring the other branch's (9,2) would truncate that value into
		// the output vector, so the whole fold DECLINES and the call answers
		// its own fallback exactly as it did before ADR-0024. Same clause,
		// same reason, for a scalar subquery or a container element (#458).
		{"an unconstrained DECIMAL beside a real one declines the fold", "COALESCE(nops, a)",
			expr.Decl(parquet.TypeFloat64), expr.Guessed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tc.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.sql, err)
			}
			got, c := nodeDeclaredType(node, decls)
			if got != tc.want || c != tc.wantC {
				t.Errorf("%s\n  declared (%v, %s), want (%v, %s)", tc.sql, got, c, tc.want, tc.wantC)
			}
		})
	}
}

// TestDecimalArithmeticStillDeclaresFloat64 pins the DEFERRED half of
// ADR-0024 item 3. The (p,s) rules for + - * / % are written and tested in
// batch.DecimalResultType, but the declaration cannot use them until there is
// decimal arithmetic to feed it: Int128 has no Mul and no QuoRem, so
// expr.BinOpNumeric resolves float mode for a DECIMAL operand and hands back
// a float64. Declaring DECIMAL over that would write a rounded float into an
// exact vector — worse than the float column a client gets today.
//
// The test exists so the day the kernel lands, this pin fails and says so.
func TestDecimalArithmeticStillDeclaresFloat64(t *testing.T) {
	decls := decDecls()
	for _, sql := range []string{"a + b", "a - b", "a * b", "a / b", "a % b", "a + 1", "a * i64"} {
		node, err := plansql.ParseExpression(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		got, c := nodeDeclaredType(node, decls)
		if got.ID != parquet.TypeFloat64 || c != expr.Decided {
			t.Errorf("%s: declared (%v, %s), want (FLOAT64, DECIDED) until the decimal "+
				"arithmetic kernel lands (TODO(#555), ADR-0024 item 3)", sql, got, c)
		}
	}
}

// TestDecimalCastStillDeclaresAsBefore pins the other deferred half:
// expr.Cast's DECIMAL destination produces a float64 for `CAST(x AS DECIMAL)`
// and passes its argument through unchanged for `CAST(x AS DECIMAL(p,s))`.
// Neither produces an exact value at a declared scale, so the declaration
// stays where it was (TODO(#555), ADR-0024 item 3).
func TestDecimalCastStillDeclaresAsBefore(t *testing.T) {
	decls := decDecls()
	for _, tc := range []struct {
		sql  string
		want parquet.TypeID
	}{
		{"CAST(a AS DECIMAL)", parquet.TypeFloat64},
		{"CAST(a AS NUMERIC)", parquet.TypeFloat64},
		{"CAST(a AS DECIMAL(10, 2))", parquet.TypeString},
	} {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.sql, err)
		}
		got, _ := nodeDeclaredType(node, decls)
		if got.ID != tc.want {
			t.Errorf("%s: declared %v, want %v (TODO(#555))", tc.sql, got, tc.want)
		}
	}
}

// TestWindowSpecOutputTypeResolvesDecimal is #587's plan half: a windowed
// MIN/MAX or value function over a DECIMAL column declares that column's
// type, (p,s) and all. A ZERO-ROW window result is described from this
// declaration alone — there is no vector for exec.Window to re-type from — so
// before this the same query described itself float8 when it matched nothing
// and numeric when it matched rows.
func TestWindowSpecOutputTypeResolvesDecimal(t *testing.T) {
	scan := &logical.Node{
		Type: logical.NodeScan, TableName: "t",
		ScanColTypes: map[string]parquet.TypeID{
			"a": parquet.TypeDecimal, "nops": parquet.TypeDecimal, "n": parquet.TypeInt64,
		},
		ScanColDecimal: map[string]logical.DecimalMeta{
			"a": {Precision: 9, Scale: 2}, "nops": {},
		},
	}
	win := &logical.Node{Type: logical.NodeWindow, Children: []*logical.Node{scan}}
	for _, tc := range []struct {
		name  string
		fn    string
		input string
		want  expr.DeclType
	}{
		{"min", "min", "a", expr.DeclDecimal(9, 2)},
		{"max", "MAX", "a", expr.DeclDecimal(9, 2)},
		{"first_value", "first_value", "a", expr.DeclDecimal(9, 2)},
		{"lag", "lag", "a, 1", expr.DeclDecimal(9, 2)},
		// SUM/AVG over a window still finalize in float64 (#586 is not in
		// this change's scope), so the name list answers for them.
		{"sum still float64", "sum", "a", expr.Decl(parquet.TypeFloat64)},
		// An unconstrained DECIMAL declines, as it does for a projection.
		{"an unconstrained DECIMAL declines", "min", "nops", expr.Decl(parquet.TypeFloat64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := windowSpecOutputType(win, logical.WindowExpr{Func: tc.fn, InputCol: tc.input, OutputCol: "w"})
			if got != tc.want {
				t.Errorf("%s(%s) declared %v, want %v", tc.fn, tc.input, got, tc.want)
			}
		})
	}
}

// TestDecimalFoldDeclinesOverAnUnknownProducer pins ADR-0024 item 2's safety
// clause at the layer that makes the decision.
//
// expr.CommonDeclType folds only the branches that DECIDED a type. A branch
// that decided nothing still produces a value, and a DECIMAL one arrives at
// ITS OWN scale — so folding it away and declaring the other branches' (p,s)
// truncates it. The fold declines instead, and the call answers exactly what
// it answered before a DECIMAL could decide anything.
//
// A bare NULL literal is SQL's `unknown`: it produces no value at all, so it
// neither contributes a type nor blocks the fold — which is why
// COALESCE(d, NULL) is numeric on PostgreSQL and stays numeric here.
func TestDecimalFoldDeclinesOverAnUnknownProducer(t *testing.T) {
	decls := decDecls()
	for _, tc := range []struct {
		name  string
		sql   string
		want  expr.DeclType
		wantC expr.Confidence
	}{
		// `nops` is a DECIMAL whose (p,s) nothing resolved: it decides
		// nothing and produces a value. So does a container element and a
		// scalar subquery, which this layer cannot type either.
		{"an unconstrained DECIMAL blocks the fold", "GREATEST(a, nops)",
			expr.Decl(parquet.TypeFloat64), expr.Guessed},
		{"and blocks a CASE the same way", "CASE WHEN i64 > 0 THEN a ELSE nops END",
			expr.DeclType{}, expr.Undecided},
		{"a container element blocks it", "COALESCE(a, element_at(a, 1))",
			expr.Decl(parquet.TypeFloat64), expr.Guessed},
		// The NULL exception, in both constructs.
		{"a NULL literal does not", "GREATEST(a, NULL)", expr.DeclDecimal(9, 2), expr.Decided},
		{"nor in a CASE", "CASE WHEN i64 > 0 THEN a ELSE NULL END",
			expr.DeclDecimal(9, 2), expr.Decided},
		{"nor as a missing ELSE", "CASE WHEN i64 > 0 THEN a END",
			expr.DeclDecimal(9, 2), expr.Decided},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tc.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.sql, err)
			}
			got, c := nodeDeclaredType(node, decls)
			if got != tc.want || c != tc.wantC {
				t.Errorf("%s\n  declared (%v, %s), want (%v, %s)", tc.sql, got, c, tc.want, tc.wantC)
			}
		})
	}
}

// TestNestedChoiceDeclarationIsLinearInDepth is a COMPLEXITY gate, not a value
// one.
//
// expr.Ret.Resolve asks its caller for each argument's type through argType,
// and on the planner side that callback is a full recursive walk of the
// argument's expression tree. Asking twice per argument — once for the
// candidate fold, once for the safety check the review added — makes the walk
// 2^depth over nested polymorphic calls: a linear-text COALESCE nested 22
// deep took 2.7 seconds to PLAN and 30 deep would not have finished, from 287
// bytes of SQL. Resolve makes exactly one call per argument now.
//
// The bound is deliberately loose (a second for depth 24, which the linear
// form does in well under a millisecond) so the test fails on a return of the
// exponential shape and never on a slow machine.
func TestNestedChoiceDeclarationIsLinearInDepth(t *testing.T) {
	decls := decDecls()
	nest := func(fn string, depth int) string {
		out := "a"
		for i := 0; i < depth; i++ {
			out = fn + "(" + out + ", b)"
		}
		return out
	}
	for _, fn := range []string{"COALESCE", "GREATEST", "LEAST"} {
		for _, depth := range []int{8, 16, 24} {
			sql := nest(fn, depth)
			node, err := plansql.ParseExpression(sql)
			if err != nil {
				t.Fatalf("parse %s depth %d: %v", fn, depth, err)
			}
			start := time.Now()
			got, c := nodeDeclaredType(node, decls)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("%s nested %d deep took %v to declare — the argument walk is "+
					"exponential in depth again", fn, depth, elapsed)
			}
			// And the answer is still the common type of a (9,2) and b (18,4).
			if want := expr.DeclDecimal(18, 4); got != want || c != expr.Decided {
				t.Errorf("%s nested %d deep declared (%v, %s), want (%v, DECIDED)",
					fn, depth, got, c, want)
			}
		}
	}
	// A CASE nests through its own fold rather than through Ret.Resolve, and
	// must stay linear for the same reason.
	sql := "a"
	for i := 0; i < 24; i++ {
		sql = "CASE WHEN i64 > 0 THEN " + sql + " ELSE b END"
	}
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		t.Fatalf("parse nested CASE: %v", err)
	}
	start := time.Now()
	if got, c := nodeDeclaredType(node, decls); got != expr.DeclDecimal(18, 4) || c != expr.Decided {
		t.Errorf("nested CASE declared (%v, %s), want (DECIMAL(18,4), DECIDED)", got, c)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("a CASE nested 24 deep took %v to declare", elapsed)
	}
}
