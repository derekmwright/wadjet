package physical

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// nfdCols is one column per numeric width, which decDecls cannot supply: it
// has no FLOAT32, and the ladder's two float rungs are the pair PostgreSQL
// keeps APART (`real ∪ numeric` is real, `real ∪ double` is double), so a
// fixture without a real column cannot tell the ladder from "the widest float
// wins".
var nfdCols = []struct {
	name string
	typ  parquet.TypeID
	pg   string
}{
	{"i32", parquet.TypeInt32, "integer"},
	{"i64", parquet.TypeInt64, "bigint"},
	{"d152", parquet.TypeDecimal, "numeric"},
	{"d3810", parquet.TypeDecimal, "numeric"},
	{"f32", parquet.TypeFloat32, "real"},
	{"f64", parquet.TypeFloat64, "double precision"},
}

func nfdDecls() colDecls {
	types := map[string]parquet.TypeID{"txt": parquet.TypeString}
	for _, c := range nfdCols {
		types[c.name] = c.typ
	}
	return colDecls{
		types: types,
		dec: map[string]logical.DecimalMeta{
			"d152":  {Precision: 15, Scale: 2},
			"d3810": {Precision: 38, Scale: 10},
		},
	}
}

func nfdScope() *colScope {
	colTypes := map[string]parquet.TypeID{"txt": parquet.TypeString}
	cols := map[string]bool{"txt": true}
	for _, c := range nfdCols {
		colTypes[c.name] = c.typ
		cols[c.name] = true
	}
	return &colScope{cols: cols, colTypes: colTypes, quals: map[string]map[string]bool{}}
}

func nfdDeclared(t *testing.T, sql string) (expr.DeclType, expr.Confidence) {
	t.Helper()
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return nodeDeclaredType(node, nfdDecls())
}

// TestQuotedLiteralIsUnknownInTheFold is #724 at the layer the defect lives
// in: a QUOTED string literal names no type of its own inside a polymorphic
// call, so the call is typed from the arguments that DO.
//
// PostgreSQL types such a literal `unknown` and resolves it from the other
// operands; wadjet typed it `Decl(TypeString), Decided`, which put a
// non-numeric decider in the list, made expr.CommonDeclType decline the fold,
// and left the call declared as its FIRST argument. That declaration is
// NARROWER than the value the call produces, and an output vector does not
// narrow a value past its range — it WRAPS it.
//
// Every want is PostgreSQL 17.11's, read off pg_attribute for a view over the
// same expression (so it carries the typmod, not just the type name).
func TestQuotedLiteralIsUnknownInTheFold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		want  expr.DeclType
		wantC expr.Confidence
		pg    string
	}{
		// The shapes from the issue. `GREATEST(bigint, real, double, '1e39')`
		// declared bigint and answered int64's MINIMUM.
		{"a quoted literal last", "GREATEST(i64, f32, f64, '1e39')",
			expr.Decl(parquet.TypeFloat64), expr.Decided, "double precision"},
		{"a quoted literal first", "GREATEST('1e39', i64, f32, f64)",
			expr.Decl(parquet.TypeFloat64), expr.Decided, "double precision"},
		{"a quoted literal in the middle", "GREATEST(f32, '16777217', f64)",
			expr.Decl(parquet.TypeFloat64), expr.Decided, "double precision"},
		{"a quoted literal beside a bigint and a double", "GREATEST(i64, '3.5', f64)",
			expr.Decl(parquet.TypeFloat64), expr.Decided, "double precision"},
		// A real beside an integer stays REAL: both float types are preferred
		// in PostgreSQL's numeric category and only float8 beats float4, so a
		// fold that answered double here would render a real 0.1 as
		// 0.10000000149011612.
		{"a quoted literal beside a real and an int", "GREATEST(f32, '3.5', i32)",
			expr.Decl(parquet.TypeFloat32), expr.Decided, "real"},
		{"a quoted literal beside a real and a decimal", "COALESCE(f32, '3.5', d152)",
			expr.Decl(parquet.TypeFloat32), expr.Decided, "real"},

		// A quoted literal contributes its SPELLING to a DECIMAL fold, scale
		// included, which is the same contribution the unquoted spelling
		// makes and the rule ADR-0012 item 12 states for every arm. The two
		// spellings agreeing is the point: PostgreSQL resolves both to
		// unconstrained numeric and answers the literal's digits, and a fold
		// that took only the declared operands' scale would refuse them.
		{"a quoted literal widens a decimal fold's scale",
			"GREATEST(d152, '12.750000000000000001')",
			expr.DeclDecimal(31, 18), expr.Decided, "numeric"},
		{"a quoted integer literal inside a decimal fold", "GREATEST(d152, '16777217')",
			expr.DeclDecimal(15, 2), expr.Decided, "numeric"},
		// It DOES widen the precision, which moves no rendered digit and is
		// what lets a large-magnitude literal be the answer at all.
		{"a quoted literal widens a decimal fold's integer part",
			"GREATEST(d152, '1234567890123')",
			expr.DeclDecimal(15, 2), expr.Decided, "numeric"},
		{"a quoted literal past the column's integer part widens it",
			"GREATEST(d152, '123456789012345678')",
			expr.DeclDecimal(20, 2), expr.Decided, "numeric"},

		// A quoted literal beside a TEXT column keeps text, which is the case
		// the fix had to not break.
		{"a quoted literal beside a text column", "COALESCE(txt, 'x')",
			expr.Decl(parquet.TypeString), expr.Decided, "text"},
		{"a text column beside a quoted literal", "COALESCE('x', txt)",
			expr.Decl(parquet.TypeString), expr.Decided, "text"},

		// With NO typed operand at all PostgreSQL resolves the whole
		// composite to text, and so does this: the quoted literal's own
		// declaration is TypeString and it is what stands when nothing else
		// decides.
		{"every argument quoted", "COALESCE('a', 'b')",
			expr.DeclQuotedLit("a"), expr.Decided, "text"},
		{"every argument quoted, greatest", "GREATEST('a', 'b')",
			expr.DeclQuotedLit("a"), expr.Decided, "text"},

		// The ladder itself, with no literal anywhere: the fold is the same
		// select_common_type, and it was "the first decider wins" until #724.
		{"int32 beside int64", "COALESCE(i32, i64)",
			expr.Decl(parquet.TypeInt64), expr.Decided, "bigint"},
		{"real beside double", "COALESCE(f32, f64)",
			expr.Decl(parquet.TypeFloat64), expr.Decided, "double precision"},
		{"double beside real", "COALESCE(f64, f32)",
			expr.Decl(parquet.TypeFloat64), expr.Decided, "double precision"},
		{"decimal beside real", "COALESCE(d152, f32)",
			expr.Decl(parquet.TypeFloat32), expr.Decided, "real"},
		{"real beside decimal", "COALESCE(f32, d152)",
			expr.Decl(parquet.TypeFloat32), expr.Decided, "real"},
		{"bigint beside decimal beside real", "GREATEST(i64, d152, f32)",
			expr.Decl(parquet.TypeFloat32), expr.Decided, "real"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, c := nfdDeclared(t, tc.sql)
			if got != tc.want || c != tc.wantC {
				t.Errorf("%s\n  declared (%v, %s), want (%v, %s) — PostgreSQL 17.11 declares %s",
					tc.sql, got, c, tc.want, tc.wantC, tc.pg)
			}
		})
	}
}

// TestNumericLiteralKeepsItsOwnDeclarationInAFold is the OTHER half of the
// same rule, and it is a non-change: ADR-0024 records that a bare numeric
// literal declares INT64 or FLOAT64 on its own rather than PostgreSQL's
// integer/numeric, and #724 does not reopen that.
//
// So a CONSTANT is folded at PostgreSQL's rung for its spelling only when
// there is a TYPED operand to resolve it against — `CASE … THEN i32 ELSE 0
// END` is `integer` on both engines — and a composite of nothing but constants
// keeps the deferral.
//
// The one case a constant does TRIGGER the fold is a FRACTIONAL literal beside
// a non-constant arm, and it is not a reopening of the deferral: it is the
// only reading under which the value survives. An integer declaration there
// builds an integer vector and the fraction the evaluator produces is
// truncated into it (round-1 review, B3). expr.fractionalLitTriggersFold and
// its runtime twin expr.fracLitArmTriggersFold are the one place that is
// decided. The wide-literal case keeps it too, for the reason
// wadjet.TestWideNumericLiteralInAChoiceStaysFloat pins: past a double's ~17
// significant digits the literal's BOX has already lost the digits a DECIMAL
// declaration would claim to hold.
func TestNumericLiteralKeepsItsOwnDeclarationInAFold(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want expr.DeclType
	}{
		{"an int literal beside an int32 column stays integer",
			"CASE WHEN i64 > 0 THEN i32 ELSE 0 END", expr.Decl(parquet.TypeInt32)},
		{"an int literal past int32 widens the fold",
			"COALESCE(i32, 3000000000)", expr.Decl(parquet.TypeInt64)},
		{"a fractional literal does not make a float column double",
			"GREATEST(f32, 3.5)", expr.Decl(parquet.TypeFloat32)},
		// A FRACTIONAL literal beside a non-constant arm is the ONE case where
		// a constant triggers the fold, and it is a value that forced it:
		// PostgreSQL types `COALESCE(i32, 1.5)` numeric, the integer
		// declaration built an int32 vector, and the 1.5 the evaluator
		// produced was TRUNCATED into it — `LEAST(c_i64, 1.5) * 3` answered 4
		// for the server's 4.5 (round-1 review, B3). The scale comes from the
		// literal's spelling; the precision from the column's range plus it.
		{"a fractional literal beside an int column widens to the decimal rung",
			"COALESCE(i32, 1.5)", expr.DeclDecimal(11, 1)},
		{"two literals keep the literal declaration",
			"GREATEST(0.5, 1.5)", expr.DeclNumericLit(parquet.TypeFloat64, "0.5")},
		{"a wide literal beside a decimal stays float",
			"GREATEST(d3810, 493827160549382.7160549350)", expr.Decl(parquet.TypeFloat64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, c := nfdDeclared(t, tc.sql)
			if got != tc.want || c != expr.Decided {
				t.Errorf("%s\n  declared (%v, %s), want (%v, DECIDED)", tc.sql, got, c, tc.want)
			}
		})
	}
}

// TestDeclaredFoldAgreesWithTheComparisonFold is the invariant the two layers
// have to hold jointly, made testable.
//
// physical.foldArgTypes resolves a composite for the plan-time literal REFUSAL
// (#646) and expr.CommonDeclType resolves the same composite for the
// DECLARATION. They answer different questions — "can this literal be read at
// all" and "what vector does the winner go into" — over one type, and a
// disagreement between them is a value narrowed or wrapped on the way out:
// that is the whole of #724, where the refusal already folded to double and
// the declaration still said bigint.
//
// Every ordered pair of the six widths, with and without a quoted literal, so
// the claim is not proved by the one ordering someone happened to write.
func TestDeclaredFoldAgreesWithTheComparisonFold(t *testing.T) {
	scope := nfdScope()
	for _, a := range nfdCols {
		for _, b := range nfdCols {
			if a.name == b.name {
				continue
			}
			for _, form := range []string{
				"GREATEST(%s, %s)",
				"GREATEST(%s, '3.5', %s)",
				"GREATEST('3.5', %s, %s)",
				"COALESCE(%s, %s)",
				"LEAST(%s, '16777217', %s)",
				// NESTED forms. A composite is an ARGUMENT of a composite,
				// which is a call shape neither layer's enumeration reaches
				// by walking the flat argument list — physical.argDeclaredType
				// recurses into greatest/least/coalesce/ifnull/nullif and a
				// CASE, and expr's classifyOperandFold into greatest/least,
				// CASE and Coalesce, and the two lists are not the same list.
				// The #724 review found the gap by hand
				// (`COALESCE(NULLIF(numeric, '…'), real)`), so it is enumerated
				// here rather than left to the next reviewer.
				"COALESCE(NULLIF(%s, '1.5'), %s)",
				"COALESCE(GREATEST(%s, '1.5'), %s)",
				"GREATEST(COALESCE(%s, '1.5'), %s)",
				"GREATEST(%s, LEAST(%s, '1.5'))",
				"COALESCE(CASE WHEN i32 > 0 THEN %s ELSE '1.5' END, %s)",
			} {
				sql := fmt.Sprintf(form, a.name, b.name)
				t.Run(sql, func(t *testing.T) {
					node, err := plansql.ParseExpression(sql)
					if err != nil {
						t.Fatalf("parse %q: %v", sql, err)
					}
					declared, c := nodeDeclaredType(node, nfdDecls())
					if c != expr.Decided {
						t.Fatalf("%s declared nothing (%s)", sql, c)
					}
					call, ok := node.(*plansql.FuncCallNode)
					if !ok {
						t.Fatalf("%s did not parse to a call", sql)
					}
					common, kind := foldArgTypes(scope, call.Args)
					if kind != argTyped {
						t.Fatalf("%s: the comparison layer could not fold it (%v)", sql, kind)
					}
					if declared.ID != common {
						t.Errorf("%s: the DECLARATION says %s and the COMPARISON folds to %s. "+
							"They must agree — the comparison decides which argument wins and "+
							"the declaration decides the vector the winner is stored in, so a "+
							"narrower declaration does not narrow the value, it wraps it (#724).",
							sql, declared.ID, common)
					}
				})
			}
		}
	}
}
