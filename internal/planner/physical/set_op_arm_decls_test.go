package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// sadScan is one side of the #551 fixture: a table with a BIGINT id and a
// DECIMAL column named dx, at the (p,s) the caller gives it.
func sadScan(table, alias string, prec, scale int) *logical.Node {
	n := logical.NewScan(table, alias)
	n.ScanColumns = []string{"id", "dx"}
	n.ScanColTypes = map[string]parquet.TypeID{"id": parquet.TypeInt64, "dx": parquet.TypeDecimal}
	n.ScanColDecimal = map[string]logical.DecimalMeta{"dx": {Precision: prec, Scale: scale}}
	return n
}

// TestSetOpArmDeclsKeepsAJoinsTwoSidesApart is #551 at the resolution layer.
//
// inputColTypes / inputColDecimal merge a join's two sides and DELETE any name
// they disagree on, which for `a.dx DECIMAL(9,2)` beside `b.dx DECIMAL(18,4)`
// throws away the fact a set operation exists to reconcile. The arm walk keys
// each side's columns under its own relation names as well as bare, so the
// QUALIFIED spelling — the one the projection actually carries — answers about
// the right side.
func TestSetOpArmDeclsKeepsAJoinsTwoSidesApart(t *testing.T) {
	join := logical.NewJoin(
		sadScan("ja", "a", 9, 2),
		sadScan("jb", "b", 18, 4),
		"inner", "a.id = b.id")
	decls := setOpArmDecls(join)

	for _, tc := range []struct {
		name       string
		ref        *plansql.ColRef
		want       expr.DeclType
		wantDecide expr.Confidence
	}{
		// The ALIAS is how a query writes the reference for a scan that has
		// one.
		{"left alias", &plansql.ColRef{Table: "a", Column: "dx"}, expr.DeclDecimal(9, 2), expr.Decided},
		{"right alias", &plansql.ColRef{Table: "b", Column: "dx"}, expr.DeclDecimal(18, 4), expr.Decided},
		// The TABLE NAME behind that alias is not, and this walk no longer
		// claims it. `SELECT ja.dx FROM ja a` is "invalid reference to
		// FROM-clause entry for table ja" in PostgreSQL 17.11 (measured live)
		// and "missing FROM-clause entry" in wadjet on every path, so a
		// declaration for it describes a spelling neither engine executes —
		// and it collided with the LEGAL `ja.dx` of a derived table aliased
		// `ja` beside it, which is #682's second shape. Undecided leaves the
		// bare-name fallback in lookupColKey to answer, which is what it did
		// for every reference this walk cannot key.
		{"left table name behind an alias", &plansql.ColRef{Table: "ja", Column: "dx"},
			expr.DeclType{}, expr.Undecided},
		{"right table name behind an alias", &plansql.ColRef{Table: "jb", Column: "dx"},
			expr.DeclType{}, expr.Undecided},
		// The BARE name still says nothing: the two sides disagree about it,
		// and an unqualified reference names no one column. Answering with
		// either side would be the mirror image of the defect.
		{"the bare name stays unresolved", &plansql.ColRef{Column: "dx"}, expr.DeclType{}, expr.Undecided},
		// A name the two sides agree on is unaffected.
		{"an agreed name", &plansql.ColRef{Column: "id"}, expr.Decl(parquet.TypeInt64), expr.Decided},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, c := colRefDeclaredType(tc.ref, decls)
			if got != tc.want || c != tc.wantDecide {
				t.Errorf("%s.%s = (%v, %s), want (%v, %s)",
					tc.ref.Table, tc.ref.Column, got, c, tc.want, tc.wantDecide)
			}
		})
	}
}

// TestSetOpArmDeclsResolvesThroughANestedJoin covers the three-way shape: the
// left side of the outer join is ITSELF a join, whose own qualified keys have
// to survive the second merge rather than be re-derived from its merged bare
// view.
func TestSetOpArmDeclsResolvesThroughANestedJoin(t *testing.T) {
	inner := logical.NewJoin(sadScan("ja", "a", 9, 2), sadScan("jb", "b", 18, 4), "inner", "a.id = b.id")
	outer := logical.NewJoin(inner, sadScan("jb", "c", 18, 4), "inner", "a.id = c.id")
	decls := setOpArmDecls(outer)

	for _, tc := range []struct {
		alias string
		want  expr.DeclType
	}{
		{"a", expr.DeclDecimal(9, 2)},
		{"b", expr.DeclDecimal(18, 4)},
		{"c", expr.DeclDecimal(18, 4)},
	} {
		got, c := colRefDeclaredType(&plansql.ColRef{Table: tc.alias, Column: "dx"}, decls)
		if got != tc.want || c != expr.Decided {
			t.Errorf("%s.dx = (%v, %s), want (%v, Decided)", tc.alias, got, c, tc.want)
		}
	}
}

// TestSetOpArmDeclsDescendsIntoADerivedTable is #554 at the resolution layer:
// inputColDecls STOPS at a Project, so a derived-table arm's columns were
// invisible and its DECIMAL arrived with no (p,s). The arm walk answers for
// the names the subplan EMITS.
func TestSetOpArmDeclsDescendsIntoADerivedTable(t *testing.T) {
	scan := logical.NewScan("t", "t")
	scan.ScanColumns = []string{"e2", "e4"}
	scan.ScanColTypes = map[string]parquet.TypeID{"e2": parquet.TypeDecimal, "e4": parquet.TypeDecimal}
	scan.ScanColDecimal = map[string]logical.DecimalMeta{
		"e2": {Precision: 9, Scale: 2},
		"e4": {Precision: 18, Scale: 4},
	}
	proj := &logical.Node{
		Type:     logical.NodeProject,
		Children: []*logical.Node{scan},
		Projections: []logical.Projection{
			{Expr: "e2", Column: "e2", Alias: "x", ASTExpr: &plansql.ColRef{Column: "e2"}},
			{Expr: "e4", Column: "e4", Alias: "y", ASTExpr: &plansql.ColRef{Column: "e4"}},
		},
	}
	decls := setOpArmDecls(proj)

	for _, tc := range []struct {
		col  string
		want expr.DeclType
	}{
		{"x", expr.DeclDecimal(9, 2)},
		{"y", expr.DeclDecimal(18, 4)},
	} {
		got, c := colRefDeclaredType(&plansql.ColRef{Column: tc.col}, decls)
		if got != tc.want || c != expr.Decided {
			t.Errorf("%s = (%v, %s), want (%v, Decided)", tc.col, got, c, tc.want)
		}
	}
	// A name only the SOURCE carries is not an output of the derived table,
	// and claiming it would let a set-operation arm reconcile a column its
	// stream does not expose under that name.
	if _, c := colRefDeclaredType(&plansql.ColRef{Column: "e2"}, decls); c != expr.Undecided {
		t.Errorf("the derived table's SOURCE name e2 resolved through its Project; "+
			"the walk must answer for what the subplan EMITS, got confidence %s", c)
	}
}

// TestSetOpArmDeclsClaimsNothingItCannotResolve is the property that keeps the
// walk from being worse than the merged one it replaced: declaredProjectionDecl
// answers STRING for a projection it cannot resolve, which is right for
// advisory wire metadata and poisonous for an arm type — a confident STRING
// makes the ladder refuse a union of two numbers.
func TestSetOpArmDeclsClaimsNothingItCannotResolve(t *testing.T) {
	scan := logical.NewScan("t", "t")
	scan.ScanColumns = []string{"e2"}
	scan.ScanColTypes = map[string]parquet.TypeID{"e2": parquet.TypeDecimal}
	scan.ScanColDecimal = map[string]logical.DecimalMeta{"e2": {Precision: 9, Scale: 2}}
	proj := &logical.Node{
		Type:     logical.NodeProject,
		Children: []*logical.Node{scan},
		Projections: []logical.Projection{
			{Expr: "nosuch", Column: "nosuch", Alias: "x", ASTExpr: &plansql.ColRef{Column: "nosuch"}},
		},
	}
	if _, ok := setOpArmDecls(proj).types["x"]; ok {
		t.Error(`a projection of a column no scan carries was typed; the walk must claim nothing, ` +
			`because a confident wrong type here casts the arm's values`)
	}
}

// TestLitDeclType pins a numeric literal's type and (p,s) to its SPELLING,
// which is how PostgreSQL types one (#665). Every row was verified against
// live postgres:17.11 with pg_typeof over `SELECT <literal>`.
func TestLitDeclType(t *testing.T) {
	for _, tc := range []struct {
		lit   string
		want  expr.DeclType
		text  string
		wantK bool
	}{
		// The issue's literal.
		{"1.23456", expr.DeclDecimal(6, 5), "1.23456", true},
		{"12.75", expr.DeclDecimal(4, 2), "12.75", true},
		// A leading zero holds no place: `0.5` is one digit at scale 1.
		{"0.5", expr.DeclDecimal(1, 1), "0.5", true},
		{"0.00", expr.DeclDecimal(2, 2), "0.00", true},
		// Trailing zeros DO count — they are digits the query wrote, and a
		// set operation must not drop a scale it stated.
		{"1.5000", expr.DeclDecimal(5, 4), "1.5000", true},
		// A trailing point is a decimal point: PostgreSQL types `1.` numeric.
		{"1.", expr.DeclDecimal(1, 0), "1", true},
		// An EXPONENT makes a literal numeric in PostgreSQL, with or without
		// a decimal point — `pg_typeof(1e2)` is numeric, not double
		// precision. The (p,s) is the digits it EXPANDS to.
		{"1e2", expr.DeclDecimal(3, 0), "100", true},
		{"1.5e1", expr.DeclDecimal(2, 0), "15", true},
		{"1.5e-2", expr.DeclDecimal(3, 3), "0.015", true},
		{"1.5E2", expr.DeclDecimal(3, 0), "150", true},
		{"2e-1", expr.DeclDecimal(1, 1), "0.2", true},
		// A plain INTEGER literal stays on the ladder's integer rung, where
		// an integer arm contributes its whole range's digits — PostgreSQL
		// types these integer and bigint, not numeric.
		{"1", expr.DeclType{}, "", false},
		{"0", expr.DeclType{}, "", false},
		{"12345678901", expr.DeclType{}, "", false},
		// …until no integer type holds it. PostgreSQL types a 21-digit
		// integer constant numeric.
		{"123456789012345678901", expr.DeclDecimal(21, 0), "123456789012345678901", true},
		// Past the carrier's width there is no DECIMAL to declare, so the
		// float declaration stands rather than a fabricated (p,s).
		{"1.0000000000000000000000000000000000000000", expr.DeclType{}, "", false},
		{"1e40", expr.DeclType{}, "", false},
		// Not a number at all.
		{"1.2.3", expr.DeclType{}, "", false},
		{"1e", expr.DeclType{}, "", false},
		{"", expr.DeclType{}, "", false},
	} {
		t.Run(tc.lit, func(t *testing.T) {
			got, ok := litDeclType(&plansql.Lit{Value: tc.lit, Kind: plansql.LitNumber})
			if ok != tc.wantK {
				t.Fatalf("litDeclType(%q) ok = %v, want %v (got %+v)", tc.lit, ok, tc.wantK, got)
			}
			if !ok {
				return
			}
			if got.decl != tc.want {
				t.Errorf("litDeclType(%q) declared %v, want %v", tc.lit, got.decl, tc.want)
			}
			// The TEXT is what the arm's projection is rewritten to, and it is
			// the whole point: the evaluator folds a numeric literal into a
			// float64, so a DECIMAL declared over the box would be an exact
			// type on an already-rounded number.
			if got.text != tc.text {
				t.Errorf("litDeclType(%q) text = %q, want %q", tc.lit, got.text, tc.text)
			}
		})
	}
	// A non-numeric literal never answers, whatever it is spelled like.
	if _, ok := litDeclType(&plansql.Lit{Value: "1.5", Kind: plansql.LitString}); ok {
		t.Error("a STRING literal spelled like a number was typed numeric")
	}
	if _, ok := litDeclType(nil); ok {
		t.Error("a nil literal was typed")
	}
}

// TestSetOpLitArmReadsTheSign covers the wrapper: the parser makes a leading
// sign a UnaryOp, and reading only the unsigned spelling would let the sign
// decide the column's TYPE.
func TestSetOpLitArmReadsTheSign(t *testing.T) {
	lit := &plansql.Lit{Value: "1.5000", Kind: plansql.LitNumber}
	for _, tc := range []struct {
		name string
		node plansql.Node
		want string
		ok   bool
	}{
		{"bare", lit, "1.5000", true},
		{"negated", &plansql.UnaryOp{Op: "-", Inner: lit}, "-1.5000", true},
		{"explicitly positive", &plansql.UnaryOp{Op: "+", Inner: lit}, "1.5000", true},
		{"parenthesized", &plansql.ParenNode{Inner: lit}, "1.5000", true},
		{"negated and parenthesized",
			&plansql.UnaryOp{Op: "-", Inner: &plansql.ParenNode{Inner: lit}}, "-1.5000", true},
		{"twice negated", &plansql.UnaryOp{Op: "-", Inner: &plansql.UnaryOp{Op: "-", Inner: lit}}, "1.5000", true},
		{"a column reference", &plansql.ColRef{Column: "a"}, "", false},
		{"an expression", &plansql.BinaryOp{Left: lit, Op: "+", Right: lit}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := setOpLitArm(tc.node)
			if ok != tc.ok {
				t.Fatalf("setOpLitArm ok = %v, want %v", ok, tc.ok)
			}
			if ok && got.text != tc.want {
				t.Errorf("setOpLitArm text = %q, want %q", got.text, tc.want)
			}
		})
	}
	// A signed ZERO keeps no sign: "-0" is not a spelling any parser or
	// reader needs to see, and ParseDecimalStringChecked would carry it.
	zero := &plansql.UnaryOp{Op: "-", Inner: &plansql.Lit{Value: "0.0", Kind: plansql.LitNumber}}
	if got, ok := setOpLitArm(zero); !ok || got.text != "0.0" {
		t.Errorf("setOpLitArm(-0.0) = %q/%v, want \"0.0\"/true", got.text, ok)
	}
}
