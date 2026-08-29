package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setOpArmDecls describes the columns a set-operation ARM's SELECT list can
// name. It is inputColDecls' job with the two differences #551 and #554 turn
// on, and it exists separately because both differences are wrong for
// inputColDecls' other callers.
//
//  1. A JOIN keeps a PER-SIDE answer. inputColTypes and inputColDecimal merge
//     a join's two sides and DELETE any name they disagree on — right for a
//     TypeID, because two tables genuinely have two `dx` columns and picking
//     a side would answer about the wrong one. For a set operation that
//     disagreement IS the fact being reconciled: `a.dx DECIMAL(9,2)` beside
//     `b.dx DECIMAL(18,4)` resolved to DECIMAL with no (p,s), no coercion was
//     emitted, and each arm's .wshf file kept its own scale — the wider arm's
//     unscaled integer read at the narrower arm's scale, 100x out, silently
//     (#551). The projection names the column QUALIFIED, so the two sides are
//     told apart by keying each side's columns under its own relation names
//     as well as bare; the BARE name still merges-and-deletes, which remains
//     the honest answer for an unqualified reference two sides disagree on.
//
//  2. A PROJECT is descended INTO rather than stopped at, so a DERIVED-TABLE
//     arm resolves through the names its subplan EMITS (#554). The nested
//     set-operation arm already reads itself that way; a derived table is the
//     same shape one node down.
//
// The walk is deliberately STRICTER than emittedColTypes about what it will
// claim: a projection it cannot resolve produces NO entry, where
// declaredProjectionDecl answers STRING. That fallback is right for advisory
// wire metadata and poisonous here — a confidently-wrong arm type makes the
// ladder cast the column, which moves values.
func setOpArmDecls(n *logical.Node) colDecls {
	if n == nil {
		return colDecls{}
	}
	switch n.Type {
	case logical.NodeScan:
		return scanArmDecls(n)
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return colDecls{}
		}
		return setOpArmDecls(n.Children[0])
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return colDecls{}
		}
		return joinArmDecls(setOpArmDecls(n.Children[0]), setOpArmDecls(n.Children[1]))
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return colDecls{}
		}
		return projectArmDecls(n, setOpArmDecls(n.Children[0]))
	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		// A nested set operation emits what ITS OWN reconciliation makes its
		// arms agree on. setOpArmProjection already asks that question when
		// the nested operation IS the arm; a derived table around it
		// (`SELECT v FROM (a UNION ALL b) t UNION ALL c`) put a Project in
		// between, the walk answered nothing, and the outer operation left
		// all three files at their own scales — the shuffle writer refused
		// the query.
		return setOpNodeDecls(n)
	}
	// An Aggregate or a Window: the existing emitted walk is the answer,
	// unchanged. Its STRING fallback cannot reach a column this walk claims,
	// because that fallback lives in the Project arm handled above.
	return emittedColDecls(n)
}

// scanArmDecls is a scan's own declarations, keyed bare AND under each
// relation name the scan answers to. The qualified keys are what let a join's
// two sides be told apart: `a.dx` names the left side's column and `b.dx` the
// right one, where the bare `dx` names whichever of them the merge kept.
//
// A bare key always wins over a qualified one it would collide with: a
// delimited identifier can contain a dot ("id.orig_h", a flat Zeek JSON
// column) and IS one name, which is why colDecls.colDecl resolves the full
// dotted spelling as a column of its own first (ADR-0022).
func scanArmDecls(n *logical.Node) colDecls {
	if len(n.ScanColTypes) == 0 {
		return colDecls{}
	}
	quals := make([]string, 0, 2)
	for _, q := range []string{n.TableAlias, n.TableName} {
		q = strings.ToLower(strings.TrimSpace(q))
		if q == "" {
			continue
		}
		dup := false
		for _, have := range quals {
			if have == q {
				dup = true
				break
			}
		}
		if !dup {
			quals = append(quals, q)
		}
	}
	types := make(map[string]parquet.TypeID, len(n.ScanColTypes)*(1+len(quals)))
	for c, t := range n.ScanColTypes {
		types[strings.ToLower(c)] = t
	}
	var dec map[string]logical.DecimalMeta
	if len(n.ScanColDecimal) > 0 {
		dec = make(map[string]logical.DecimalMeta, len(n.ScanColDecimal)*(1+len(quals)))
		for c, m := range n.ScanColDecimal {
			dec[strings.ToLower(c)] = m
		}
	}
	for _, q := range quals {
		for c, t := range n.ScanColTypes {
			k := q + "." + strings.ToLower(c)
			if _, taken := types[k]; taken {
				continue
			}
			types[k] = t
			if m, ok := n.ScanColDecimal[strings.ToLower(c)]; ok && dec != nil {
				dec[k] = m
			}
		}
	}
	return colDecls{types: types, fields: n.ScanColFields, dec: dec}
}

// joinArmDecls unions the two sides. A QUALIFIED key belongs to exactly one
// side and is carried through untouched — that is the whole point. A BARE
// name the two sides declare differently is DELETED, exactly as inputColTypes
// does, because an unqualified reference to it names no one column.
func joinArmDecls(left, right colDecls) colDecls {
	if left.types == nil || right.types == nil {
		return colDecls{}
	}
	types := make(map[string]parquet.TypeID, len(left.types)+len(right.types))
	dec := make(map[string]logical.DecimalMeta, len(left.dec)+len(right.dec))
	for c, t := range left.types {
		types[c] = t
	}
	for c, m := range left.dec {
		dec[c] = m
	}
	for c, t := range right.types {
		if prev, dup := types[c]; dup && prev != t {
			delete(types, c)
			delete(dec, c)
			continue
		}
		if m, ok := right.dec[c]; ok {
			if prev, dup := dec[c]; dup && prev != m {
				// Same TypeID, different (p,s) — DECIMAL(9,2) beside
				// DECIMAL(18,4). The bare name cannot say which, so it says
				// nothing; the qualified keys carry both.
				delete(types, c)
				delete(dec, c)
				continue
			}
			dec[c] = m
		} else if _, dup := dec[c]; dup {
			// One side declares a (p,s) and the other does not, for a name
			// both carry as DECIMAL. Not one column either.
			delete(types, c)
			delete(dec, c)
			continue
		}
		types[c] = t
	}
	if len(dec) == 0 {
		dec = nil
	}
	return colDecls{types: types, dec: dec}
}

// setOpNodeDecls describes a nested set operation's output: its own result
// column names at the types ITS arms are reconciled to, which is exactly what
// the enclosing operation will read out of its files.
func setOpNodeDecls(n *logical.Node) colDecls {
	names := setOpOutputNames(n)
	inferred := setOpNodeResultTypes(n)
	if len(names) == 0 || len(inferred) != len(names) {
		return colDecls{}
	}
	types := make(map[string]parquet.TypeID, len(names))
	var dec map[string]logical.DecimalMeta
	for i, name := range names {
		ct := inferred[i]
		if !ct.known {
			continue
		}
		lc := strings.ToLower(name)
		types[lc] = ct.typ
		if ct.typ == parquet.TypeDecimal && ct.decKnown && ct.dec.Precision > 0 {
			if dec == nil {
				dec = make(map[string]logical.DecimalMeta, len(names))
			}
			dec[lc] = ct.dec
		}
	}
	if len(types) == 0 {
		return colDecls{}
	}
	return colDecls{types: types, dec: dec}
}

// projectArmDecls is what a Project EMITS, which is what a derived-table arm's
// SELECT list names (#554).
//
// Only a projection this walk can RESOLVE gets an entry. declaredProjectionDecl
// answers STRING for one it cannot, which is the right advisory answer for a
// wire schema and the wrong one here: an arm confidently typed STRING makes
// the ladder refuse a union of two numbers, and an arm confidently typed with
// the wrong DECIMAL scale moves values by a power of ten.
func projectArmDecls(n *logical.Node, in colDecls) colDecls {
	strictInt := strictIntArithCols(n.Children[0])
	types := make(map[string]parquet.TypeID, len(n.Projections))
	var dec map[string]logical.DecimalMeta
	put := func(name string, d expr.DeclType) {
		lc := strings.ToLower(strings.TrimSpace(name))
		if lc == "" {
			return
		}
		types[lc] = d.ID
		if d.ID == parquet.TypeDecimal && d.DecKnown && d.Precision > 0 {
			if dec == nil {
				dec = make(map[string]logical.DecimalMeta, len(n.Projections))
			}
			dec[lc] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
		}
	}
	for _, proj := range n.Projections {
		name := declaredProjectionName(proj)
		if name == "" {
			continue
		}
		if d, ok := projectionArmDecl(proj, in, strictInt); ok {
			put(name, d)
		}
	}
	if len(types) == 0 {
		return colDecls{}
	}
	return colDecls{types: types, fields: inputColFields(n), dec: dec}
}

// projectionArmDecl resolves one projection the way declaredProjectionDecl
// does, minus its STRING fallback: ok=false means "this walk does not know",
// which the caller reports as an unresolved column rather than as a type.
func projectionArmDecl(proj logical.Projection, decls colDecls, strictInt map[string]bool) (expr.DeclType, bool) {
	if proj.IsAgg {
		name := declaredProjectionName(proj)
		key, ok := lookupColKey(decls.types, name)
		if !ok {
			return expr.DeclType{}, false
		}
		return declFromKey(decls, key), true
	}
	if fc, ok := declaredFieldPath(proj, decls); ok {
		if fc.Type == parquet.TypeDecimal {
			if fc.Precision <= 0 {
				return expr.Decl(parquet.TypeDecimal), true
			}
			return expr.DeclDecimal(fc.Precision, fc.Scale), true
		}
		return expr.Decl(fc.Type), true
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		d, c := nodeDeclaredType(proj.ASTExpr, decls)
		if c != expr.Decided {
			return expr.DeclType{}, false
		}
		if strictInt != nil && expr.IntArithOn() {
			// The integer-preserving-arithmetic hint, the same one
			// inferProjectionDeclType applies (#297, #445): an all-int
			// expression stays INT64 rather than becoming FLOAT64.
			if d2 := inferProjectionDeclType(proj.ASTExpr, parquet.TypeString, strictInt, decls); d2.ID == parquet.TypeInt64 {
				return d2, true
			}
		}
		return d, true
	}
	if cr, ok := bareColRefOf(proj.ASTExpr); ok {
		if c, ok := decls.colDecl(cr); ok {
			if c.Type == parquet.TypeDecimal {
				if c.Precision <= 0 {
					return expr.Decl(parquet.TypeDecimal), true
				}
				return expr.DeclDecimal(c.Precision, c.Scale), true
			}
			return expr.Decl(c.Type), true
		}
		return expr.DeclType{}, false
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	key, ok := lookupColKey(decls.types, ref)
	if !ok {
		return expr.DeclType{}, false
	}
	return declFromKey(decls, key), true
}

// lookupColKey is lookupColType's resolution with the KEY that answered kept,
// so a caller can read the TypeID and the (p,s) out of the same entry. Reading
// them from two lookups is how a declaration comes to describe two different
// columns, which is the mistake ADR-0024 removed from declaredProjectionDecl.
func lookupColKey(colTypes map[string]parquet.TypeID, name string) (string, bool) {
	if colTypes == nil || name == "" {
		return "", false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if _, ok := colTypes[lc]; ok {
		return lc, true
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		if _, ok := colTypes[lc[dot+1:]]; ok {
			return lc[dot+1:], true
		}
	}
	return "", false
}

// declFromKey reads one resolved key's full declaration.
func declFromKey(decls colDecls, key string) expr.DeclType {
	t := decls.types[key]
	if t != parquet.TypeDecimal {
		return expr.Decl(t)
	}
	if m, ok := decls.dec[key]; ok && m.Precision > 0 {
		return expr.DeclDecimal(m.Precision, m.Scale)
	}
	return expr.Decl(parquet.TypeDecimal)
}

// setOpArmComputedSource rewrites an arm's SELECT item that merely FORWARDS a
// derived table's COMPUTED column into the expression that computes it.
//
// walkStages emits no stage for a Project, so an arm's materialized output
// carries SOURCE names — the convention resolveOutputRenameSource compensates
// for by chasing a RENAME down to the column the stream really carries. A
// COMPUTED alias has no such column to chase to: `SELECT x FROM (SELECT i8 + 1
// AS x FROM t) a UNION ALL …` reached the union stage projecting a column
// named `x` over a stream carrying i8, and the task failed with `column "x"
// does not exist in the input schema` — #554's remaining shape, the one #490's
// rename fix does not reach.
//
// The expression is resolved through the level BELOW the Project that owns it,
// so a computed alias sitting on a rename chain (`SELECT y + 1 AS x FROM
// (SELECT i8 AS y FROM t)`) names source columns too.
//
// ok=true ONLY when the chain ends at a computed alias. An AGGREGATE output
// stops it: those exist under the name the aggregate machinery chose, which is
// the alias itself, so forwarding the reference is already right.
func setOpArmComputedSource(name string, n *logical.Node) (plansql.Node, bool) {
	resolved := strings.ToLower(name)
	for n != nil {
		switch {
		case n.Type == logical.NodeProject:
			bare := derivedScopeBareName(resolved, n)
			if proj := projectionForName(n.Projections, resolved, bare); proj != nil {
				if proj.IsAgg {
					return nil, false
				}
				if proj.Column == "" {
					if proj.ASTExpr == nil || len(n.Children) != 1 {
						return nil, false
					}
					if sub, ok := substituteNestedRenameRefs(proj.ASTExpr, n.Children[0]); ok && sub != nil {
						return sub, true
					}
					return proj.ASTExpr, true
				}
				next := proj.Column
				if proj.Expr != "" {
					next = strings.ToLower(proj.Expr)
				}
				if strings.EqualFold(next, resolved) {
					return nil, false
				}
				resolved = next
			}
		case n.Type == logical.NodeAggregate:
			return nil, false
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			if e, ok := setOpArmComputedSource(resolved, n.Children[0]); ok {
				return e, true
			}
			if jt := strings.ToLower(n.JoinType); jt == "semi" || jt == "anti" {
				return nil, false
			}
			return setOpArmComputedSource(resolved, n.Children[1])
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		break
	}
	return nil, false
}

// litOf unwraps an expression that IS a literal, parentheses and all — the
// same shape bareColRefOf accepts for a column reference.
//
// A leading sign is part of the literal here, though the parser makes it a
// UnaryOp: `-1.5000` is numeric(5,4) to PostgreSQL exactly as `1.5000` is,
// and reading only the unsigned spelling would make the sign decide the
// column's TYPE.
func litOf(e plansql.Node) (*plansql.Lit, bool) {
	for {
		switch n := e.(type) {
		case *plansql.Lit:
			return n, true
		case *plansql.ParenNode:
			e = n.Inner
		case *plansql.UnaryOp:
			if n.Op != "-" && n.Op != "+" {
				return nil, false
			}
			e = n.Inner
		default:
			return nil, false
		}
	}
}

// litDeclType is a numeric LITERAL's own type, as PostgreSQL reads its
// SPELLING: `1.23456` is `numeric(6,5)` and `1` is an integer (#665).
//
// It is deliberately NOT wired into nodeDeclaredType's Lit case, which still
// answers FLOAT64 for a fractional literal everywhere else: the declared type
// of a literal in an ARITHMETIC expression is being decided alongside
// ADR-0024 item 3's decimal arithmetic, and declaring DECIMAL over an
// evaluator that still folds a literal into a float64 would write a rounded
// value into an exact vector. A set-operation ARM is the one site where the
// literal's own type is the whole answer — the arm produces the literal and
// nothing else — so it is resolved here and nowhere else.
//
// An exponent (`1e3`, `1.5e-2`) answers false: PostgreSQL types those float8,
// not numeric.
func litDeclType(n *plansql.Lit) (expr.DeclType, bool) {
	if n == nil || n.Kind != plansql.LitNumber {
		return expr.DeclType{}, false
	}
	v := strings.TrimSpace(n.Value)
	if v == "" {
		return expr.DeclType{}, false
	}
	if strings.ContainsAny(v, "eE") {
		return expr.DeclType{}, false
	}
	v = strings.TrimPrefix(strings.TrimPrefix(v, "-"), "+")
	dot := strings.IndexByte(v, '.')
	if dot < 0 {
		return expr.DeclType{}, false
	}
	intPart, frac := v[:dot], v[dot+1:]
	if strings.IndexByte(frac, '.') >= 0 {
		return expr.DeclType{}, false
	}
	for _, s := range []string{intPart, frac} {
		for i := 0; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return expr.DeclType{}, false
			}
		}
	}
	if frac == "" {
		return expr.DeclType{}, false
	}
	// PostgreSQL's numeric literal keeps every digit it was written with, and
	// drops a leading zero's place: `0.5` is numeric(1,1), `1.23456` is
	// numeric(6,5), `12.75` is numeric(4,2).
	prec := len(strings.TrimLeft(intPart, "0")) + len(frac)
	if prec > batch.MaxDecimalPrecision {
		return expr.DeclType{}, false
	}
	return expr.DeclDecimal(prec, len(frac)), true
}
