package physical

import (
	"strconv"
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
	return setOpArmDeclsInScope(n, nil)
}

// setOpArmDeclsInScope is setOpArmDecls carrying the relation names the
// ENCLOSING query has for the subtree it is entering — a derived table's
// alias, a CTE's name and the name one reference gives it — collected on the
// way down from the subtree ROOT, where they are recorded, to the Project that
// publishes the columns they qualify.
//
// It is a SCOPE and not a set of stamps. armScopeNames used to walk the whole
// subtree and collect every derived alias in it, on the claim that a name from
// an INNER derived table "only ever ADDS a resolution for a spelling SQL
// cannot legally write". That claim is false where the inner name is also a
// LEGAL one at the outer level: over
//
//	(SELECT id, dx FROM (SELECT id, dx FROM b) a) s JOIN ja a ON s.id = a.id
//
// the inner `a` was stamped onto the LEFT side's emitted names, so `a.dx`
// existed on both sides of the join at two different (p,s), joinArmDecls
// deleted the contested key as it must, the arm came back untyped and the set
// operation was REFUSED — for a query PostgreSQL answers, where the only
// legal `a` is the join's right side (#682).
func setOpArmDeclsInScope(n *logical.Node, scope []string) colDecls {
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
		// A pass-through node can be the subtree ROOT that carries the alias —
		// `(SELECT … ORDER BY id) s` records `s` on the Sort — so the scope
		// travels down through it to the Project that publishes the columns.
		return setOpArmDeclsInScope(n.Children[0], armScopeAt(n, scope))
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return colDecls{}
		}
		// Each side opens its own scope: a name in scope for the join is not
		// in scope for one of its sides.
		return joinArmDecls(setOpArmDeclsInScope(n.Children[0], nil), setOpArmDeclsInScope(n.Children[1], nil))
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return colDecls{}
		}
		return projectArmDecls(n, setOpArmDeclsInScope(n.Children[0], nil), armScopeAt(n, scope))
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
//
// An ALIAS HIDES the table name, so a scan that has one answers to it alone.
// PostgreSQL is explicit about this — `SELECT ja.dx FROM ja a` is "invalid
// reference to FROM-clause entry for table ja", and wadjet's own resolver
// refuses it too ("missing FROM-clause entry") on every path — so keying the
// table name behind an alias described a spelling neither engine can execute
// and cost a real one: over `(SELECT id, dx FROM jb) ja JOIN ja a`, the
// derived table's legal `ja.dx` collided with the hidden `ja.dx` of the
// aliased base table at a different (p,s), joinArmDecls deleted the contested
// key, and the set operation was refused for a query PostgreSQL answers
// (#682).
func scanArmDecls(n *logical.Node) colDecls {
	if len(n.ScanColTypes) == 0 {
		return colDecls{}
	}
	names := []string{n.TableAlias, n.TableName}
	if strings.TrimSpace(n.TableAlias) != "" {
		names = names[:1]
	}
	quals := make([]string, 0, 2)
	for _, q := range names {
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
	// The ROW FIELDS are deliberately NOT carried across the join. A field
	// path resolves against them, and over a join the union stage cannot
	// SPELL one: the join's output names its ROW column `a.rd` while the
	// SELECT list wrote `rd.d`, so typing the arm only moves the failure from
	// a plan-time refusal naming the column to a task error naming a column
	// that does not exist. Leaving the arm untyped is what routes it to the
	// refusal, which is the better of the two loud answers.
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
func projectArmDecls(n *logical.Node, in colDecls, quals []string) colDecls {
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
	// quals is the SCOPE this Project's output answers to — a derived table's
	// alias, a CTE's name — keyed alongside the bare ones, exactly as
	// scanArmDecls keys a scan's table alias. Without them a DERIVED side of a
	// join contributed only bare names, joinArmDecls deleted the contested
	// one, and `s.dx` over `(SELECT id, dx FROM b) s JOIN a` resolved to
	// nothing: the arm came back untyped and every file kept its own scale —
	// #551 again, one node down.
	for _, proj := range n.Projections {
		name := declaredProjectionName(proj)
		if name == "" {
			continue
		}
		// The whole TERM first, because the alternative is not "no answer"
		// but a CONFIDENT WRONG one: projectionArmDecl reads
		// `n_regionkey + 1` as arithmetic, cannot resolve `n_regionkey`
		// above the aggregate that groups by it, and falls to the float
		// rule. FLOAT64 then reconciles the set operation to double where
		// PostgreSQL resolves bigint.
		d, ok := wholeTermArmDecl(proj, in)
		if !ok {
			// Below an aggregate a
			// computed GROUP BY key is emitted under the key's own
			// expression text, so `n_regionkey + 1 AS gk` is a projection
			// whose source is ONE column named `n_regionkey + 1` — and a
			// walk that reads it as arithmetic looks for `n_regionkey`,
			// which the aggregate's output does not carry, and gives up.
			//
			// Giving up is not neutral here. An untyped arm sends
			// reconcileSetOpArmTypes to FLOAT64 and makes it CAST the other
			// arm, so `WITH a AS (SELECT g+1 AS gk, COUNT(*) AS n FROM t
			// GROUP BY g+1) SELECT gk FROM a UNION ALL SELECT g FROM t`
			// resolved double where PostgreSQL resolves bigint — and once
			// the aggregate arm stopped answering NULL, the two arms
			// disagreed about what `gk` IS and the sort above indexed an
			// empty column (#656 R4).
			d, ok = projectionArmDecl(proj, in, strictInt)
		}
		if ok {
			put(name, d)
			for _, q := range quals {
				put(q+"."+strings.ToLower(strings.TrimSpace(name)), d)
			}
		}
	}
	if len(types) == 0 {
		return colDecls{}
	}
	return colDecls{types: types, fields: inputColFields(n), dec: dec}
}

// armScopeAt adds the relation names recorded ON ONE NODE to the scope in
// force: the DERIVED TABLE alias the enclosing query gave this subtree, a
// CTE's name, and the alias one reference gives that CTE. All three are
// recorded on the subtree ROOT (logical.setSubtreeAlias, resolveTableOrCTE),
// which is why the scope is collected on the way DOWN rather than gathered
// from the whole subtree: the stamps a scan carries include the ones every
// ENCLOSING derived table applied, and those name a different level.
func armScopeAt(n *logical.Node, scope []string) []string {
	out := scope
	add := func(name string) {
		lc := strings.ToLower(strings.TrimSpace(name))
		if lc == "" {
			return
		}
		for _, have := range out {
			if have == lc {
				return
			}
		}
		out = append(append([]string(nil), out...), lc)
	}
	add(n.DerivedAlias)
	add(n.CTEName)
	add(n.CTERefAlias)
	return out
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

// setOpLitDecimal is a numeric LITERAL arm's exact value: the DECIMAL its
// spelling names, and the plain decimal TEXT that carries it.
//
// The text is not decoration. The arm's projection is EVALUATED, and the
// evaluator folds a numeric literal into a float64 box — `1234567890123456.78`
// becomes 1234567890123456.8 before it reaches the DECIMAL vector, and
// exec.Project's checked writer then stores that float's own shortest decimal
// faithfully. Declaring DECIMAL over a float box makes the type say EXACT
// about a number that is already rounded, which is worse than the float8
// column the arm had before. So the arm's expression is rewritten to the
// literal's text as a QUOTED string, which SetValueChecked parses at the
// column's scale with no float in between (ADR-0024 item 4) — the same shape
// coerceSetOpArmRows already hands the single-process path.
type setOpLitDecimal struct {
	decl expr.DeclType
	text string
}

// setOpLitArm reads a set-operation arm's SELECT item as a numeric literal,
// parentheses and a leading sign included: the parser makes the sign a
// UnaryOp, and `-1.5000` is numeric(5,4) to PostgreSQL exactly as `1.5000` is.
func setOpLitArm(e plansql.Node) (setOpLitDecimal, bool) {
	neg := false
	for {
		switch n := e.(type) {
		case *plansql.Lit:
			d, ok := litDeclType(n)
			if !ok {
				return setOpLitDecimal{}, false
			}
			if neg && strings.Trim(d.text, "0.") != "" {
				// A signed ZERO keeps no sign: "-0.00" reads back as the same
				// carrier and only makes the rewritten SQL odder to read.
				d.text = "-" + d.text
			}
			return d, true
		case *plansql.ParenNode:
			e = n.Inner
		case *plansql.UnaryOp:
			switch n.Op {
			case "-":
				neg = !neg
			case "+":
			default:
				return setOpLitDecimal{}, false
			}
			e = n.Inner
		default:
			return setOpLitDecimal{}, false
		}
	}
}

// litDeclType is a numeric LITERAL's own type, as PostgreSQL reads its
// SPELLING, with the plain decimal text that spelling expands to.
//
// PostgreSQL's rule, verified live against 17.11 with pg_typeof:
//
//	1.23456  1.  0.0        -> numeric   (a decimal point)
//	1e2  1.5e1  1.5e-2      -> numeric   (an exponent, WITH or without a point)
//	1  1234567890           -> integer
//	12345678901             -> bigint
//	123456789012345678901   -> numeric   (too wide for bigint)
//
// So a literal is numeric when it carries a decimal point OR an exponent, and
// an INTEGER literal is numeric only when no integer type holds it. The
// integer forms answer false here and stay on the ladder's integer rung, where
// an integer arm contributes its whole range's digits.
//
// The (p,s) is the digits the literal EXPANDS to — PostgreSQL's numeric
// constant carries typmod −1 and an exact value, and a finite carrier needs a
// declaration wide enough to hold that value without moving it. `1e2` is
// numeric(3,0), `1.5e-2` is numeric(3,3), `0.5` is numeric(1,1) (a leading
// zero holds no place), and trailing zeros count because they are digits the
// query wrote and a set operation must not drop a scale it stated.
//
// It is deliberately NOT wired into nodeDeclaredType's Lit case, which still
// answers FLOAT64 for a fractional literal everywhere else: the declared type
// of a literal in an ARITHMETIC expression is being decided alongside
// ADR-0024 item 3's decimal arithmetic. A set-operation ARM is the one site
// where the literal's own type is the whole answer — the arm produces the
// literal and nothing else.
func litDeclType(n *plansql.Lit) (setOpLitDecimal, bool) {
	if n == nil || n.Kind != plansql.LitNumber {
		return setOpLitDecimal{}, false
	}
	v := strings.TrimSpace(n.Value)
	v = strings.TrimPrefix(strings.TrimPrefix(v, "-"), "+")
	if v == "" {
		return setOpLitDecimal{}, false
	}
	mant, expPart, hasExp := v, "", false
	if i := strings.IndexAny(v, "eE"); i >= 0 {
		mant, expPart, hasExp = v[:i], v[i+1:], true
	}
	dot := strings.IndexByte(mant, '.')
	intPart, frac := mant, ""
	if dot >= 0 {
		intPart, frac = mant[:dot], mant[dot+1:]
	}
	if !allDigits(intPart) || !allDigits(frac) || intPart+frac == "" {
		return setOpLitDecimal{}, false
	}
	exp := 0
	if hasExp {
		e, err := strconv.Atoi(expPart)
		if err != nil {
			return setOpLitDecimal{}, false
		}
		exp = e
	}
	if dot < 0 && !hasExp {
		// A plain integer literal. It is numeric to PostgreSQL only when no
		// integer type holds it; otherwise the ladder's integer rung answers
		// and this declines, exactly as it did before.
		if _, err := strconv.ParseInt(intPart, 10, 64); err == nil {
			return setOpLitDecimal{}, false
		}
	}
	// Expand to a plain decimal spelling: digits with the point moved by exp.
	digits := intPart + frac
	point := len(intPart) + exp
	var whole, fracOut string
	switch {
	case point >= len(digits):
		whole = digits + strings.Repeat("0", point-len(digits))
	case point <= 0:
		whole, fracOut = "0", strings.Repeat("0", -point)+digits
	default:
		whole, fracOut = digits[:point], digits[point:]
	}
	prec := len(strings.TrimLeft(whole, "0")) + len(fracOut)
	if prec == 0 {
		prec = 1
	}
	if prec > batch.MaxDecimalPrecision || len(fracOut) > batch.MaxDecimalScale {
		// No DECIMAL declaration this carrier can honour. Declaring one
		// anyway would move the value; the float8 the arm had stands.
		return setOpLitDecimal{}, false
	}
	text := whole
	if fracOut != "" {
		text += "." + fracOut
	}
	return setOpLitDecimal{decl: expr.DeclDecimal(prec, len(fracOut)), text: text}, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// wholeTermArmDecl resolves a projection whose ENTIRE expression is the name
// of one input column — the shape a computed GROUP BY key takes above its
// aggregate, where the stage emits the key under the expression's own text.
//
// It runs only after projectionArmDecl declines, so an expression that really
// is arithmetic over resolvable columns keeps its inferred type.
func wholeTermArmDecl(proj logical.Projection, in colDecls) (expr.DeclType, bool) {
	term := strings.TrimSpace(proj.Expr)
	if term == "" || proj.ASTExpr == nil {
		return expr.DeclType{}, false
	}
	if _, isRef := proj.ASTExpr.(*plansql.ColRef); isRef {
		return expr.DeclType{}, false // an ordinary reference is projectionArmDecl's job
	}
	lc := strings.ToLower(term)
	t, ok := in.types[lc]
	if !ok {
		return expr.DeclType{}, false
	}
	d := expr.DeclType{ID: t}
	if t == parquet.TypeDecimal {
		m, has := in.dec[lc]
		if !has || m.Precision <= 0 {
			// A DECIMAL with no (p,s) is not a type (ADR-0024 item 2).
			return expr.DeclType{}, false
		}
		d.Precision, d.Scale, d.DecKnown = m.Precision, m.Scale, true
	}
	return d, true
}
