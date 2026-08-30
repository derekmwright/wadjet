package physical

import (
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A window's PARTITION BY / ORDER BY terms, and the two ways they stopped
// naming a column (#585).
//
// exec.Window reads its keys by NAME off the input batch, so a term the batch
// does not carry used to drop out of the key list — and a window whose keys
// all drop out is a window over ONE partition spanning the whole input. Two
// spellings a BI client produces routinely did exactly that:
//
//	PARTITION BY p.g       the batch carries `g`; the qualifier misses
//	PARTITION BY id % 3    nothing computed it, so no batch carries it
//
// Both answered silently and wrongly: ROW_NUMBER() numbered straight through
// every group, SUM OVER returned the whole-table sum.
//
// The two halves need different repairs, and this file is where the choice is
// made once for BOTH execution paths — buildWindow's operator and walkStages'
// stage spec go through windowExecColumn, which calls resolveWindowKeys, so
// the single-process pipeline and the DAG cannot come to different
// conclusions about what a key names.
//
//   - A QUALIFIED reference is BOUND to the input column, which is the rule
//     #488 settled for a qualified ORDER BY at the query root: `p.g` is `g`
//     in the FROM scope, whatever the SELECT list calls it. The binding is
//     done here, at plan time, and not left to exec.Window's runtime fallback
//     alone, because the key name is ALSO the DAG's clustering key — the
//     exchange ahead of a window stage hash-partitions on it (distribution.go,
//     windowPartitionKeys), and it cannot partition on a name no upstream
//     stage emits.
//
//   - An EXPRESSION is MATERIALIZED as a computed column, exactly as a GROUP
//     BY expression is pre-projected for the hash aggregate
//     (`SUBSTR(c_phone, 1, 2)` — plan.go's preProjectCols, and the worker's
//     buildAggInputProjection). It is named __winkey_N, the same synthetic
//     convention __gb_expr_N uses and for the same reason: the expression's
//     own TEXT is already a key elsewhere — exec.Project resolves a
//     projection's output type by looking its source spelling up in the input
//     batch — so a column literally named `id % 7` made the SELECT list's own
//     `id % 7 AS k` type itself from the window's scratch column and write an
//     INT64 value through a FLOAT64 evaluator.
//
//     Nothing ABOVE the window reads __winkey_N, which is what keeps it clear
//     of #558: the SELECT-list projection drops it on the single-process path
//     and the gather projects to the visible output on the DAG. It is never a
//     sort key, so the materialize-above-a-window problem that issue names
//     does not arise.
//
// Anything this pass cannot resolve is left exactly as written and reaches
// exec.Window.bindKeyNames, which refuses it. Degrading to one partition is
// never an outcome either half can produce.

// windowKeyColPrefix names a materialized window key. The "__" marks it
// derived, the same convention as __sortkey_N and __gb_expr_N.
const windowKeyColPrefix = string(SlotWindowKey)

// windowKey is one resolved PARTITION BY or window ORDER BY term.
type windowKey struct {
	// Name is what the operator reads off the batch: the input column for a
	// bound reference, the synthetic __winkey_N for a materialized one.
	Name string
	// Expr is the expression to compute under Name, nil for a term that
	// names a column already; Text is its SQL, which is what the DAG's stage
	// spec carries (a worker has no AST).
	Expr plansql.Node
	Text string
	// Field is the ROW field a field-path key names, and nil for every other
	// key. It is what types the materialized column: the field name alone
	// types as STRING (#568), which for an INT64 field is a wrong number
	// rather than a missing one.
	Field *parquet.Column
	// Type is the declared type of the materialized column. Only meaningful
	// when Expr is non-nil.
	Type parquet.TypeID
	// Precision/Scale carry a materialized DECIMAL key's (p,s), which Type
	// alone cannot (ADR-0024 item 2).
	Precision int
	Scale     int
}

// resolveWindowKeys resolves the PARTITION BY / ORDER BY terms of the window
// expressions on node, keyed by the term's original text.
//
// The map is keyed by text and not by position because one window stage
// carries several OVER clauses that routinely share terms, and a shared term
// must resolve to one column: two clauses partitioning on `id % 3` compute it
// once, and two spelling `g` and `p.g` end up on the same group (which is
// what lets exec.Window run them in a single pass).
func resolveWindowKeys(node *logical.Node) map[string]windowKey {
	if node == nil || len(node.Children) != 1 {
		return nil
	}
	child := node.Children[0]
	colTypes := inputColTypes(child)
	// The types a materialized key is DECLARED from can come from further
	// down than the names are bound against: inputColTypes stops at an
	// aggregate, and a window over a GROUP BY is an ordinary shape whose
	// keys are the group keys. See windowKeyInputTypes.
	typeCols, strictInt := windowKeyInputTypes(child)
	// The (p,s) beside the TypeIDs: a materialized DECIMAL key's vector is
	// built from this declaration, and one without a scale reads every value
	// back at 10^0 (ADR-0024 item 2).
	typeDec := windowKeyInputDecimal(child)
	colFields := inputColRowFields(child)
	out := map[string]windowKey{}
	// One allocator for this window stage's keys, seeded with the names its
	// INPUT already carries — a stored `__winkey_N` column among them — so a
	// materialized key can never land on a name the batch already has, and two
	// keys can never land on each other's (reserved_slots.go, SlotAllocator).
	keyAlloc := plansql.NewSlotAllocator()
	for name := range colTypes {
		keyAlloc.Seed(name)
	}
	add := func(term string) {
		term = strings.TrimSpace(term)
		if term == "" {
			return
		}
		if _, dup := out[term]; dup {
			return
		}
		// cleanExpr is the spelling every other window consumer already got:
		// it drops the table qualifier, which is why `PARTITION BY p.g` used
		// to reach the operator as the bare `g` and work — right up until a
		// join made two columns share that bare name. The cases below only
		// ever narrow it further.
		k := windowKey{Name: cleanExpr(term)}
		if ast, err := plansql.ParseExpression(term); err == nil {
			ref, isCol := ast.(*plansql.ColRef)
			switch {
			case !isCol:
				k.Expr, k.Text = ast, term
			case fieldOf(colFields, ref) != nil:
				// `rw.f` over a ROW column is a FIELD PATH, not a qualified
				// reference — dropping the qualifier would look up a column
				// named `f` that does not exist. It has to be evaluated, so
				// it takes the materialized route like any other expression,
				// and it is typed from the FIELD rather than from the name
				// (#603, #568's rule).
				k.Expr, k.Text = ast, ast.String()
				k.Field = fieldOf(colFields, ref)
			case ref.Table != "":
				if bound, ok := bindWindowColRef(ref, colTypes); ok {
					k.Name = bound
				}
			}
		}
		if k.Expr != nil {
			fresh, ok := keyAlloc.Next(SlotWindowKey)
			if !ok {
				return // the family is exhausted; leave this key as written
			}
			k.Name = fresh
			if k.Field != nil {
				k.Type, k.Precision, k.Scale = k.Field.Type, k.Field.Precision, k.Field.Scale
			} else {
				// The declaration is inferred from the expression RESPELLED
				// through any derived-table or CTE rename between here and
				// the producer, because that is where the column
				// declarations live: `SUM(v * 2) OVER ()` over
				// `SELECT d_4 AS v` types `v` from nothing and falls back to
				// the float rule, and with exact DECIMAL arithmetic the
				// evaluator then hands an exact value to a FLOAT64 vector —
				// `cannot store string into FLOAT64 vector`, on BOTH paths.
				//
				// Only the TYPE is taken from the respelled form. The TEXT
				// keeps the alias, because the single-process pipeline runs
				// the Project below the window as a real operator and its
				// output really is called `v`; the DAG respells the text as
				// well, where that Project emits no stage (#672, #656).
				typed := k.Expr
				if node != nil && len(node.Children) == 1 {
					if r, ok := respellDerivedAliasRefs(k.Expr, node.Children[0]); ok {
						typed = r
					}
				}
				k.Type, k.Precision, k.Scale = declTypeParts(
					inferProjectionDeclType(typed, parquet.TypeString, strictInt,
						colDecls{types: typeCols, dec: typeDec}))
			}
		}
		out[term] = k
	}
	for _, we := range node.WindowExprs {
		// The ARGUMENT is a key candidate too, and for the same reason the
		// terms are: exec.Window reads it by NAME off the input batch, so an
		// argument no column is named after resolves to no input vector and
		// the operator writes NULL in every row.
		//
		// Two spellings do that. A ROW FIELD PATH — `SUM(rw.f) OVER ()`
		// reached the operator as the bare `f` (#603). And an EXPRESSION —
		// `SUM(d * 2) OVER ()`, `AVG(CASE …) OVER ()`, `MAX(COALESCE(x, 0))
		// OVER ()` — which nothing ever computed, so every window over an
		// expression answered NULL on both paths, for every input type
		// (#672). Both are materialized as __winkey_N and read under that
		// name, exactly as a computed PARTITION BY term is.
		//
		// A bare or qualified column keeps today's route (exec.Window's
		// qualified-to-bare fallback settles it), and `*` and a literal are
		// not arguments to materialize at all: COUNT(*) counts rows, and a
		// constant is one the operator already has.
		if col := strings.TrimSpace(we.InputColumn()); col != "" {
			if ast, err := plansql.ParseExpression(col); err == nil {
				switch e := ast.(type) {
				case *plansql.ColRef:
					if fieldOf(colFields, e) != nil {
						add(col)
					}
				case *plansql.StarNode, *plansql.Lit, *plansql.IntervalLit:
					// Nothing to compute.
				default:
					add(col)
				}
			}
		}
		for _, pb := range we.PartitionBy {
			add(pb)
		}
		for _, ob := range we.OrderBy {
			add(ob.Column)
		}
	}
	return out
}

// fieldOf is windowRowField as a pointer test, for the switch above.
func fieldOf(colFields map[string][]parquet.Column, ref *plansql.ColRef) *parquet.Column {
	f, ok := windowRowField(colFields, ref)
	if !ok {
		return nil
	}
	return &f
}

// windowKeyInputTypes resolves the column types and integer-arithmetic hints a
// materialized window key is DECLARED from.
//
// inputColTypes stops at an aggregate, and a window over a GROUP BY is an
// ordinary shape — `ROW_NUMBER() OVER (PARTITION BY g % 2)` beside
// `GROUP BY g`. With no types the inference falls back to FLOAT64 for
// arithmetic, and an INT64 key declared FLOAT64 loses precision past 2^53:
// two partitions that differ in their last digits become one, which is #585's
// symptom reached by a second route. The aggregate's own pre-projection
// resolves its GROUP BY expressions against the aggregate's INPUT for the
// same reason (derivedGroupKeyTypes), so this does too, with the aggregate's
// declared outputs laid over the top.
func windowKeyInputTypes(child *logical.Node) (map[string]parquet.TypeID, map[string]bool) {
	if t := inputColTypes(child); len(t) > 0 {
		return t, strictIntArithCols(child)
	}
	// A DERIVED TABLE between the window and its producer: inputColTypes
	// answers nothing for a Project, so a materialized key over one of its
	// aliases had no declarations at all and fell to the float rule. With
	// exact DECIMAL arithmetic the evaluator then hands an exact value to a
	// FLOAT64 vector — `cannot store string into FLOAT64 vector`, on BOTH
	// paths, for `SUM(v * 2) OVER ()` over `SELECT d_4 AS v`. These are the
	// declarations the key is respelled against (#672, #656).
	if t := sourceColTypesThroughRenames(child); len(t) > 0 {
		return t, strictIntArithColsThroughRenames(child)
	}
	agg := aggregateUnderWindow(child)
	if agg == nil || len(agg.Children) != 1 {
		return nil, nil
	}
	base := inputColTypes(agg.Children[0])
	if len(base) == 0 {
		return nil, nil
	}
	types := make(map[string]parquet.TypeID, len(base)+len(agg.AggExprs))
	for name, t := range base {
		types[name] = t
	}
	for _, a := range agg.AggExprs {
		if t, known := aggSpecOutputType(agg, a); known {
			types[strings.ToLower(a.OutputCol)] = t
		}
	}
	return types, strictIntArithCols(agg.Children[0])
}

// windowKeyInputDecimal is windowKeyInputTypes' companion for DECIMAL
// precision and scale, over the same two shapes: the window's own input, or
// the aggregate below it when the window reads a GROUP BY's output.
func windowKeyInputDecimal(child *logical.Node) map[string]logical.DecimalMeta {
	if d := inputColDecimal(child); len(d) > 0 {
		return d
	}
	// windowKeyInputTypes' derived-table fallback, for the (p,s) half: a
	// DECIMAL declared without its scale is not a declaration at all
	// (ADR-0024 item 2), so the two have to come from the same place.
	if d := sourceColDeclsThroughRenames(child).dec; len(d) > 0 {
		return d
	}
	agg := aggregateUnderWindow(child)
	if agg == nil || len(agg.Children) != 1 {
		return nil
	}
	base := inputColDecimal(agg.Children[0])
	out := make(map[string]logical.DecimalMeta, len(base)+len(agg.AggExprs))
	for name, m := range base {
		out[name] = m
	}
	for _, a := range agg.AggExprs {
		if m, known := aggSpecOutputDecimal(agg, a); known {
			out[strings.ToLower(a.OutputCol)] = m
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// aggregateUnderWindow finds the Aggregate a window reads from, descending
// only through nodes that pass their input's columns along. The bound is
// logical.aggregateBelow's shape; the depth cap is there so a malformed plan
// cannot spin.
func aggregateUnderWindow(n *logical.Node) *logical.Node {
	for depth := 0; n != nil && depth < 8; depth++ {
		switch n.Type {
		case logical.NodeAggregate:
			return n
		case logical.NodeFilter, logical.NodeProject, logical.NodeSort,
			logical.NodeLimit, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// bindWindowColRef reports the input column a window key's column reference
// names, and false when the input's column set cannot settle it.
//
// A BARE reference is taken at its word: it is already the spelling every
// resolver downstream expects, and exec.Window's runtime fallback covers the
// case where the batch happens to carry it qualified (a join qualifying a
// colliding name).
//
// A QUALIFIED one is bound: the exact spelling first, since a join really can
// emit `p.g`, then the bare column. Failing both it is left alone — the input
// column set is unknown for the node types inputColTypes declines (an
// aggregate or another window below), and inventing a binding from an empty
// map would rename a key that was right.
func bindWindowColRef(ref *plansql.ColRef, colTypes map[string]parquet.TypeID) (string, bool) {
	if ref.Table == "" {
		return "", false
	}
	if len(colTypes) == 0 {
		return "", false
	}
	qualified := ref.Table + "." + ref.Column
	if _, ok := colTypes[strings.ToLower(qualified)]; ok {
		return qualified, true
	}
	if _, ok := colTypes[strings.ToLower(ref.Column)]; ok {
		return ref.Column, true
	}
	return "", false
}

// windowRowField resolves `rw.f` against the input's ROW declarations,
// returning the field's own parquet.Column.
//
// A field path parses to the same plansql.ColRef a table-qualified reference
// does, and the window path had no way to tell them apart — so `rw.f` became
// the bare `f`, a column of nothing, and every window consumer of a field
// path answered silently wrong: `SUM(rw.f) OVER ()` NULL in every row,
// `LAG(rw.f)` NULL, `ORDER BY rw.f` ignored, `PARTITION BY rw.f` one partition
// (#603). The field's DECLARED column is what the materialized key is typed
// from — the field name alone types as STRING, which is #568's defect reached
// through the window.
//
// The lookup walks the input's ROW columns (logical.Node.ScanColFields,
// populated from the catalog). #568 introduces a general colDecls lookup over
// the same map; when the two meet, this is the caller to point at it.
func windowRowField(colFields map[string][]parquet.Column, ref *plansql.ColRef) (parquet.Column, bool) {
	if ref == nil || ref.Table == "" {
		return parquet.Column{}, false
	}
	for _, f := range colFields[strings.ToLower(ref.Table)] {
		if strings.EqualFold(f.Name, ref.Column) {
			return f, true
		}
	}
	return parquet.Column{}, false
}

// inputColRowFields is inputColTypes' companion for ROW field declarations:
// the same walk, sourced from ScanColFields, holding an entry only for a ROW
// column. A name two scans disagree on is dropped rather than picking a side,
// the rule inputColTypes and inputColDecimal both apply.
func inputColRowFields(n *logical.Node) map[string][]parquet.Column {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		return n.ScanColFields
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct,
		logical.NodeProject, logical.NodeAggregate, logical.NodeWindow:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColRowFields(n.Children[0])
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left, right := inputColRowFields(n.Children[0]), inputColRowFields(n.Children[1])
		if len(left) == 0 {
			return right
		}
		if len(right) == 0 {
			return left
		}
		merged := make(map[string][]parquet.Column, len(left)+len(right))
		for c, f := range left {
			merged[c] = f
		}
		for c, f := range right {
			if _, dup := merged[c]; dup {
				delete(merged, c)
				continue
			}
			merged[c] = f
		}
		return merged
	}
	return nil
}

// windowKeySpecs lists the materialized keys of a window node, for the DAG's
// stage spec and for the single-process pre-projection.
//
// The order is __winkey_N's own, which resolveWindowKeys assigns by first
// encounter over the window's expressions — a slice, so the numbering (and
// therefore the plan) is deterministic where iterating the map would not be.
func windowKeySpecs(keys map[string]windowKey) []ProjectExprSpec {
	if len(keys) == 0 {
		return nil
	}
	specs := make([]ProjectExprSpec, 0, len(keys))
	for _, k := range keys {
		if k.Expr == nil {
			continue
		}
		specs = append(specs, ProjectExprSpec{
			Expr: k.Text,
			Name: k.Name,
			// The materialized column exists in no catalog, so its declared
			// type IS its runtime type, and TypeKnown must ride along
			// because Type's zero value is TypeBool — materializeSortKey's
			// rule, for the same reason (#445, #472).
			Type:      k.Type,
			TypeKnown: true,
			Precision: k.Precision,
			Scale:     k.Scale,
		})
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

// windowKeyProjections compiles the materialized keys into the pre-window
// projection the single-process pipeline runs. Returns nil when every key
// already names a column.
//
// A key whose expression will not compile is NOT skipped: leaving it out puts
// exec.Window back where #585 found it, resolving a name nothing produces.
// The refusal is the planner's, where the expression text is still available
// to name in the message.
func (p *Planner) windowKeyProjections(keys map[string]windowKey) ([]exec.ProjectColumn, error) {
	specs := windowKeySpecs(keys)
	if len(specs) == 0 {
		return nil, nil
	}
	byName := make(map[string]windowKey, len(keys))
	for _, k := range keys {
		if k.Expr != nil {
			byName[k.Name] = k
		}
	}
	cols := make([]exec.ProjectColumn, 0, len(specs))
	for _, spec := range specs {
		k := byName[spec.Name]
		compiled, err := expr.CompileWithRunner(k.Expr, p.subqueryRunner)
		if err != nil {
			return nil, windowKeyCompileError(k.Text, err)
		}
		pc := exec.ProjectColumn{
			Name:      k.Name,
			Type:      k.Type,
			Precision: k.Precision,
			Scale:     k.Scale,
			Expr:      wrapExpr(compiled),
			Computed:  true,
		}
		if ve, ok := compiled.(expr.VecExpr); ok {
			pc.VecEval = ve.EvalVec
		}
		cols = append(cols, pc)
	}
	return cols, nil
}

// windowKeyCompileError reports a window key the engine cannot evaluate. A
// window that cannot honour its PARTITION BY has no answer to give — the one
// it used to give, computed over a single partition, was a different query's
// answer (#585).
func windowKeyCompileError(term string, err error) error {
	return &windowKeyError{term: term, err: err}
}

type windowKeyError struct {
	term string
	err  error
}

func (e *windowKeyError) Error() string {
	return "window PARTITION BY/ORDER BY " + e.term + ": " + e.err.Error()
}

func (e *windowKeyError) Unwrap() error { return e.err }

// validateWindowKeyExprs checks that every materialized key a window stage
// names is one its fragment can compute. See the StageWindow case in
// native_dag_rewrite.go for why this is a plan-time check.
func validateWindowKeyExprs(s Stage) error {
	if len(s.WindowCols) == 0 {
		return nil
	}
	computed := make(map[string]bool, len(s.WindowKeyExprs))
	for _, k := range s.WindowKeyExprs {
		computed[strings.ToLower(k.Name)] = true
	}
	check := func(name string) error {
		if !strings.HasPrefix(strings.ToLower(name), windowKeyColPrefix) ||
			computed[strings.ToLower(name)] {
			return nil
		}
		return fmt.Errorf("native-DAG: window stage %s keys on %q, which nothing computes "+
			"(WindowKeyExprs carries %d entries)", s.ID, name, len(s.WindowKeyExprs))
	}
	for _, wc := range s.WindowCols {
		if err := check(wc.InputCol); err != nil {
			return err
		}
		for _, pb := range wc.PartitionBy {
			if err := check(pb); err != nil {
				return err
			}
		}
		for _, ob := range wc.OrderBy {
			if err := check(ob.Column); err != nil {
				return err
			}
		}
	}
	return nil
}
