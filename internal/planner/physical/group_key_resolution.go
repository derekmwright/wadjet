package physical

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// resolveStageGroupKeys settles the RESOLUTION spelling of every GROUP BY key
// that names a derived table's COMPUTED alias, against what the producing
// fragment really emits.
//
// It runs at the END of planning for the reason `resolveFilterAliasSpelling`
// and `resolveDerivedAliasSortKeys` do: the two candidate spellings for such a
// key are the ALIAS and the expression that DEFINES it, and which one a
// fragment carries is decided by `attachScanSelectProjections` and
// `absorbWindowArmProjection`, which run after `walkStages` emits the stage.
// ADR-0026 §4a records three attempts to infer it from NODE KINDS at emission
// time, each wrong in a different direction; the answer is not a property of
// the logical plan at all.
//
// The rules, in order, and each is a statement the stream model can make:
//
// EVERY rule asks WHICH ARM first. The key names a derived table, that table
// is one arm of the join, and a column of the same name on another arm is a
// different value — so a candidate is only a candidate if it came from the arm
// the key names. `keyArmConstraint` answers that from the model's per-column
// arm: the build alias the join declares, or "" for the probe side.
//
// Skipping it is a silent wrong answer and not a missed optimisation. With the
// key naming a build arm whose own inner ORDER BY / LIMIT stopped
// `attachScanSelectProjections` from materialising the alias, the bare column
// of that name in the stream is the PROBE's, and the fragment then groups by a
// different table's value under the key's name (#781's R6/R8 cell — four
// shapes answering `x.a * 3` where the key is `z.a * 3`).
//
//  1. the stream spells the alias EXACTLY (`y.w`), because the join qualified
//     this arm's duplicate column — resolve by that name;
//  2. the stream carries exactly ONE BARE column of the alias's bare name
//     FROM THE KEY'S ARM, some fragment COMPUTED it, and no arm's column of
//     that name was dropped — resolve by the bare name;
//  3. no bare one, and exactly ONE QUALIFIED column of that bare name from
//     that arm, whose qualifier is the alias's own or the key was written
//     bare — resolve by the qualified name. This is the runtime's own
//     qualified↔bare fallback, decided where the key is decided rather than
//     left to a lookup;
//  4. the key's ARM carries every column the DEFINITION reads — resolve by
//     the definition RE-SPELLED into the spellings that arm's columns have in
//     the stream (`a * 3` becomes `z.a * 3` where the join qualified z's
//     copy), which the fragment materializes into a hidden slot;
//  5. none of the above: the arm carries the value nowhere. REFUSED with the
//     arm named, and the coordinator answers the query on its local pipeline,
//     where the derived table's Project is a real operator and the alias is a
//     real column.
//
// Rule 2's MATERIALIZED test is what keeps it off the shape that killed the
// last attempt: `(SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) x GROUP BY
// g` puts a window alias over a table that has its own `g`, and the stream
// carries that base column under the same name. Nothing computed it, so rule 2
// declines and rule 4 answers `__win_0 + 0` — which is what the DAG has always
// evaluated there, correctly.
func resolveStageGroupKeys(stages []Stage) error {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	for i := range stages {
		s := &stages[i]
		if len(s.GroupByResolve) == 0 || !stageComputesGroupKeys(s) {
			continue
		}
		var in []streamCol
		var arms map[string]bool
		resolved := false
		for k := range s.GroupByResolve {
			r := &s.GroupByResolve[k]
			if !r.deferred() {
				continue
			}
			if !resolved {
				in, arms = aggregateInputStreamColumns(stages, idx, s)
				resolved = true
			}
			expr, computed, err := resolveDerivedAliasKey(*r, in, arms, s.ID)
			if err != nil {
				return err
			}
			r.Expr, r.Computed = expr, computed
			r.Alias, r.Def = "", ""
		}
		for k, r := range s.GroupByResolve {
			if err := refuseUnevaluableGroupKey(r, stageGroupKeyList(s), k, s.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// refuseUnevaluableGroupKey refuses a key whose RESOLUTION contains an
// AGGREGATE or WINDOW call.
//
// A materialized key is built by a pre-aggregate PROJECTION, and a projection
// evaluates a scalar over one row: `COUNT(*)` and `SUM(a) OVER ()` are neither.
// The value they name was computed by the operator BELOW and published under a
// slot (`__agg_0`, `__win_0`), and only that slot is evaluable here.
//
// `SELECT DISTINCT g, COUNT(*) + 0 AS w FROM t GROUP BY g` is where such a key
// comes from: the DISTINCT lowering makes every SELECT item a GROUP BY key, so
// the aggregate CALL becomes a key expression and the stage carries that text.
// Both of the key's names are then that text, and they agree — the two-name
// carrier has nothing to separate. The fragment evaluated `count(*)` against a
// batch and answered ONE NULL group over the whole table (the window spelling
// failed loudly instead: `GROUP BY key "sum(a) OVER (...) + 0" is not a column
// of its input`).
//
// Refusing routes the query to the coordinator-local pipeline, where the
// aggregate and the window are real operators and their outputs are real
// columns, and it answers PostgreSQL's rows. The real repair is for the
// DISTINCT lowering to record the SLOT the operator below publishes instead of
// the call it was written as — a defect of that rewrite, not of this carrier,
// and it is the next lead in this family.
func refuseUnevaluableGroupKey(r GroupKeyResolution, keys []string, i int, stageID string) error {
	if !r.Computed || r.Expr == "" {
		return nil
	}
	name := r.Expr
	if i < len(keys) {
		name = keys[i]
	}
	node, err := plansql.ParseExpression(r.Expr)
	if err != nil {
		// A resolution the PLANNER cannot parse is one the worker cannot
		// compile either, and its projection would fail the task. It is the
		// same shape as the two below wearing a rendering: a window call is
		// printed `sum(a) OVER (...)`, which is not SQL, so the text a
		// DISTINCT lowering recorded for such a key does not even round-trip.
		return fmt.Errorf("%w: the key %q is materialized on stage %s from %q, which is not a"+
			" parseable expression — the value it names was computed below and published under"+
			" a slot this text does not spell",
			ErrGroupKeyDistributed, name, stageID, r.Expr)
	}
	if agg := plansql.FindNestedAggregate(node); agg != nil {
		return fmt.Errorf("%w: the key %q is materialized on stage %s from %q, and %s is an"+
			" AGGREGATE — a pre-aggregate projection evaluates one row at a time and the value"+
			" that call names was computed below it, under a slot this expression does not spell",
			ErrGroupKeyDistributed, name, stageID, r.Expr, agg.String())
	}
	if w := plansql.FindAllWindowFuncs(node); len(w) > 0 {
		return fmt.Errorf("%w: the key %q is materialized on stage %s from %q, and %s is a"+
			" WINDOW function — a pre-aggregate projection cannot evaluate one, and the value"+
			" it names was computed below under a slot this expression does not spell",
			ErrGroupKeyDistributed, name, stageID, r.Expr, w[0].String())
	}
	return nil
}

// resolveDerivedAliasKey applies the rules to one key.
func resolveDerivedAliasKey(r GroupKeyResolution, in []streamCol, arms map[string]bool,
	stageID string) (string, bool, error) {
	bare := strings.ToLower(stripQualifier(r.Alias))
	aliasQual := strings.ToLower(qualifierOf(r.Alias))
	arm, constrained := keyArmConstraint(r.Alias, arms)
	fromKeysArm := func(c streamCol) bool {
		return !constrained || strings.EqualFold(c.Arm, arm)
	}
	var exact *streamCol
	var bareHits, qualHits []streamCol
	droppedBare := false
	for i := range in {
		c := in[i]
		if strings.EqualFold(c.Name, r.Alias) && !c.Dropped && fromKeysArm(c) {
			exact = &in[i]
		}
		if strings.ToLower(stripQualifier(c.Name)) != bare {
			continue
		}
		if c.Dropped {
			if fromKeysArm(c) {
				droppedBare = true
			}
			continue
		}
		if !fromKeysArm(c) {
			continue
		}
		if strings.IndexByte(c.Name, '.') < 0 {
			bareHits = append(bareHits, c)
			continue
		}
		qualHits = append(qualHits, c)
	}
	// (1) The stream spells the alias exactly — a join qualified this arm's
	// duplicate column, which is the ONLY way `y.w` reaches a stream.
	if exact != nil && exact.Materialized {
		return exact.Name, false, nil
	}
	// (2) One computed BARE column of that name FROM THE KEY'S ARM, and no
	// copy of it dropped.
	if len(bareHits) == 1 && bareHits[0].Materialized && !droppedBare {
		return bareHits[0].Name, false, nil
	}
	// (3) No bare one, one QUALIFIED one, and it is not ANOTHER arm's: the
	// join qualified this column because some other arm shared its bare name,
	// and the key either names that arm or was written bare. Binding a
	// qualified key to a different arm's qualified column is the one thing
	// this must never do, which is why the qualifiers are compared rather
	// than stripped.
	if len(bareHits) == 0 && len(qualHits) == 1 && qualHits[0].Materialized && !droppedBare {
		if aliasQual == "" || aliasQual == strings.ToLower(qualifierOf(qualHits[0].Name)) {
			return qualHits[0].Name, false, nil
		}
	}
	// (4) The definition, over the KEY'S ARM's columns, re-spelled into the
	// names the stream gives them. Handing the fragment the definition's own
	// text is not enough: `a * 3` resolves by an ordinary lookup, and where
	// both arms carry an `a` the PROBE's copy wins whichever arm the key meant.
	if r.Def != "" {
		if respelled, ok := respellDefOverArm(r.Def, in, arm, constrained); ok {
			return respelled, true, nil
		}
	}
	// (5) Neither. Say so, and say WHICH ARM, so the reason is in the error
	// and not in a comment. A derived arm whose ORDER BY / LIMIT stopped
	// attachScanSelectProjections from materializing the alias, read through
	// a join whose payload carries neither the alias nor the expression's
	// columns FROM THAT ARM, is the shape that reaches here.
	carried := make([]string, 0, len(in))
	for _, c := range in {
		if c.Dropped || !fromKeysArm(c) {
			continue
		}
		carried = append(carried, c.Name)
	}
	where := "the probe arm"
	if arm != "" {
		where = fmt.Sprintf("arm %q", arm)
	}
	if !constrained {
		where = "this stage"
	}
	return "", false, fmt.Errorf("%w: the key %q names a derived table's computed alias, and %s of"+
		" stage %s carries neither that name nor the columns its definition (%s) reads — it"+
		" carries %v",
		ErrGroupKeyDistributed, r.Alias, where, stageID, r.Def, carried)
}

// keyArmConstraint decides which JOIN ARM a key's alias names.
//
// A qualifier the join declares as a build alias names that build arm;
// anything else names the PROBE side, whose columns the model marks with an
// empty arm. A key written BARE constrains nothing — SQL already resolved its
// ambiguity, and there is exactly one column of that name to find.
func keyArmConstraint(alias string, arms map[string]bool) (arm string, constrained bool) {
	q := qualifierOf(alias)
	if q == "" {
		return "", false
	}
	if arms[strings.ToLower(q)] {
		return q, true
	}
	return "", true
}

// respellDefOverArm rewrites a definition's column references into the exact
// spellings the KEY'S ARM's columns have in the stream, and reports whether
// every reference resolved there.
//
// It is the group-key twin of `respellSpecsOverProducerOutput` (ADR-0025): a
// name is re-spelled to what the producing fragment really calls it, and the
// arm is what makes the choice unambiguous when two arms carry the name.
func respellDefOverArm(def string, in []streamCol, arm string, constrained bool) (string, bool) {
	node, err := plansql.ParseExpression(def)
	if err != nil {
		return "", false
	}
	pick := func(ref *plansql.ColRef) (string, bool) {
		want := strings.ToLower(stripQualifier(ref.Column))
		if ref.Table != "" {
			want = strings.ToLower(ref.Column)
		}
		match, hits := "", 0
		for _, c := range in {
			if c.Dropped {
				continue
			}
			if constrained && !strings.EqualFold(c.Arm, arm) {
				continue
			}
			if strings.ToLower(stripQualifier(c.Name)) != want {
				continue
			}
			match, hits = c.Name, hits+1
		}
		return match, hits == 1
	}
	ok := true
	out := plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		ref, isRef := n.(*plansql.ColRef)
		if !isRef || !ok {
			return nil, false
		}
		name, found := pick(ref)
		if !found {
			ok = false
			return nil, false
		}
		if strings.EqualFold(name, qualifiedColumn(ref)) {
			return nil, false // already spelled the way the stream carries it
		}
		if dot := strings.IndexByte(name, '.'); dot > 0 {
			return &plansql.ColRef{Table: name[:dot], Column: name[dot+1:]}, true
		}
		return &plansql.ColRef{Column: name}, true
	})
	if !ok {
		return "", false
	}
	return out.String(), true
}

// qualifierOf returns the table qualifier of a possibly-qualified name, or ""
// when it carries none.
func qualifierOf(name string) string {
	if dot := strings.IndexByte(name, '.'); dot > 0 {
		return name[:dot]
	}
	return ""
}

// defResolvesOverStream reports whether every column reference in a definition
// names a column the stream carries, with the runtime lookup's own tolerance
// for the qualified↔bare asymmetry.
func defResolvesOverStream(def string, in []streamCol) bool {
	node, err := plansql.ParseExpression(def)
	if err != nil {
		return false
	}
	emitted := make(map[string]string, len(in))
	for _, c := range in {
		if !c.Dropped {
			emitted[strings.ToLower(c.Name)] = c.Name
		}
	}
	if len(emitted) == 0 {
		return false
	}
	for _, ref := range collectColRefs(node) {
		if !columnResolves(ref, emitted) {
			return false
		}
	}
	return true
}
