package physical

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// resolveStageGroupKeys runs after projection passes, using the actual stream
// to settle alias vs definition (ADR-0026 §4a). Constrain EVERY candidate to
// the key's arm (keyArmConstraint; build alias or empty probe arm, #781).
// Try in order: exact alias; exactly one materialized bare alias from that arm
// with no same-name arm column dropped; if no bare match, one qualified match
// from that arm with the alias's qualifier or a key originally written bare;
// then a definition whose every reference that arm carries, re-spelled to its
// stream names and materialized in a hidden slot. Otherwise REFUSE and route
// local. A same-named base column is not proof an alias was materialized.
// See docs/internals/stage-group-key-resolution.md for the design.
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

// refuseUnevaluableGroupKey refuses aggregate/window calls in a key's
// RESOLUTION: pre-aggregate projections evaluate scalar rows and can read
// only the published __agg_N/__win_N slot, not re-evaluate its call.
// Matching publication/resolution text does not make a call evaluable.
// Refusal routes to local operators. DISTINCT lowering must record the
// operator's published slot to remove this residual; the carrier cannot.
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
