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
//  1. the stream spells the alias EXACTLY (`y.w`), because the join qualified
//     this arm's duplicate column — resolve by that name;
//  2. the stream carries exactly ONE BARE column of the alias's bare name,
//     some fragment COMPUTED it, and no arm's column of that name was
//     dropped — resolve by the bare name. That is the probe arm's, or the
//     only arm that publishes it;
//  3. no bare one, and exactly ONE QUALIFIED column of that bare name, whose
//     qualifier is the alias's own or the key was written bare — resolve by
//     the qualified name. This is the runtime's own qualified↔bare fallback,
//     decided where the key is decided rather than left to a lookup;
//  4. the stream carries every column the DEFINITION reads — resolve by the
//     definition, which the fragment materializes into a hidden slot;
//  5. none of the above: the plan carries the value nowhere. REFUSED, and the
//     coordinator answers the query on its local pipeline, where the derived
//     table's Project is a real operator and the alias is a real column.
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
		resolved := false
		for k := range s.GroupByResolve {
			r := &s.GroupByResolve[k]
			if !r.deferred() {
				continue
			}
			if !resolved {
				in, resolved = aggregateInputStreamColumns(stages, idx, s), true
			}
			expr, computed, err := resolveDerivedAliasKey(*r, in, s.ID)
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
func resolveDerivedAliasKey(r GroupKeyResolution, in []streamCol, stageID string) (string, bool, error) {
	bare := strings.ToLower(stripQualifier(r.Alias))
	aliasQual := strings.ToLower(qualifierOf(r.Alias))
	var exact *streamCol
	var bareHits, qualHits []streamCol
	droppedBare := false
	for i := range in {
		c := in[i]
		if strings.EqualFold(c.Name, r.Alias) && !c.Dropped {
			exact = &in[i]
		}
		if strings.ToLower(stripQualifier(c.Name)) != bare {
			continue
		}
		if c.Dropped {
			droppedBare = true
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
	// (2) One computed BARE column of that name, and no arm's copy dropped.
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
	// (4) The definition, over columns the stream carries.
	if r.Def != "" && defResolvesOverStream(r.Def, in) {
		return r.Def, true, nil
	}
	// (5) Neither. Say so, and say which, so the reason is in the error and
	// not in a comment. A derived arm whose ORDER BY / LIMIT stopped
	// attachScanSelectProjections from materializing the alias, read through
	// a join that ships neither the alias nor the expression's columns, is
	// the shape that reaches here.
	carried := make([]string, 0, len(in))
	for _, c := range in {
		if !c.Dropped {
			carried = append(carried, c.Name)
		}
	}
	return "", false, fmt.Errorf("%w: the key %q names a derived table's computed alias, and stage %s"+
		" emits neither that name nor the columns its definition (%s) reads — it carries %v",
		ErrGroupKeyDistributed, r.Alias, stageID, r.Def, carried)
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
