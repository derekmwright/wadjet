// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setOpKindCase is one arm kind of the mark table (ADR-0024 §10): the column
// the arm declares and the select item it is spelled with.
type setOpKindCase struct {
	name string
	col  parquet.Column
	item string // the select item, parsed for its facts
}

func setOpKindCases() []setOpKindCase {
	dec := func(p, s int, marked bool) parquet.Column {
		return parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: p, Scale: s, Unconstrained: marked}
	}
	str := parquet.Column{Name: "v", Type: parquet.TypeString}
	return []setOpKindCase{
		{"marked", dec(38, 10, true), "v"},
		{"fixed", dec(10, 2, false), "v"},
		{"fixed0", dec(10, 0, false), "v"},
		{"intcol", parquet.Column{Name: "v", Type: parquet.TypeInt32}, "v"},
		{"intlit", parquet.Column{Name: "v", Type: parquet.TypeInt64}, "2"},
		{"null", str, "NULL"},
		{"quoted", str, "'1.5'"},
		{"quoted0", str, "'1.50'"},
		{"lit", dec(2, 1, false), "2.5"},
		{"lit0", dec(3, 2, false), "2.50"},
		{"typednull", parquet.Column{Name: "v", Type: parquet.TypeFloat64}, "CAST(NULL AS NUMERIC)"},
		{"fixednull", dec(10, 2, false), "CAST(NULL AS NUMERIC(10,2))"},
		{"expr", dec(38, 10, false), "v + 0"},
		{"castfixed", dec(10, 2, false), "CAST(v AS NUMERIC(10,2))"},
		{"float", parquet.Column{Name: "v", Type: parquet.TypeFloat64}, "v"},
	}
}

func setOpKindFacts(t *testing.T, item string) SetOpArmFacts {
	t.Helper()
	e, err := plansql.ParseExpressionComplete(item)
	if err != nil {
		t.Fatalf("parse %q: %v", item, err)
	}
	return SetOpArmFacts{setOpItemFact(e)}
}

// setOpFlatColumn is the one rule over every arm at once, through the stage
// planner's call site (setOpTargetType).
func setOpFlatColumn(cols []parquet.Column, facts []SetOpArmFacts) (SetOpColType, bool) {
	plans := make([]SetOpArmPlan, len(cols))
	for i, c := range cols {
		plans[i] = SetOpArmPlan{Types: []SetOpColType{setOpColTypeOfColumn(c)}, facts: facts[i]}
	}
	want, allKnown, err := setOpTargetType(plans, 0, "v", "UNION")
	return want, err == nil && allKnown && want.Known
}

// setOpStageColumn is the stage planner over a left-deep chain of binary
// operations, as the logical plan nests them: each step's left arm is the
// previous node's result type (setOpNodeResultTypes) with no select list of
// its own, so no facts. ok=false where the stage planner leaves the column
// untyped or refuses it.
func setOpStageColumn(cols []parquet.Column, facts []SetOpArmFacts) (SetOpColType, bool) {
	left := setOpColTypeOfColumn(cols[0])
	lf := facts[0]
	for i := 1; i < len(cols); i++ {
		plans := []SetOpArmPlan{{Types: []SetOpColType{left}, facts: lf},
			{Types: []SetOpColType{setOpColTypeOfColumn(cols[i])}, facts: facts[i]}}
		want, allKnown, err := setOpTargetType(plans, 0, "v", "UNION")
		if err != nil || !allKnown || !want.Known {
			return SetOpColType{}, false
		}
		left, lf = want, nil
	}
	return left, true
}

// setOpSingleColumn is the single-process call site over the same chain: each
// step's left arm is the previous step's runtime column, whose facts are its
// node's result type (setOpAdapterArmFacts), and an untyped arm is first given
// the other arm's type (setOpResolveUnknownLiteralArms).
func setOpSingleColumn(cols []parquet.Column, facts []SetOpArmFacts) parquet.Column {
	left, lf := []parquet.Column{cols[0]}, facts[0]
	for i := 1; i < len(cols); i++ {
		right, rf := []parquet.Column{cols[i]}, facts[i]
		l, r := setOpResolveUnknownLiteralArms(left, right, lf, rf)
		left = unifySetOpSchemas(l, r, lf, rf)
		inner, _ := setOpStageColumn(cols[:i+1], facts[:i+1])
		lf = SetOpArmFacts{{role: inner.fold}}
	}
	return left[0]
}

// TestSetOpResultColumnOneAnswerOnBothCallSites enumerates every arm-kind pair
// and triple and asserts the stage planner's call site and the single-process
// path's give one column: the same type, (precision, scale) and mark. The
// stage answer is also asserted independent of arm ORDER (the rule is a fold).
func TestSetOpResultColumnOneAnswerOnBothCallSites(t *testing.T) {
	kinds := setOpKindCases()
	checked := 0
	var tuples [][]int
	for a := range kinds {
		for b := range kinds {
			tuples = append(tuples, []int{a, b})
			for c := range kinds {
				tuples = append(tuples, []int{a, b, c})
			}
		}
	}
	for _, tup := range tuples {
		cols := make([]parquet.Column, len(tup))
		facts := make([]SetOpArmFacts, len(tup))
		name := ""
		for i, k := range tup {
			cols[i] = kinds[k].col
			facts[i] = setOpKindFacts(t, kinds[k].item)
			name += kinds[k].name + "∪"
		}
		stage, ok := setOpStageColumn(cols, facts)
		single := setOpSingleColumn(cols, facts)
		if !ok {
			continue // a pair the ladder refuses (setOpArmTypeConflict, both paths)
		}
		if stage.Typ == parquet.TypeDecimal && !stage.DecKnown {
			continue // the stage planner refuses an unresolved scale
		}
		got := fmt.Sprintf("%s(%d,%d) mark=%v", single.Type, single.Precision, single.Scale, single.Unconstrained)
		want := fmt.Sprintf("%s(%d,%d) mark=%v", stage.Typ, stage.Dec.Precision, stage.Dec.Scale, stage.Dec.Unconstrained)
		if stage.Typ != parquet.TypeDecimal {
			got = fmt.Sprintf("%s mark=%v", single.Type, single.Unconstrained)
			want = fmt.Sprintf("%s mark=false", stage.Typ)
		}
		if got != want {
			t.Errorf("%s: stage planner %s, single-process path %s", name, want, got)
		}
		checked++
		// The nested chain marks what the flat fold marks, and every
		// rotation of the arms resolves the same mark.
		// The nested chain marks what the flat fold marks — unless an inner
		// node resolved to a type other than DECIMAL, which PostgreSQL (that
		// also resolves the inner node first) types the same way and which
		// leaves its arms nothing to say about a mark — and every rotation
		// of the arms resolves the flat fold's mark.
		flat, flatOK := setOpFlatColumn(cols, facts)
		innerDecimal := true
		for k := 2; k < len(cols); k++ {
			inner, ok := setOpStageColumn(cols[:k], facts[:k])
			innerDecimal = innerDecimal && ok && inner.Typ == parquet.TypeDecimal
		}
		if flatOK && innerDecimal && flat.Dec.Unconstrained != stage.Dec.Unconstrained {
			t.Errorf("%s: nested marks %v, the flat fold %v", name, stage.Dec.Unconstrained, flat.Dec.Unconstrained)
		}
		for r := 1; flatOK && r < len(tup); r++ {
			rc := append(append([]parquet.Column(nil), cols[r:]...), cols[:r]...)
			rf := append(append([]SetOpArmFacts(nil), facts[r:]...), facts[:r]...)
			rot, ok := setOpFlatColumn(rc, rf)
			if ok && rot.Dec.Unconstrained != flat.Dec.Unconstrained {
				t.Errorf("%s: rotation %d marks %v, the original order %v", name, r,
					rot.Dec.Unconstrained, flat.Dec.Unconstrained)
			}
		}
	}
	if checked < 2000 {
		t.Fatalf("only %d tuples compared: the enumeration lost its coverage", checked)
	}
}

// TestSetOpMarkTable is ADR-0024 §10's table: an unconstrained NUMERIC column
// beside each arm kind, in either position. MARKED where trimming prints that
// arm's values as PostgreSQL 17.11 does; unmarked where the arm holds
// fraction digits of its own (a NUMERIC(p,s) column or CAST, a literal
// spelled with trailing zeros).
func TestSetOpMarkTable(t *testing.T) {
	want := map[string]bool{
		"marked": true, "fixed": false, "fixed0": true, "intcol": true, "intlit": true,
		"null": true, "quoted": true, "quoted0": false, "lit": true, "lit0": false,
		"typednull": true, "fixednull": true, "expr": true, "castfixed": false,
	}
	kinds := setOpKindCases()
	marked := kinds[0]
	for _, k := range kinds {
		w, ok := want[k.name]
		if !ok {
			continue
		}
		for _, order := range [][2]setOpKindCase{{marked, k}, {k, marked}} {
			cols := []parquet.Column{order[0].col, order[1].col}
			facts := []SetOpArmFacts{setOpKindFacts(t, order[0].item), setOpKindFacts(t, order[1].item)}
			got, ok := setOpFlatColumn(cols, facts)
			if !ok || got.Typ != parquet.TypeDecimal {
				t.Errorf("%s ∪ %s: no DECIMAL result (%+v)", order[0].name, order[1].name, got)
				continue
			}
			if got.Dec.Unconstrained != w {
				t.Errorf("%s ∪ %s: marked=%v, the table says %v", order[0].name, order[1].name, got.Dec.Unconstrained, w)
			}
		}
	}
	// Without a marked arm the result is unmarked: n*1 ∪ n*2 prints 2.50 as
	// PostgreSQL does.
	cols := []parquet.Column{kinds[12].col, kinds[12].col}
	facts := []SetOpArmFacts{setOpKindFacts(t, "v * 1"), setOpKindFacts(t, "v * 2")}
	if got, _ := setOpFlatColumn(cols, facts); got.Dec.Unconstrained || got.fold != setOpMarkNeutral {
		t.Errorf("two computed arms: marked=%v fold=%d, want unmarked and neutral", got.Dec.Unconstrained, got.fold)
	}
}
