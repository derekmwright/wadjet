// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A GROUP BY key's PUBLISHED name (Stage.GroupByCols/plansql.GroupKeyName)
// is what the aggregate emits and consumers read. Its RESOLUTION spelling
// names a bare input column (including an already-published inner key), an
// expression materialized as __gb_expr_N, or a join column with a different
// qualifier. Do not recover resolution by parsing publication: differing
// names must not collapse rows into one NULL group (#736; ADR-0026 §2, §4a).
// See docs/internals/group-key-publication-and-resolution.md for the design.

// GroupKeyResolution is one GROUP BY key's resolution spelling: what the
// fragment computing the key looks up in its input.
//
// Alias and Def are the planner's own deferred decision and never reach the
// wire. A key that names a derived table's COMPUTED alias has two candidate
// spellings — the alias, and the expression that defines it — and which one
// the producing fragment emits is decided by `attachScanSelectProjections` and
// `absorbWindowArmProjection`, which run AFTER `walkStages`. Emission records
// both candidates; `resolveStageGroupKeys` settles it at the end of planning
// against the producer's real output, exactly as `resolveFilterAliasSpelling`
// settles a predicate's spelling and `resolveDerivedAliasSortKeys` a sort
// key's (ADR-0025).
type GroupKeyResolution struct {
	// Expr is the spelling the computing fragment resolves the key by: a
	// column of its input when Computed is false, an expression over columns
	// of its input when Computed is true.
	Expr string
	// Computed marks Expr as an EXPRESSION the fragment must evaluate into a
	// hidden slot, rather than a name it can look up. The planner decides it;
	// nothing downstream re-derives it by parsing the text, because the text
	// cannot say (`GROUP BY "g + 1"` names a column and `GROUP BY g + 1` is
	// arithmetic, and both are recorded as `g + 1` — ADR-0026 §2c).
	Computed bool
	// Alias is the key as the query wrote it when it names a derived table's
	// COMPUTED alias, and "" for every other key. Planner-only.
	Alias string
	// Def is that alias's defining expression, re-spelled into the columns
	// the derived table's own input carries. Planner-only.
	Def string
	// Decl is that definition's declared type, resolved in the scope the
	// definition is SPELLED IN — the derived table's own input — rather than
	// in the aggregate's, which cannot name its columns. Planner-only, and
	// read by stageGroupKeyDecls in place of the walk that scope defeats
	// (ADR-0026 §5).
	Decl expr.DeclType
}

// deferred reports whether this key's resolution is still a choice between two
// candidate spellings that only the finished stage graph can settle.
func (r GroupKeyResolution) Deferred() bool { return r.Alias != "" }

// StageGroupKeyNames computes both names of every GROUP BY key of one logical
// Aggregate: what the stage PUBLISHES it as, and what the computing fragment
// RESOLVES it by.
//
// The published list is `groupKeyOutputs`' Name — `plansql.GroupKeyName` —
// with one deliberate exception: a LITERAL key. The single-process path elides
// a literal from the key set and re-attaches it as a constant under a
// synthetic name; the stage DAG does not elide, so its literal key is a real
// key published under the text the query wrote. Publishing it under the
// single path's slot name would name a column the DAG's own consumers do not
// ask for.
func StageGroupKeyNames(agg, child *logical.Node) (published []string, resolve []GroupKeyResolution) {
	keys := groupKeyOutputs(agg)
	published = make([]string, len(agg.GroupBy))
	resolve = make([]GroupKeyResolution, len(agg.GroupBy))
	// execRule[i] marks a key whose two names are the SAME string, so the
	// fragment gets no GroupByOutNames for it and `exec.PublishedGroupKeyNames`
	// own rule decides what the aggregate emits. See the loop's tail.
	execRule := make([]bool, len(agg.GroupBy))
	for i, gb := range agg.GroupBy {
		k := groupKeyOut{Name: plansql.NormalizeIdentRef(strings.TrimSpace(gb))}
		if i < len(keys) {
			k = keys[i]
		}
		switch {
		case k.Literal:
			// Not elided here, so the key IS its own text on both sides and
			// the fragment materializes it exactly as it does today.
			published[i] = gb
			resolve[i] = GroupKeyResolution{Expr: gb, Computed: true}
		case k.PublishedBelow:
			// The aggregate DIRECTLY BELOW already computed this key and
			// publishes it under this same name, so the value is a COLUMN of
			// this aggregate's input. Re-deriving it as arithmetic reads
			// leaves that aggregate no longer emits, which collapsed the
			// whole table into ONE NULL group — #736's first refusal, and it
			// is a refusal only because a stage had one field for both names.
			published[i] = k.Name
			resolve[i] = GroupKeyResolution{Expr: k.Name}
		case k.Derived:
			// A computed key. Its leaves may be bound by a rename Project the
			// DAG flattens, so the spelling the worker evaluates is the one
			// re-spelled into source columns; the PUBLISHED name stays what
			// the query wrote (ADR-0026 §2c: the re-spelling is for DISPATCH
			// only).
			published[i] = k.Name
			// From the CANONICAL text and not the recorded one, so
			// `GROUP BY (g + 1)` resolves and publishes ONE string: the
			// redundant outer parentheses are spelling, and a resolution that
			// differed from the published name only by them would make every
			// reader that compares the two say "these are two names".
			expr := k.Name
			if respelled, ok := AggStageDerivedKey(k.Name, child); ok {
				expr = respelled
			}
			resolve[i] = GroupKeyResolution{Expr: expr, Computed: true}
		default:
			// A bare column reference. The input carries it under its own
			// name unless a Project below renames it — and the DAG emits no
			// stage for a Project, so the fragment sees the source column.
			//
			// RESOLVED by k.Slot and PUBLISHED as k.Name. The two are the
			// same string for every key the query wrote — Slot IS Name there
			// — and differ only for a key the planner MINTED, which resolves
			// by the input column and publishes under a hidden slot (#956).
			// Reading the published name for both is what would send the slot
			// to a fragment that has no column of that name.
			published[i] = k.Name
			resolve[i] = GroupKeyResolution{Expr: k.Slot}
			resolved, def, defScope, renamed := ResolveAggInputName(gb, child)
			if !renamed {
				execRule[i] = agg.LateralAggregate && !k.Minted && !k.Delimited
				break
			}
			if def == nil {
				// A plain rename: the fragment reads the source column.
				resolve[i] = GroupKeyResolution{Expr: resolved}
				break
			}
			// The alias names an EXPRESSION, and there are two candidate
			// spellings — the alias, and the definition. Which one the
			// producing fragment emits is decided after the projection
			// passes; record both and settle it in resolveStageGroupKeys.
			resolve[i] = GroupKeyResolution{
				Expr:     def.String(),
				Computed: true,
				Alias:    k.Name,
				Def:      def.String(),
				// TYPED where the expression was re-spelled TO (ADR-0026 §5),
				// which for a derived table's alias is the node its Project
				// reads — the only scope that can NAME the definition's
				// columns. Typing it against the aggregate's own child leaves
				// a DECIMAL key on the FLOAT rule, and the exact value then
				// meets the #361 store guard on both DAG arms.
				Decl: DerivedGroupKeyDecl(def.String(), def, defScope),
			}
		}
	}
	// For marked keys of a DECORRELATED LATERAL aggregate only, resolution equal
	// to publication means no GroupByOutNames: mirror exec.PublishedGroupKeyNames,
	// stripping qualifiers unless keys collide. Consumers must see the actual
	// published schema, including empty partitions (ADR-0010, #767).
	// Do not apply globally: ordinary output aliases can bind the wrong key (#947).
	// Keys with differing names (derived aliases, literals, delimited or minted
	// keys) already carry planner-chosen GroupByOutNames and bypass exec's strip
	// (#467, #480, #740; ADR-0026 §2).
	if anyExecRule(execRule) {
		emitted := exec.PublishedGroupKeyNames(published, nil, LogicalAggOutNames(agg), false)
		for i := range published {
			if execRule[i] {
				published[i] = emitted[i]
			}
		}
	}
	return published, resolve
}

// anyExecRule reports whether any key is left to exec's own naming rule.
func anyExecRule(flags []bool) bool {
	for _, f := range flags {
		if f {
			return true
		}
	}
	return false
}

// StageEmittedKeyNames is the column name the aggregate's FRAGMENT emits for
// each key — the published list run through `exec.PublishedGroupKeyNames`,
// which is the same rule and the same call the worker makes and the
// single-process operator applies to its own key list.
//
// The pair fed to it mirrors what both engines build: a materialized key
// resolves by a hidden slot and is NAMED by the planner; every other key
// resolves by its published name and takes the rule's qualifier strip. The
// slot placeholder here is not the slot the worker allocates — that index is a
// runtime fact — but the rule only reads a name's qualifier and its collisions,
// and a reserved-family name has neither.
func StageEmittedKeyNames(published []string, resolve []GroupKeyResolution, aggOut []string) []string {
	byRule := make([]string, len(published))
	overrides := make([]string, len(published))
	for i := range published {
		if i < len(resolve) && resolve[i].Computed {
			byRule[i] = plansql.SlotName(plansql.SlotGroupKey, i)
			overrides[i] = published[i]
			continue
		}
		byRule[i] = published[i]
	}
	return exec.PublishedGroupKeyNames(byRule, overrides, aggOut, false)
}

// LogicalAggOutNames is an Aggregate node's OUTPUT column names, the list
// `exec.PublishedGroupKeyNames` asks about (ADR-0026 §2b).
func LogicalAggOutNames(agg *logical.Node) []string {
	if agg == nil || len(agg.AggExprs) == 0 {
		return nil
	}
	out := make([]string, 0, len(agg.AggExprs))
	for i := range agg.AggExprs {
		out = append(out, agg.AggExprs[i].OutputCol)
	}
	return out
}
