package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A GROUP BY key travels the stage DAG under TWO names.
//
// The PUBLISHED name is what the aggregate emits the value as, and what every
// consumer above it reads: `Stage.GroupByCols`, `plansql.GroupKeyName`, the
// same text the single-process planner hands `exec.HashAggregate`.
//
// The RESOLUTION spelling is what the fragment that COMPUTES the key resolves
// it by, against the columns its own input carries. It is one of three things,
// and the third is why a second field is needed at all:
//
//   - a bare column of that input — every ordinary `GROUP BY c`, and every
//     key an aggregate DIRECTLY BELOW already published (`SELECT DISTINCT
//     g + 1 … GROUP BY g + 1` lowers to two aggregates keyed alike, and the
//     outer one reads a column, not arithmetic);
//   - an expression over columns that input carries, which the fragment
//     materializes into a hidden `__gb_expr_N` slot;
//   - a column a JOIN's stream spells differently from the query — `w` where
//     the query wrote `x.w`, or `y.w` where the join qualified a duplicate.
//
// `Stage.GroupByCols` was one field doing both jobs, and the worker recovered
// the second by PARSING the first (`worker.derivedGroupKeys`). Every unfixed
// member of #736's family was that: a key whose two names differ answers one
// NULL group over the whole table, silently, on both DAG arms, where the
// single-process path answers PostgreSQL's rows (ADR-0026 §2, §4a).

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
func (r GroupKeyResolution) deferred() bool { return r.Alias != "" }

// stageGroupKeyList returns the group-key list a stage carries and the field
// it lives in. A stage carries exactly ONE of the three: an aggregate stage
// its own GroupByCols, a fused scan-aggregate its FusedAggGroupBy, and a join
// that absorbed a chain-terminal partial its ChainedAggGroupBy.
// GroupByResolve is index-aligned with whichever one answers.
func stageGroupKeyList(s *Stage) []string {
	switch {
	case len(s.GroupByCols) > 0:
		return s.GroupByCols
	case len(s.FusedAggGroupBy) > 0:
		return s.FusedAggGroupBy
	case len(s.ChainedAggGroupBy) > 0:
		return s.ChainedAggGroupBy
	}
	return nil
}

// stageComputesGroupKeys reports whether this stage's fragment RESOLVES the
// group keys against a raw input — the only class that reads GroupByResolve.
//
// A "final_aggregate" or "merge_aggregate" consumes a partial's OUTPUT, where
// every key is already a column under its published name, so it resolves by
// the published name and carries no resolution list. The exception is a
// RawInputAggregate final: the distribution pass hash-partitions raw rows into
// disjoint groups and the final aggregates them in ONE level, so that fragment
// computes the keys itself.
func stageComputesGroupKeys(s *Stage) bool {
	switch s.Type {
	case StageScan:
		return len(s.FusedAggGroupBy) > 0
	case StageAggregate:
		return true
	case StageFinalAggregate, StageMergeAggregate:
		return s.RawInputAggregate
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return len(s.ChainedAggGroupBy) > 0
	}
	return false
}

// stageGroupKeyNames computes both names of every GROUP BY key of one logical
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
func stageGroupKeyNames(agg, child *logical.Node) (published []string, resolve []GroupKeyResolution) {
	keys := groupKeyOutputs(agg)
	published = make([]string, len(agg.GroupBy))
	resolve = make([]GroupKeyResolution, len(agg.GroupBy))
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
			if respelled, ok := aggStageDerivedKey(k.Name, child); ok {
				expr = respelled
			}
			resolve[i] = GroupKeyResolution{Expr: expr, Computed: true}
		default:
			// A bare column reference. The input carries it under its own
			// name unless a Project below renames it — and the DAG emits no
			// stage for a Project, so the fragment sees the source column.
			published[i] = k.Name
			resolve[i] = GroupKeyResolution{Expr: k.Name}
			resolved, def, defScope, renamed := resolveAggInputName(gb, child)
			if !renamed {
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
				Decl: derivedGroupKeyDecl(def.String(), def, defScope),
			}
		}
	}
	return published, resolve
}

// resolveExprs is the resolution list as plain text, for the callers that need
// the spelling alone (the read-set prune, the type walk).
func resolveExprs(resolve []GroupKeyResolution) []string {
	out := make([]string, len(resolve))
	for i, r := range resolve {
		out[i] = r.Expr
	}
	return out
}

// identityGroupKeyResolutions is the resolution list for keys whose two names
// are one string — every key a stage resolves by exactly the name it publishes.
// Saying it explicitly is not redundant: an ABSENT list means "an older
// coordinator", and the worker then recovers the second name by parsing the
// first, which is the behaviour ADR-0026 §2 replaced.
func identityGroupKeyResolutions(names []string) []GroupKeyResolution {
	if len(names) == 0 {
		return nil
	}
	out := make([]GroupKeyResolution, len(names))
	for i, n := range names {
		out[i] = GroupKeyResolution{Expr: n}
	}
	return out
}

// stageEmittedKeyNames is the column name the aggregate's FRAGMENT emits for
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
func stageEmittedKeyNames(published []string, resolve []GroupKeyResolution) []string {
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
	return exec.PublishedGroupKeyNames(byRule, overrides, false)
}

// aggregateEmittedKeyNames is the column name a stage's aggregate emits for
// each of its GROUP BY keys — `exec.PublishedGroupKeyNames` over the published
// list, which is the same answer the single-process operator gives.
func aggregateEmittedKeyNames(s *Stage) []string {
	keys := stageGroupKeyList(s)
	if len(keys) == 0 {
		return nil
	}
	if len(s.GroupByResolve) != len(keys) {
		return keys
	}
	return stageEmittedKeyNames(keys, s.GroupByResolve)
}

// stageGroupKeyDecls types every key the computing fragment MATERIALIZES, so
// the worker builds its vector from the planner's declaration rather than
// inferring one from the expression text with no catalog (#379, ADR-0024
// item 2).
//
// Keyed by the PUBLISHED name. The resolution spelling is not a stable key: a
// derived-alias key's resolution is re-spelled at the end of planning, and a
// map keyed by it would then answer for a text nothing carries.
func stageGroupKeyDecls(published []string, resolve []GroupKeyResolution,
	child *logical.Node) (map[string]parquet.TypeID, map[string]logical.DecimalMeta) {
	var out map[string]parquet.TypeID
	var dec map[string]logical.DecimalMeta
	for i, r := range resolve {
		if !r.Computed || r.Expr == "" || i >= len(published) {
			continue
		}
		node, err := plansql.ParseExpression(r.Expr)
		if err != nil {
			continue
		}
		if out == nil {
			out = make(map[string]parquet.TypeID)
		}
		d := r.Decl
		if d.ID == 0 && !d.DecKnown {
			d = derivedGroupKeyDecl(r.Expr, node, child)
		}
		out[published[i]] = d.ID
		if d.ID == parquet.TypeDecimal && d.DecKnown {
			// The (p,s) beside the TypeID: the worker builds the key vector
			// from this declaration, and a DECIMAL one with no scale
			// truncates every value written into it (ADR-0024 item 2).
			if dec == nil {
				dec = make(map[string]logical.DecimalMeta)
			}
			dec[published[i]] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
		}
	}
	return out, dec
}
