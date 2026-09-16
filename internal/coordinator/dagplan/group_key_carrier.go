// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// StageGroupKeyList returns the group-key list a stage carries and the field
// it lives in. A stage carries exactly ONE of the three: an aggregate stage
// its own GroupByCols, a fused scan-aggregate its FusedAggGroupBy, and a join
// that absorbed a chain-terminal partial its ChainedAggGroupBy.
// GroupByResolve is index-aligned with whichever one answers.
func StageGroupKeyList(s *Stage) []string {
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

// StageComputesGroupKeys reports whether this stage's fragment RESOLVES the
// group keys against a raw input — the only class that reads GroupByResolve.
//
// A "final_aggregate" or "merge_aggregate" consumes a partial's OUTPUT, where
// every key is already a column under its published name, so it resolves by
// the published name and carries no resolution list. The exception is a
// RawInputAggregate final: the distribution pass hash-partitions raw rows into
// disjoint groups and the final aggregates them in ONE level, so that fragment
// computes the keys itself.
func StageComputesGroupKeys(s *Stage) bool {
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

// resolveExprs is the resolution list as plain text, for the callers that need
// the spelling alone (the read-set prune, the type walk).
func resolveExprs(resolve []physical.GroupKeyResolution) []string {
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
func identityGroupKeyResolutions(names []string) []physical.GroupKeyResolution {
	if len(names) == 0 {
		return nil
	}
	out := make([]physical.GroupKeyResolution, len(names))
	for i, n := range names {
		out[i] = physical.GroupKeyResolution{Expr: n}
	}
	return out
}

// stageAggOutNames is a Stage's aggregate OUTPUT names, from whichever of the
// three spec lists the stage carries — the same list one operator lower.
func stageAggOutNames(s *Stage) []string {
	if s == nil {
		return nil
	}
	var out []string
	for _, specs := range [][]AggSpec{s.AggSpecs, s.FusedAggSpecs, s.ChainedAggSpecs} {
		for _, a := range specs {
			out = append(out, a.OutputCol)
		}
	}
	return out
}

// aggregateEmittedKeyNames is the column name a stage's aggregate emits for
// each of its GROUP BY keys — `exec.PublishedGroupKeyNames` over the published
// list, which is the same answer the single-process operator gives.
func aggregateEmittedKeyNames(s *Stage) []string {
	keys := StageGroupKeyList(s)
	if len(keys) == 0 {
		return nil
	}
	if len(s.GroupByResolve) != len(keys) {
		return keys
	}
	return physical.EmittedKeyNames(keys, s.GroupByResolve, stageAggOutNames(s))
}

// stageGroupKeyDecls types every key the computing fragment MATERIALIZES, so
// the worker builds its vector from the planner's declaration rather than
// inferring one from the expression text with no catalog (#379, ADR-0024
// item 2).
//
// Keyed by the PUBLISHED name. The resolution spelling is not a stable key: a
// derived-alias key's resolution is re-spelled at the end of planning, and a
// map keyed by it would then answer for a text nothing carries.
func stageGroupKeyDecls(published []string, resolve []physical.GroupKeyResolution,
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
			d = physical.DerivedGroupKeyDecl(r.Expr, node, child)
		}
		resolve[i].Decl = d
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
