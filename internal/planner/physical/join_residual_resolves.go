// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// JoinResidualUnresolved names every reference of an outer join's ON residual
// that resolves against NEITHER declared side, using residualEval.resolve's own
// order. It is the plan-time form of a check the evaluator can only make at run
// time, where its disposition is silent: an unbound slot reads SQL NULL, the
// residual is UNKNOWN for every candidate pair, and a LEFT/FULL join answers
// its whole preserved side NULL-padded rather than refusing.
//
// A caller holding both sides' DECLARED schemas — a distributed fragment does —
// refuses on a non-empty result. An empty schema means the caller has no
// declaration to check against, and the answer is then "nothing unresolved".
//
// It is the FLOOR under dagplan.residualWithStageSpellings, which re-spells a
// residual's leaves into what the stage publishes: a leaf that rewrite cannot
// reach refuses here rather than padding the preserved side in silence. The
// check, its resolution order and the measurement that a build-side-only
// residual still answers are round 1's reviewer's work, applied as measured.
func JoinResidualUnresolved(filter, buildAlias string, probe, build []parquet.Column) []string {
	if len(probe) == 0 || len(build) == 0 {
		return nil
	}
	node := parseJoinCondExpr(filter)
	if node == nil {
		return nil
	}
	refs, err := plansql.ColumnRefs(node)
	if err != nil {
		return nil
	}
	alias := strings.ToLower(buildAlias)
	var bad []string
	seen := map[string]bool{}
	for _, r := range refs {
		spelled := r.Column
		if r.Table != "" {
			spelled = r.Table + "." + r.Column
		}
		if seen[strings.ToLower(spelled)] {
			continue
		}
		seen[strings.ToLower(spelled)] = true
		if residualRefResolves(r, alias, probe, build) {
			continue
		}
		bad = append(bad, spelled)
	}
	return bad
}

func residualRefResolves(r *plansql.ColRef, buildAlias string, probe, build []parquet.Column) bool {
	col := strings.ToLower(r.Column)
	if r.Table != "" {
		qual := strings.ToLower(r.Table) + "." + col
		switch {
		case schemaRowField(probe, strings.ToLower(r.Table), col),
			schemaRowField(build, strings.ToLower(r.Table), col),
			batch.ResolveSchemaIndex(probe, qual) >= 0,
			batch.ResolveSchemaIndex(build, qual) >= 0:
			return true
		case strings.ToLower(r.Table) == buildAlias:
			return batch.ResolveSchemaIndex(build, col) >= 0
		}
	}
	return batch.ResolveSchemaIndex(probe, col) >= 0 ||
		batch.ResolveSchemaIndex(build, col) >= 0
}

// schemaRowField reports whether container.field names a ROW column's child.
func schemaRowField(schema []parquet.Column, container, field string) bool {
	i := batch.ResolveSchemaIndex(schema, container)
	if i < 0 || schema[i].Type != parquet.TypeRow {
		return false
	}
	for _, f := range schema[i].Fields {
		if strings.EqualFold(f.Name, field) {
			return true
		}
	}
	return false
}

// RefuseUnresolvedJoinResidual is JoinResidualUnresolved's error form.
func RefuseUnresolvedJoinResidual(bad []string) error {
	return fmt.Errorf("its reference%s %s resolve%s on neither side of this join here; "+
		"an unresolved reference would make the residual UNKNOWN for every candidate and "+
		"pad the preserved side in silence",
		plural(len(bad)), strings.Join(bad, ", "), verb(len(bad)))
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func verb(n int) string {
	if n == 1 {
		return "s"
	}
	return ""
}
