// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// respellDeclaredJoinSideSchemas makes a join's advisory side schemas spell
// their columns the way the producing stage really does, WHERE A LATE PASS
// CHANGED THAT SPELLING AFTER THE DECLARATION WAS WRITTEN.
//
// `physical.DeclaredJoinSchema` mirrors `joinOutputSchemaWithMapping`'s duplicate rule
// at emission: the first relation to publish a bare name keeps it and the next
// one is qualified by the alias that owns it. `markCoPathingSelfJoinBuilds`
// then runs over the FINISHED stage list and sets `QualifyAllBuildCols` on
// joins whose build scans co-path (the Q07 rule, ADR-0026 §7), which qualifies
// EVERY build column of those stages — including ones the declaration left
// bare because nothing contested them. A block whose body is such a join then
// declared `customer` where the stream writes `o2.customer`, and the task
// whose partition was EMPTY wrote that name beside its siblings':
// `names column 2 "customer" where an earlier file of the same stage input
// named it "o2.customer"` (ADR-0010, arc R2 round 2 B1).
//
// It fires ONLY for a side whose producing chain carries that flag, because
// that is the only spelling this layer knows changed. Elsewhere the
// declaration stands: the stream MODEL and the executor agree on every shape
// this arc measured except one, a FULL OUTER join over a CamelCase schema
// where the model qualifies a build column the executor emits bare
// (`camel_case_schema_invariance_test.go`, measured in round 2 and recorded as
// a filing candidate) — and respelling from the model there put the model's
// answer into a file the executor then disagreed with.
//
// Only the QUALIFIER moves, never the column's own spelling: the model carries
// whatever case the planner's lists hold and the executor keeps the catalog's.
func respellDeclaredJoinSideSchemas(stages []Stage) {
	idx := make(map[string]int, len(stages))
	for i := range stages {
		idx[stages[i].ID] = i
	}
	respell := func(decl []parquet.Column, dep string) {
		d, ok := idx[dep]
		if !ok || len(decl) == 0 || !chainQualifiesAllBuildCols(stages, idx, d, passThroughDepth) {
			return
		}
		in := stageStreamColumns(stages, idx, &stages[d], passThroughDepth)
		if len(in) == 0 {
			return
		}
		for j := range decl {
			if name, ok := streamSpellingFor(in, decl[j].Name); ok {
				decl[j].Name = name
			}
		}
	}
	for i := range stages {
		s := &stages[i]
		if !isJoinStage(s.Type) {
			continue
		}
		probeDep, buildDep := s.LeftDepStage, s.RightDepStage
		if probeDep == "" && len(s.Dependencies) > 0 {
			probeDep = s.Dependencies[0]
		}
		if buildDep == "" && len(s.Dependencies) > 1 {
			buildDep = s.Dependencies[1]
		}
		respell(s.JoinProbeSchema, probeDep)
		respell(s.JoinBuildSchema, buildDep)
	}
}

// chainQualifiesAllBuildCols reports whether the stage at i, or a stage it
// reads through, had `QualifyAllBuildCols` set on it.
func chainQualifiesAllBuildCols(stages []Stage, idx map[string]int, i, depth int) bool {
	if depth <= 0 || i < 0 || i >= len(stages) {
		return false
	}
	if stages[i].QualifyAllBuildCols {
		return true
	}
	for _, dep := range stages[i].Dependencies {
		if d, ok := idx[dep]; ok && chainQualifiesAllBuildCols(stages, idx, d, depth-1) {
			return true
		}
	}
	return false
}

// streamSpellingFor is the QUALIFIER a stream carries a declared column under,
// put back on the declared column's own spelling, when the two disagree about
// it and exactly one stream column answers to the declared bare name.
func streamSpellingFor(in []streamCol, declared string) (string, bool) {
	want := strings.TrimSpace(declared)
	if want == "" {
		return "", false
	}
	bare := physical.WantBareName(want)
	written := want
	if dot := strings.LastIndexByte(written, '.'); dot >= 0 {
		written = written[dot+1:]
	}
	match, hits := "", 0
	for _, c := range in {
		if c.Dropped {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(c.Name), want) {
			return "", false // the stream already spells it this way
		}
		if physical.WantBareName(c.Name) == bare {
			match, hits = strings.TrimSpace(c.Name), hits+1
		}
	}
	if hits != 1 {
		return "", false
	}
	if dot := strings.LastIndexByte(match, '.'); dot >= 0 {
		return match[:dot+1] + written, true
	}
	return written, true
}
