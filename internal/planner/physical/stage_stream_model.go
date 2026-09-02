package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// What a stage's fragment SHIPS, per column, with the arm it came from.
//
// `stageEmittedColumns` answers a weaker question and answers it for one stage
// at a time: which names appear in this stage's own lists. That is enough for
// a reachability check and it is NOT enough to resolve a GROUP BY key, for two
// reasons this model exists to fix (#795):
//
//   - a JOIN's output is not either side's column list. The executor emits the
//     probe's columns, then the build's with every DUPLICATE name QUALIFIED by
//     its owning alias (`joinOutputSchemaWithMapping`), so a stream really can
//     carry `w` and `y.w` at once — and ADR-0026 §4a's claim that "a join
//     stream carries `w`, never `y.w`" was a fact about the MODEL, not about
//     the engine.
//   - a chained link carries its OWN `Columns` as that link's output filter,
//     so a fused chain's real output is the LAST link's list and not the
//     stage's. Reading the stage's list refused a CTE shape the DAG was
//     executing correctly.
//
// Nothing here infers from node kinds. Every rule below mirrors a line of the
// executor: the qualification rule is `joinOutputSchemaWithMapping`'s, the
// filter rule is its output-filter loop including both halves of the
// qualified↔bare fallback, and a pass-through stage forwards what its
// dependency ships.

// streamCol is one column a fragment's output batch carries.
type streamCol struct {
	// Name is the exact spelling the batch carries.
	Name string
	// Arm is the JOIN ARM this column came from: the `BuildTableAlias` (or
	// `BuildColOrigins` entry) of the build subtree that produced it, and ""
	// for a column the PROBE subtree produced. It is set for EVERY build
	// column, contested or not — the arm is a fact about where the value came
	// from, and the join's duplicate-name qualification only decides how the
	// stream SPELLS it.
	//
	// Setting it only where the name was qualified is what let a GROUP BY key
	// written against one arm bind another arm's column of the same name: with
	// the arm unknown for every uncontested column, no rule could ask the
	// question (#781's R6/R8 cell). The innermost arm wins — a nested join
	// inside a build subtree already named its own arm, and that is the
	// derived table the key can spell.
	Arm string
	// Materialized marks a column some fragment COMPUTES under this exact
	// name: a projection's output, a window's output, an aggregate's key or
	// output. A column merely READ from a table is not materialized, and the
	// difference decides a derived alias: `SUM(id) OVER () + 0 AS g` over a
	// table that also has a `g` is one name and two values, and only "did a
	// fragment compute this" tells them apart (ADR-0026 §4a's third
	// over-fire).
	Materialized bool
	// Dropped marks a build column the join could NOT emit: its bare name
	// collided with one the stream already carried and no alias was available
	// to qualify it with, so the executor drops it (the `case isDup:` arm of
	// joinOutputSchemaWithMapping). It is recorded rather than omitted
	// because a key that names such a column must be REFUSED and not bound
	// to the surviving column of that name, which is a different value.
	Dropped bool
}

// stageStreamColumns lists what the stage at s ships, in output order.
//
// depth bounds the walk the way passThroughDepth bounds every other one here:
// stage graphs are acyclic and these chains are short, and the bound is what
// keeps a malformed graph from spinning.
func stageStreamColumns(stages []Stage, idx map[string]int, s *Stage, depth int) []streamCol {
	return stageStreamColumnsFiltered(stages, idx, s, depth, true)
}

// stageStreamColumnsFiltered is stageStreamColumns with the option to ignore
// every list that only NARROWS and that a later pass can widen back: an
// exchange's payload manifest, a join's OutputFilter, and a scan's shipped set.
//
// That view answers "what can this subtree SUPPLY", which is the question a
// GROUP BY key's resolution asks. The filtered view answers "what does it ship
// today", which is a different and — at the point `resolveStageGroupKeys` runs
// — premature question: `ensureJoinCarriesEvaluatedColumns` runs AFTER it and
// adds back exactly the columns the resolution turns out to need, which is why
// that pass reads the resolutions. A scan's READ SET is the real bound and is
// always honoured, because widening never changes what is scanned.
func stageStreamColumnsFiltered(stages []Stage, idx map[string]int, s *Stage, depth int,
	applyFilters bool) []streamCol {
	if s == nil || depth <= 0 {
		return nil
	}
	dep := func(id string) []streamCol {
		i, ok := idx[id]
		if !ok {
			return nil
		}
		return stageStreamColumnsFiltered(stages, idx, &stages[i], depth-1, applyFilters)
	}
	// An OpProject runs LAST in every fragment that carries one and NARROWS
	// the output to exactly its own list, whatever produced the rows. It is
	// checked before the per-type arms for that reason — and it is where a
	// derived arm's computed alias lives, since attachScanSelectProjections
	// puts `a * 3 AS w` on the producing scan's fragment.
	if len(s.ProjectExprs) > 0 {
		out := make([]streamCol, 0, len(s.ProjectExprs))
		for _, p := range s.ProjectExprs {
			if p.Name == "" {
				continue
			}
			out = append(out, streamCol{Name: p.Name, Materialized: computesName(p)})
		}
		return out
	}
	switch {
	case isJoinStage(s.Type):
		cols, _ := joinStreamColumnsArms(stages, idx, s, depth, applyFilters)
		return cols
	case s.Type == StageScan:
		return scanStreamColumnsFiltered(s, applyFilters)
	case s.Type == StageAggregate || s.Type == StageFinalAggregate || s.Type == StageMergeAggregate:
		if s.GroupByAll {
			// A keys-only aggregate over EVERY input column is
			// name-transparent: it ships its input's schema verbatim.
			return dep(firstDep(s))
		}
		out := make([]streamCol, 0, len(s.GroupByCols)+len(s.AggSpecs))
		for _, n := range aggregateEmittedKeyNames(s) {
			if n != "" {
				out = append(out, streamCol{Name: n, Materialized: true})
			}
		}
		for _, a := range s.AggSpecs {
			if a.OutputCol != "" {
				out = append(out, streamCol{Name: a.OutputCol, Materialized: true})
			}
		}
		return out
	case s.Type == StageUnion:
		if len(s.UnionArms) == 0 {
			return nil
		}
		out := make([]streamCol, 0, len(s.UnionArms[0].Projections))
		for _, p := range s.UnionArms[0].Projections {
			if p.Name != "" {
				out = append(out, streamCol{Name: p.Name, Materialized: true})
			}
		}
		return out
	case forwardsInputColumns(s.Type):
		// Sort, limit, project-with-no-projection, window and every exchange
		// forward their input; a window APPENDS its own outputs, and an
		// exchange's Columns list is a payload manifest that NARROWS.
		out := dep(firstDep(s))
		for _, w := range s.WindowCols {
			if w.OutputCol != "" {
				out = append(out, streamCol{Name: w.OutputCol, Materialized: true})
			}
		}
		if applyFilters && isExchangeStage(s.Type) && len(s.Columns) > 0 {
			out = filterStreamColumns(out, s.Columns)
		}
		return out
	}
	return nil
}

// scanStreamColumns is what a scan fragment ships: the columns it can actually
// READ, narrowed by a security projection, and — on a FUSED scan-aggregate —
// replaced by that aggregate's own output.
//
// `Stage.Columns` is a READ SET and not an output schema: it carries names
// ancestors ask for, including ones no file has, and the worker's projection
// guard reverts to full width rather than failing on them. Intersecting with
// the catalog's declared schema is what makes this a statement about the
// stream instead of about the request (`dropUnbackedJoinColumns` is the same
// correction one stage type over).
func scanStreamColumns(s *Stage) []streamCol {
	return scanStreamColumnsFiltered(s, true)
}

func scanStreamColumnsFiltered(s *Stage, applyFilters bool) []streamCol {
	if len(s.FusedAggGroupBy) > 0 || len(s.FusedAggSpecs) > 0 {
		out := make([]streamCol, 0, len(s.FusedAggGroupBy)+len(s.FusedAggSpecs))
		for _, n := range aggregateEmittedKeyNames(s) {
			if n != "" {
				out = append(out, streamCol{Name: n, Materialized: true})
			}
		}
		for _, a := range s.FusedAggSpecs {
			if a.OutputCol != "" {
				out = append(out, streamCol{Name: a.OutputCol, Materialized: true})
			}
		}
		return out
	}
	if len(s.SecurityProjectExprs) > 0 {
		out := make([]streamCol, 0, len(s.SecurityProjectExprs))
		for _, p := range s.SecurityProjectExprs {
			if p.Name != "" {
				out = append(out, streamCol{Name: p.Name, Materialized: computesName(p)})
			}
		}
		return out
	}
	read := s.Columns
	if applyFilters && len(s.OutputColumns) > 0 {
		read = s.OutputColumns
	}
	declared := map[string]bool{}
	for _, c := range s.ScanSchema {
		declared[strings.ToLower(c.Name)] = true
	}
	out := make([]streamCol, 0, len(read))
	for _, c := range read {
		if c == "" || strings.EqualFold(c, logical.RowCountOnlyColumn) {
			continue
		}
		if len(declared) > 0 && !declared[strings.ToLower(stripQualifier(c))] {
			continue // a name ancestors asked for that this table does not have
		}
		out = append(out, streamCol{Name: c})
	}
	return out
}

// joinStreamColumns models a join fragment's output the way the executor
// builds it: probe columns, then each build's with duplicate names qualified
// by their owning alias, each stage of the chain narrowed by its own output
// filter.
func joinStreamColumns(stages []Stage, idx map[string]int, s *Stage, depth int) []streamCol {
	out, _ := joinStreamColumnsArms(stages, idx, s, depth, true)
	return out
}

// joinStreamColumnsArms is joinStreamColumns with the arm aliases the join
// declares, and with the option to skip the output FILTERS.
//
// The unfiltered view is what a GROUP BY key is resolved against, and the
// difference is deliberate. `Stage.Columns` on a join is an OutputFilter built
// from the join node's NeededColumns at stage-emission time; a column the key
// turns out to need is added back by `ensureJoinCarriesEvaluatedColumns`,
// which runs AFTER `resolveStageGroupKeys` for exactly the reason the filter
// passes do. Resolving against the FILTERED view would refuse a key whose arm
// can supply the value and whose payload is about to be widened to carry it —
// which is the `right → routed` transition the review found (#794 round 2).
func joinStreamColumnsArms(stages []Stage, idx map[string]int, s *Stage, depth int,
	applyFilters bool) ([]streamCol, map[string]bool) {
	arms := map[string]bool{}
	note := func(alias string, origins map[string]string) {
		if alias != "" {
			arms[strings.ToLower(alias)] = true
		}
		for _, o := range origins {
			if o != "" {
				arms[strings.ToLower(o)] = true
			}
		}
	}
	dep := func(id string) []streamCol {
		i, ok := idx[id]
		if !ok {
			return nil
		}
		cols := stageStreamColumnsFiltered(stages, idx, &stages[i], depth-1, applyFilters)
		for _, c := range cols {
			if c.Arm != "" {
				arms[strings.ToLower(c.Arm)] = true
			}
		}
		return cols
	}
	filter := func(cols []streamCol, list []string) []streamCol {
		if !applyFilters || len(list) == 0 {
			return cols
		}
		return filterStreamColumns(cols, list)
	}
	probeID := s.LeftDepStage
	if probeID == "" {
		probeID = firstDep(s)
	}
	out := dep(probeID)
	// The PRIMARY build, then every broadcast join fused onto this stage, in
	// the order the fragment probes them.
	//
	// The primary build is `RightDepStage`, falling back to `Dependencies[1]`
	// — `buildTaskInputsForStage`'s own rule. Taking "every dependency that is
	// not the probe" instead gave a CHAINED link's build the PRIMARY join's
	// alias as well as its own, so the model reported one arm's columns twice
	// under two aliases and a bare key of that name looked ambiguous when the
	// stream has exactly one.
	fusedBuilds := map[string]bool{}
	for _, fj := range s.FusedJoins {
		fusedBuilds[fj.BuildDepStage] = true
	}
	buildID := s.RightDepStage
	if buildID == "" {
		for _, d := range s.Dependencies {
			if d == probeID || fusedBuilds[d] || chainedBuildDep(s, d) {
				continue
			}
			buildID = d
			break
		}
	}
	if buildID != "" {
		note(s.BuildTableAlias, s.BuildColOrigins)
		out = appendBuildColumns(out, dep(buildID), s.BuildTableAlias, s.BuildColOrigins,
			s.QualifyAllBuildCols)
	}
	for _, fj := range s.FusedJoins {
		note(fj.BuildTableAlias, fj.BuildColOrigins)
		out = appendBuildColumns(out, dep(fj.BuildDepStage), fj.BuildTableAlias,
			fj.BuildColOrigins, false)
	}
	out = filter(out, s.Columns)
	// A chained link's OWN Columns is that link's output filter, and the LAST
	// one is what the fused stage emits. Reading the stage's list instead
	// under-reports every fused chain (#795).
	for _, cj := range s.ChainedJoins {
		note(cj.BuildTableAlias, cj.BuildColOrigins)
		out = appendBuildColumns(out, dep(cj.BuildDepStage), cj.BuildTableAlias,
			cj.BuildColOrigins, cj.QualifyAllBuildCols)
		out = filter(out, cj.Columns)
	}
	if len(s.ChainedAggGroupBy) > 0 || len(s.ChainedAggSpecs) > 0 {
		// A chain-terminal partial aggregate REPLACES the join's stream with
		// its own output — but the keys are resolved against the stream
		// above, which is what the caller asked for. See
		// aggregateInputStreamColumns.
		agg := make([]streamCol, 0, len(s.ChainedAggGroupBy)+len(s.ChainedAggSpecs))
		for _, n := range aggregateEmittedKeyNames(s) {
			if n != "" {
				agg = append(agg, streamCol{Name: n, Materialized: true})
			}
		}
		for _, a := range s.ChainedAggSpecs {
			if a.OutputCol != "" {
				agg = append(agg, streamCol{Name: a.OutputCol, Materialized: true})
			}
		}
		return agg, arms
	}
	return out, arms
}

// chainedBuildDep reports whether id is the build input of a chained link,
// which the chain loop appends under its OWN alias.
func chainedBuildDep(s *Stage, id string) bool {
	for _, cj := range s.ChainedJoins {
		if cj.BuildDepStage == id {
			return true
		}
	}
	return false
}

// appendBuildColumns applies `joinOutputSchemaWithMapping`'s naming rule: a
// build column whose bare name the stream already carries is QUALIFIED by its
// owning alias, one that already carries a qualifier is emitted verbatim, and
// a duplicate with no alias to disambiguate by is dropped.
func appendBuildColumns(probe, build []streamCol, buildAlias string,
	origins map[string]string, qualifyAll bool) []streamCol {
	seen := make(map[string]bool, len(probe))
	for _, c := range probe {
		if !c.Dropped {
			seen[c.Name] = true
		}
	}
	// The arm a build column belongs to: the one a nested join inside the
	// build subtree already named, else this join's build alias.
	armOf := func(c streamCol) string {
		if c.Arm != "" {
			return c.Arm
		}
		if o := origins[strings.ToLower(c.Name)]; o != "" {
			return o
		}
		return buildAlias
	}
	out := append([]streamCol(nil), probe...)
	for _, c := range build {
		arm := armOf(c)
		if strings.IndexByte(c.Name, '.') >= 0 {
			// Named by a nested join INSIDE the build subtree; already unique.
			c.Arm = arm
			out = append(out, c)
			seen[c.Name] = true
			continue
		}
		alias := buildAlias
		if o := origins[strings.ToLower(c.Name)]; o != "" {
			alias = o
		}
		isDup := seen[c.Name]
		switch {
		case (isDup || qualifyAll) && alias != "":
			q := alias + "." + c.Name
			if seen[q] {
				continue // the same build column reached this stream twice
			}
			seen[q] = true
			out = append(out, streamCol{Name: q, Arm: arm, Materialized: c.Materialized})
		case isDup:
			// No alias to disambiguate by — the executor drops it. Recorded
			// so a key naming this arm's column is refused rather than bound
			// to the OTHER arm's column of the same name.
			out = append(out, streamCol{Name: c.Name, Arm: arm,
				Materialized: c.Materialized, Dropped: true})
		default:
			c.Arm = arm
			out = append(out, c)
			seen[c.Name] = true
		}
	}
	return out
}

// filterStreamColumns applies a join's or an exchange's output filter with
// BOTH halves of the qualified↔bare fallback the executor applies: a filter
// entry keeps a column whose bare name it spells, and a bare column whose
// QUALIFIED spelling the filter names.
func filterStreamColumns(cols []streamCol, filter []string) []streamCol {
	if len(filter) == 0 {
		return cols
	}
	want := make(map[string]bool, len(filter))
	bareWanted := make(map[string]bool, len(filter))
	for _, f := range filter {
		want[f] = true
		if dot := strings.IndexByte(f, '.'); dot > 0 && dot < len(f)-1 {
			bareWanted[strings.ToLower(f[dot+1:])] = true
		}
	}
	out := make([]streamCol, 0, len(cols))
	for _, c := range cols {
		if c.Dropped {
			out = append(out, c) // a dropped column survives the filter as a record
			continue
		}
		keep := want[c.Name]
		if !keep {
			if dot := strings.IndexByte(c.Name, '.'); dot >= 0 {
				keep = want[c.Name[dot+1:]]
			}
		}
		if !keep && strings.IndexByte(c.Name, '.') < 0 {
			keep = bareWanted[strings.ToLower(c.Name)]
		}
		if keep {
			out = append(out, c)
		}
	}
	if len(out) == len(cols) {
		return cols
	}
	return out
}

// aggregateInputStreamColumns lists what the fragment computing s's GROUP BY
// keys resolves them against.
//
// It is deliberately NOT "what s emits": an aggregate's keys are looked up in
// its INPUT. Three shapes carry the aggregate on a stage that also produces
// that input — a fused scan-aggregate reads the table, a chain-terminal
// partial reads the join's rows — and for those the input is the stage's own
// pre-aggregate stream rather than its dependency's.
func aggregateInputStreamColumns(stages []Stage, idx map[string]int, s *Stage) ([]streamCol, map[string]bool) {
	switch {
	case s.Type == StageScan:
		raw := *s
		raw.FusedAggGroupBy, raw.FusedAggSpecs = nil, nil
		return scanStreamColumns(&raw), nil
	case isJoinStage(s.Type):
		bare := *s
		bare.ChainedAggGroupBy, bare.ChainedAggSpecs = nil, nil
		return joinStreamColumnsArms(stages, idx, &bare, passThroughDepth, false)
	case s.Type == StageAggregate, s.Type == StageFinalAggregate, s.Type == StageMergeAggregate:
	default:
		return nil, nil
	}
	i, ok := idx[firstDep(s)]
	if !ok {
		return nil, nil
	}
	dep := &stages[i]
	if isJoinStage(dep.Type) {
		// The aggregate reads this join's output, and it is resolved against
		// what the join's ARMS produce rather than what its OutputFilter
		// currently ships — see joinStreamColumnsArms.
		return joinStreamColumnsArms(stages, idx, dep, passThroughDepth, false)
	}
	return stageStreamColumnsFiltered(stages, idx, dep, passThroughDepth, false), nil
}

// computesName reports whether a projection spec COMPUTES its output rather
// than copying a column of that same name through.
func computesName(p ProjectExprSpec) bool {
	return p.Expr != "" && !strings.EqualFold(strings.TrimSpace(p.Expr), strings.TrimSpace(p.Name))
}

// firstDep is a stage's single upstream, or "" when it has none or many.
func firstDep(s *Stage) string {
	if len(s.Dependencies) == 0 {
		return ""
	}
	return s.Dependencies[0]
}

// isExchangeStage reports whether typ MOVES rows without computing any.
func isExchangeStage(typ string) bool {
	switch typ {
	case StageExchangeRepartition, StageExchangeReplicate, StageExchangeGather:
		return true
	}
	return false
}
