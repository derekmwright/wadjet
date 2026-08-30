package physical

import "strings"

// Retargeting the references an absorbed aggregate projection renamed away.
//
// absorbAggregateOutputProjection makes an aggregate stage emit `id` where it
// used to emit the group key's expression text `g + 1`. That is the point —
// no consumer can name the text. But everything ABOVE the stage was written
// against the OLD spelling while the plan was still being built: the gather's
// OutputRenames source, a sort key the resolver chased down to the group key,
// a predicate re-spelled into it. Each of those then names a column no stage
// emits any more.
//
// The gather is the one that fails SILENTLY. Its rename maps
// `"g + 1"` → `id`, finds no such column, falls back to passing the stage's
// output through, and the client gets the aggregate's full width — `[id, v]`
// for `SELECT s.id FROM (… GROUP BY g + 1) s WHERE s.v > 0`, where
// PostgreSQL and the single-process pipeline answer `[id]` (#656 follow-up,
// F1). A sort key or a filter fails loud instead, which is why the gather is
// the one that had to be found by a gate.
//
// The rewrite is deliberately conservative: a name is retargeted only when NO
// stage emits it any more and some stage emits the new one. A plan where both
// spellings are live is one this pass has no business touching.

// aggRenameSite is one absorb: the stage whose output was renamed, and the
// renames it performed.
type aggRenameSite struct {
	stage   string
	renames map[string]string
}

// recordAggProjectionRenames notes one absorb. Nil-safe, because the absorb
// declines far more often than it fires.
func (p *Planner) recordAggProjectionRenames(stageID string, renames map[string]string) {
	if len(renames) == 0 {
		return
	}
	p.aggProjectionRenames = append(p.aggProjectionRenames,
		aggRenameSite{stage: stageID, renames: renames})
}

// stagesDownstreamOf returns the stages that transitively READ the output of
// the stage with this ID, excluding the stage itself.
//
// The direction is the whole of the restriction. A rename lands on the
// aggregate's OUTPUT, so only what reads that output was written against the
// old spelling; everything FEEDING the aggregate — the exchange that
// hash-partitions its input on the group key, the fused partial scan — still
// carries the old name and must keep it. Rewriting upstream turned the
// gather's stale rename into `partitioned shuffle: key "id" not in schema`.
func stagesDownstreamOf(stages []Stage, id string) map[string]bool {
	consumers := map[string][]string{}
	for i := range stages {
		s := &stages[i]
		for _, dep := range s.Dependencies {
			consumers[dep] = append(consumers[dep], s.ID)
		}
		if s.LeftDepStage != "" {
			consumers[s.LeftDepStage] = append(consumers[s.LeftDepStage], s.ID)
		}
		if s.RightDepStage != "" {
			consumers[s.RightDepStage] = append(consumers[s.RightDepStage], s.ID)
		}
	}
	out := map[string]bool{}
	queue := []string{id}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, c := range consumers[cur] {
			if out[c] {
				continue
			}
			out[c] = true
			queue = append(queue, c)
		}
	}
	delete(out, id)
	return out
}

// retargetAbsorbedAggregateRenames points every downstream reference to a
// name an absorbed projection renamed away at the name the stage now emits.
func retargetAbsorbedAggregateRenames(stages []Stage, sites []aggRenameSite) {
	for _, site := range sites {
		retargetOneAbsorbedRename(stages, site)
	}
}

func retargetOneAbsorbedRename(stages []Stage, site aggRenameSite) {
	renames := site.renames
	if len(renames) == 0 {
		return
	}
	downstream := stagesDownstreamOf(stages, site.stage)
	if len(downstream) == 0 {
		return
	}
	// The emitted set is DOWNSTREAM only. The stage feeding the aggregate —
	// a fused partial scan, the exchange that clusters it — still emits the
	// group key's old spelling and always will; counting it made this pass
	// decline the very rename it exists for.
	emitted := map[string]bool{}
	for i := range stages {
		s := &stages[i]
		// The renamed stage itself belongs in the set: it is what emits the
		// NEW name, and it no longer emits the old one.
		if !downstream[s.ID] && s.ID != site.stage {
			continue
		}
		for k := range stageEmittedColumns(s) {
			emitted[k] = true
		}
		// A stage carrying a projection emits EXACTLY that projection —
		// OpProject narrows — so its group keys, aggregate outputs and
		// window columns are gone. Counting them anyway is what made this
		// pass decline the very rename it exists for: the aggregate still
		// LISTS `g + 1` in GroupByCols while emitting only `id`.
		if len(s.ProjectExprs) > 0 {
			continue
		}
		for _, w := range s.WindowCols {
			if w.OutputCol != "" {
				emitted[strings.ToLower(w.OutputCol)] = true
			}
		}
		for _, k := range s.GroupByCols {
			emitted[strings.ToLower(k)] = true
		}
		for _, a := range s.AggSpecs {
			if a.OutputCol != "" {
				emitted[strings.ToLower(a.OutputCol)] = true
			}
		}
	}
	// Only the renames that really did retire their old spelling.
	live := make(map[string]string, len(renames))
	for old, name := range renames {
		if !emitted[old] && emitted[strings.ToLower(name)] {
			live[old] = name
		}
	}
	if len(live) == 0 {
		return
	}
	retarget := func(name string) (string, bool) {
		if name == "" {
			return name, false
		}
		if to, ok := live[strings.ToLower(name)]; ok {
			return to, true
		}
		// The consumer may spell it through the derived table's own alias
		// (`s.id`), which no stage ever carries either.
		if bare := stripQualifier(name); bare != name {
			if to, ok := live[strings.ToLower(bare)]; ok {
				return to, true
			}
		}
		return name, false
	}
	for i := range stages {
		s := &stages[i]
		if !downstream[s.ID] {
			continue
		}
		for j := range s.OutputRenames {
			if s.OutputRenames[j].Expr != nil {
				continue // the gather computes this one; From is only an anchor
			}
			if to, ok := retarget(s.OutputRenames[j].From); ok {
				s.OutputRenames[j].From = to
			}
		}
		for j := range s.SortKeys {
			if to, ok := retarget(s.SortKeys[j].Column); ok {
				s.SortKeys[j].Column = to
			}
			if to, ok := retarget(s.SortKeys[j].AliasSource); ok {
				s.SortKeys[j].AliasSource = to
			}
		}
		if s.Exchange != nil {
			for j, k := range s.Exchange.Keys {
				if to, ok := retarget(k); ok {
					s.Exchange.Keys[j] = to
				}
			}
		}
		for j, k := range s.JoinLeftKeys {
			if to, ok := retarget(k); ok {
				s.JoinLeftKeys[j] = to
			}
		}
		for j, k := range s.JoinRightKeys {
			if to, ok := retarget(k); ok {
				s.JoinRightKeys[j] = to
			}
		}
	}
}
