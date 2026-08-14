package physical

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync/atomic"
)

// SharedSubplanDedup gates dedupeSharedSubplans. Kill switch
// WADJET_SHARED_SUBPLAN=0. Exported atomic.Bool (ScalarAggSemijoin
// pattern) so tests can pin either arm.
var SharedSubplanDedup atomic.Bool

func init() {
	SharedSubplanDedup.Store(os.Getenv("WADJET_SHARED_SUBPLAN") != "0")
}

// dedupeSharedSubplans drops stage subtrees that duplicate a sibling subtree,
// rewiring the duplicate's consumers onto the surviving root and letting the
// orphaned subtree fall out of the plan. Duplicates arise when subquery
// decorrelation clones a leg of the main query (Q11's scalar-subquery leg is
// a stage-for-stage copy of its main leg; Q17's decorrelated AVG leg re-joins
// lineitem⋈part that the main leg already computed).
//
// Equivalence is a structural fingerprint over the stage subtree that
// deliberately EXCLUDES projected columns (Columns/OutputColumns) and scan
// aliases — clones consumed by different outer queries always disagree on
// those (same rationale as cteSubtreeHash's RequiredColumns exclusion). What
// a leg projects is instead handled by the coverage check: the kept leg's
// scans must read a superset of the dropped leg's columns, so the kept output
// resolves every name the rewired consumers reference.
//
// Two match forms:
//
//  1. Exact: fingerprints equal. Any consumer may rewire.
//  2. Semi≡inner: a semi join whose fingerprint-with-JoinType-"inner" matches
//     an inner join (same children, keys, no JoinFilter). The inner output is
//     the semi output with build columns appended and probe rows repeated
//     once per matching build row. No build-uniqueness oracle exists, so
//     rewiring is gated on every consumer being an aggregate that is
//     provably invariant under that duplication: GroupByCols ⊇ the probe
//     join keys (the duplication factor is a function of the join key, hence
//     constant within each group) and every aggregate function is
//     duplication-invariant (avg/min/max). Q17's AVG(l_quantity) GROUP BY
//     l_partkey consumer is exactly this shape.
//
// Runs after distributions are final and BEFORE fuseStageChains: chain
// fusion absorbs consumers into the legs, making clones asymmetric; at this
// position the legs are still stage-for-stage symmetric. The coordinator
// side needs nothing new — stage outputs already support N consumers
// (Q18's rp-7 is read twice) and scratch cleanup is per-query.
func dedupeSharedSubplans(stages []Stage) []Stage {
	if !SharedSubplanDedup.Load() {
		return stages
	}
	d := newSubplanDeduper(stages)

	// Candidate roots: joins only. Deduping a bare scan/exchange pair is
	// already covered by dedupeSubsumedScanExchanges and EnsureDistribution's
	// exchange sharing; join-rooted subtrees are where the clone legs are
	// (and deduping the maximal root subsumes every duplicate below it).
	type candidate struct {
		idx  int
		fp   string
		size int
	}
	var cands []candidate
	for i := range stages {
		s := &stages[i]
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin && s.Type != StageSortMergeJoin {
			continue
		}
		fp, ok := d.fingerprint(s.ID)
		if !ok {
			continue
		}
		cands = append(cands, candidate{idx: i, fp: fp, size: d.subtreeSize(s.ID)})
	}
	// Largest subtrees first so maximal clones dedup before their internals.
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].size > cands[j].size })

	// Pass 1: exact matches. The first live member holds the group; the
	// coverage walk inside rewireDuplicate may hand keepership to a
	// wider-columned duplicate, so track the survivor across the group.
	rewired := false
	byFP := make(map[string][]int)
	for _, c := range cands {
		byFP[c.fp] = append(byFP[c.fp], c.idx)
	}
	processed := make(map[string]bool)
	for _, c := range cands {
		if processed[c.fp] {
			continue
		}
		processed[c.fp] = true
		group := byFP[c.fp]
		if len(group) < 2 {
			continue
		}
		keeper := -1
		for _, gi := range group {
			if d.dropped[stages[gi].ID] {
				continue
			}
			if keeper == -1 {
				keeper = gi
				continue
			}
			if surv, changed := d.rewireDuplicate(keeper, gi, false); changed {
				rewired = true
				keeper = surv
			}
		}
	}

	// Pass 2: semi joins riding a live inner sibling.
	for _, c := range cands {
		b := &stages[c.idx]
		if d.dropped[b.ID] || b.JoinType != "semi" || b.JoinFilter != "" {
			continue
		}
		fpInner, ok := d.fingerprintAs(b.ID, "inner")
		if !ok {
			continue
		}
		for _, ai := range byFP[fpInner] {
			a := &stages[ai]
			if d.dropped[a.ID] || a.JoinType != "inner" {
				continue
			}
			if _, changed := d.rewireDuplicate(ai, c.idx, true); changed {
				rewired = true
				break
			}
		}
	}

	if !rewired {
		return stages
	}
	return d.sweep()
}

type subplanDeduper struct {
	stages    []Stage
	idx       map[string]int      // stage ID -> index
	consumers map[string][]int    // stage ID -> indexes of consuming stages (all edge kinds)
	roots     []int               // pre-rewire zero-consumer stages (the gather et al.)
	fps       map[string]string   // stage ID -> fingerprint ("" = memoized failure)
	fpOK      map[string]bool
	sizes     map[string]int
	dropped   map[string]bool
}

func newSubplanDeduper(stages []Stage) *subplanDeduper {
	d := &subplanDeduper{
		stages:    stages,
		idx:       make(map[string]int, len(stages)),
		consumers: make(map[string][]int),
		fps:       make(map[string]string),
		fpOK:      make(map[string]bool),
		sizes:     make(map[string]int),
		dropped:   make(map[string]bool),
	}
	for i := range stages {
		d.idx[stages[i].ID] = i
	}
	for i := range stages {
		for _, dep := range stageEdgeRefs(&stages[i]) {
			d.consumers[dep] = append(d.consumers[dep], i)
		}
	}
	for i := range stages {
		if len(d.consumers[stages[i].ID]) == 0 {
			d.roots = append(d.roots, i)
		}
	}
	return d
}

// stageEdgeRefs returns every stage ID this stage references: dependencies
// plus the side-channel reference fields. Anything added here must also be
// handled by rewireEdges (and, if it can appear on a fingerprinted stage, by
// the slot resolution in fingerprintAs) — see the field-coverage guard test.
func stageEdgeRefs(s *Stage) []string {
	refs := make([]string, 0, len(s.Dependencies)+2)
	refs = append(refs, s.Dependencies...)
	seen := func(id string) bool {
		for _, r := range refs {
			if r == id {
				return true
			}
		}
		return false
	}
	add := func(id string) {
		if id != "" && !seen(id) {
			refs = append(refs, id)
		}
	}
	add(s.LeftDepStage)
	add(s.RightDepStage)
	for i := range s.FusedJoins {
		add(s.FusedJoins[i].BuildDepStage)
	}
	for i := range s.ChainedJoins {
		add(s.ChainedJoins[i].BuildDepStage)
	}
	for _, v := range s.ScalarDependencies {
		add(v)
	}
	for i := range s.ConsumeDynamicFilters {
		add(s.ConsumeDynamicFilters[i].SourceStageID)
	}
	return refs
}

// rewireEdges redirects every reference to `from` onto `to` in a consumer.
func rewireEdges(s *Stage, from, to string) {
	for i, dep := range s.Dependencies {
		if dep == from {
			s.Dependencies[i] = to
		}
	}
	if s.LeftDepStage == from {
		s.LeftDepStage = to
	}
	if s.RightDepStage == from {
		s.RightDepStage = to
	}
	for i := range s.FusedJoins {
		if s.FusedJoins[i].BuildDepStage == from {
			s.FusedJoins[i].BuildDepStage = to
		}
	}
	for i := range s.ChainedJoins {
		if s.ChainedJoins[i].BuildDepStage == from {
			s.ChainedJoins[i].BuildDepStage = to
		}
	}
	for k, v := range s.ScalarDependencies {
		if v == from {
			s.ScalarDependencies[k] = to
		}
	}
	for i := range s.ConsumeDynamicFilters {
		if s.ConsumeDynamicFilters[i].SourceStageID == from {
			s.ConsumeDynamicFilters[i].SourceStageID = to
		}
	}
}

// sharedSubplanTypes are the stage types a fingerprintable subtree may
// contain. Anything else (aggregates, sorts, windows, gathers) makes the
// subtree fingerprint fail — v1 dedups the join-over-scan legs that
// decorrelation clones, nothing wider.
func sharedSubplanFingerprintable(t string) bool {
	switch t {
	case StageScan, StageExchangeReplicate, StageExchangeRepartition,
		StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return true
	}
	return false
}

func (d *subplanDeduper) fingerprint(id string) (string, bool) {
	if ok, memoed := d.fpOK[id]; memoed {
		return d.fps[id], ok
	}
	fp, ok := d.fingerprintAs(id, "")
	d.fps[id], d.fpOK[id] = fp, ok
	return fp, ok
}

// fingerprintAs computes the structural fingerprint of the subtree rooted at
// id. joinTypeOverride, when non-empty, replaces the ROOT stage's JoinType
// (the semi≡inner probe); child fingerprints always use their real types.
func (d *subplanDeduper) fingerprintAs(id, joinTypeOverride string) (string, bool) {
	i, ok := d.idx[id]
	if !ok {
		return "", false
	}
	s := d.stages[i]
	if !sharedSubplanFingerprintable(s.Type) {
		return "", false
	}
	// Fields that would need semantics this pass doesn't model: refuse to
	// fingerprint rather than risk a false match. All are either set by
	// later passes (dynamic filters) or never appear on these stage types.
	if len(s.ScalarDependencies) > 0 || len(s.OutputRenames) > 0 ||
		len(s.EmitDynamicFilters) > 0 || len(s.ConsumeDynamicFilters) > 0 ||
		len(s.PreComputedAggregates) > 0 || len(s.BuildCachePreScans) > 0 {
		return "", false
	}

	// Resolve dep-referencing fields to dependency slot indexes so clone
	// legs (different IDs, same structure) hash identically.
	slot := make(map[string]string, len(s.Dependencies))
	for di, dep := range s.Dependencies {
		slot[dep] = fmt.Sprintf("@%d", di)
	}
	resolve := func(ref string) (string, bool) {
		if ref == "" {
			return "", true
		}
		sl, ok := slot[ref]
		return sl, ok
	}

	c := s // shallow copy; slices are shared but only replaced, never mutated
	c.ID = ""
	c.Dependencies = nil
	c.ScanAlias = ""
	c.Columns = nil
	c.OutputColumns = nil
	c.ScanFileSizes = nil
	c.EstimatedBytes = 0
	c.EstimatedRows = 0
	if joinTypeOverride != "" {
		c.JoinType = joinTypeOverride
	}
	if c.LeftDepStage, ok = resolve(s.LeftDepStage); !ok {
		return "", false
	}
	if c.RightDepStage, ok = resolve(s.RightDepStage); !ok {
		return "", false
	}
	if len(s.FusedJoins) > 0 {
		c.FusedJoins = append([]FusedJoinSpec(nil), s.FusedJoins...)
		for fi := range c.FusedJoins {
			if c.FusedJoins[fi].BuildDepStage, ok = resolve(s.FusedJoins[fi].BuildDepStage); !ok {
				return "", false
			}
		}
	}
	if len(s.ChainedJoins) > 0 {
		c.ChainedJoins = append([]ChainedJoinSpec(nil), s.ChainedJoins...)
		for ci := range c.ChainedJoins {
			if c.ChainedJoins[ci].BuildDepStage, ok = resolve(s.ChainedJoins[ci].BuildDepStage); !ok {
				return "", false
			}
		}
	}
	if s.Exchange != nil {
		ex := *s.Exchange
		// Consumer-derived candidate-selection metadata, not subtree
		// semantics — clone legs differ only by the ":N" alias suffix here.
		ex.BuildAlias = ""
		ex.ProbeAlias = ""
		ex.BuildBytes = 0
		c.Exchange = &ex
	}

	blob, err := json.Marshal(&c) // sorts map keys — deterministic
	if err != nil {
		return "", false
	}
	h := sha256.New()
	h.Write(blob)
	for _, dep := range s.Dependencies {
		depFP, ok := d.fingerprint(dep)
		if !ok {
			return "", false
		}
		h.Write([]byte{0})
		h.Write([]byte(depFP))
	}
	return hex.EncodeToString(h.Sum(nil)), true
}

func (d *subplanDeduper) subtreeSize(id string) int {
	if n, ok := d.sizes[id]; ok {
		return n
	}
	seen := make(map[string]bool)
	var walk func(string)
	walk = func(cur string) {
		if seen[cur] {
			return
		}
		seen[cur] = true
		if i, ok := d.idx[cur]; ok {
			for _, dep := range d.stages[i].Dependencies {
				walk(dep)
			}
		}
	}
	walk(id)
	d.sizes[id] = len(seen)
	return len(seen)
}

// rewireDuplicate points every consumer of stages[bi] at stages[ai] and marks
// bi dropped. The coverage walk may swap keeper and duplicate (exact matches
// only) when the duplicate's scans read the wider column set. Returns the
// surviving keeper index and whether anything was rewired; on gate failure
// nothing is mutated and the original keeper survives.
func (d *subplanDeduper) rewireDuplicate(ai, bi int, semiToInner bool) (int, bool) {
	a, b := &d.stages[ai], &d.stages[bi]
	dir, ok := d.coverageDirection(a.ID, b.ID)
	if !ok {
		slog.Info("shared_subplan: skip, scan columns incomparable or collision hazard",
			"keep", a.ID, "drop", b.ID)
		return ai, false
	}
	if dir < 0 {
		if semiToInner {
			// The inner join must stay the keeper: its output is the
			// superset shape. A wider semi leg cannot serve the inner
			// consumers.
			slog.Info("shared_subplan: skip, semi leg reads wider columns than inner sibling",
				"inner", a.ID, "semi", b.ID)
			return ai, false
		}
		ai, bi = bi, ai
		a, b = b, a
	}

	bCons := d.consumers[b.ID]
	if len(bCons) == 0 {
		return ai, false
	}
	// All-or-nothing: every consumer must be rewireable, else B stays.
	for _, ci := range bCons {
		c := &d.stages[ci]
		if d.dropped[c.ID] {
			continue
		}
		for _, ref := range stageEdgeRefs(c) {
			if ref == a.ID {
				// Consumer already reads the keeper (self-join shapes).
				// Rewiring would collapse two distinct inputs into one slot.
				slog.Info("shared_subplan: skip, consumer already depends on keeper",
					"keep", a.ID, "drop", b.ID, "consumer", c.ID)
				return ai, false
			}
		}
		if semiToInner && !semiConsumerDuplicationInvariant(c, b.JoinLeftKeys) {
			slog.Info("shared_subplan: skip, semi consumer not duplication-invariant",
				"inner", a.ID, "semi", b.ID, "consumer", c.ID)
			return ai, false
		}
	}
	for _, ci := range bCons {
		if !d.dropped[d.stages[ci].ID] {
			rewireEdges(&d.stages[ci], b.ID, a.ID)
		}
	}
	d.dropped[b.ID] = true
	slog.Info("shared_subplan: deduped", "keep", a.ID, "drop", b.ID,
		"semi_to_inner", semiToInner, "subtree_stages", d.subtreeSize(b.ID))
	return ai, true
}

// coverageDirection walks the two fingerprint-matched subtrees in lockstep
// and compares paired SCAN column sets (joins' Columns carry NeededColumns
// residue that differs between clone legs; scans are the real column
// source). Returns dir=+1 when a's scans cover b's, dir=-1 when b's cover
// a's (0 when equal), ok=false when the legs are incomparable or when the
// keeper's extra probe columns would collide with a build-side output name —
// a collision flips the join executor's qualify-on-collision naming and
// silently rebinds bare references in rewired consumers.
func (d *subplanDeduper) coverageDirection(aID, bID string) (int, bool) {
	dir := 0
	var extraA, extraB []string // columns one side's scans read beyond the other's
	var walk func(aID, bID string) bool
	walk = func(aID, bID string) bool {
		ai, aok := d.idx[aID]
		bi, bok := d.idx[bID]
		if !aok || !bok {
			return false
		}
		a, b := &d.stages[ai], &d.stages[bi]
		if a.Type == StageScan {
			aSet := colSet(a.Columns)
			bSet := colSet(b.Columns)
			aCovers, bCovers := covers(aSet, bSet), covers(bSet, aSet)
			switch {
			case aCovers && bCovers:
			case aCovers:
				extraA = append(extraA, diffCols(aSet, bSet)...)
				if dir < 0 {
					return false
				}
				dir = 1
			case bCovers:
				extraB = append(extraB, diffCols(bSet, aSet)...)
				if dir > 0 {
					return false
				}
				dir = -1
			default:
				return false
			}
			return true
		}
		if len(a.Dependencies) != len(b.Dependencies) {
			return false // fingerprints matched, so this should not happen
		}
		for i := range a.Dependencies {
			if !walk(a.Dependencies[i], b.Dependencies[i]) {
				return false
			}
		}
		return true
	}
	if !walk(aID, bID) {
		return 0, false
	}
	// Collision hazard: the keeper's extra probe columns must not shadow a
	// build-side output name anywhere in the kept subtree.
	extras := extraA
	keepID := aID
	if dir < 0 {
		extras = extraB
		keepID = bID
	}
	if len(extras) > 0 && d.buildColsCollide(keepID, extras) {
		return 0, false
	}
	return dir, true
}

// buildColsCollide reports whether any of cols appears among the scan
// columns of a build-side subtree (RightDepStage or FusedJoins builds) of
// any join in the subtree rooted at id.
func (d *subplanDeduper) buildColsCollide(id string, cols []string) bool {
	colset := colSet(cols)
	var buildScanCols func(string) map[string]bool
	buildScanCols = func(cur string) map[string]bool {
		out := make(map[string]bool)
		i, ok := d.idx[cur]
		if !ok {
			return out
		}
		s := &d.stages[i]
		for _, c := range s.Columns {
			if s.Type == StageScan {
				out[c] = true
			}
		}
		for _, dep := range s.Dependencies {
			for c := range buildScanCols(dep) {
				out[c] = true
			}
		}
		return out
	}
	var walk func(string) bool
	walk = func(cur string) bool {
		i, ok := d.idx[cur]
		if !ok {
			return false
		}
		s := &d.stages[i]
		var builds []string
		if s.RightDepStage != "" {
			builds = append(builds, s.RightDepStage)
		}
		for _, fj := range s.FusedJoins {
			builds = append(builds, fj.BuildDepStage)
		}
		for _, bdep := range builds {
			for c := range buildScanCols(bdep) {
				if colset[c] {
					return true
				}
			}
		}
		for _, dep := range s.Dependencies {
			if walk(dep) {
				return true
			}
		}
		return false
	}
	return walk(id)
}

// semiConsumerDuplicationInvariant reports whether consumer c's result is
// unchanged when its input's rows are uniformly duplicated within each value
// of the probe join keys — the condition under which a semi-join consumer
// may read the matching inner join's output without a build-uniqueness
// proof. Requires an aggregate grouping on (at least) the probe keys, with
// only duplication-invariant functions.
func semiConsumerDuplicationInvariant(c *Stage, probeKeys []string) bool {
	if c.Type != StageAggregate && c.Type != "final_aggregate" {
		return false
	}
	if len(c.GroupByCols) == 0 || len(probeKeys) == 0 {
		return false
	}
	group := colSet(c.GroupByCols)
	for _, k := range probeKeys {
		if !group[k] {
			return false
		}
	}
	if len(c.AggSpecs) == 0 {
		return false
	}
	for _, a := range c.AggSpecs {
		switch a.Func {
		case "avg", "min", "max":
		default:
			return false
		}
	}
	// Absorbed specs would mean this stage does more than the plain
	// aggregate the invariance argument covers.
	if len(c.FusedAggSpecs) > 0 || len(c.ChainedAggSpecs) > 0 ||
		len(c.ChainedJoins) > 0 || len(c.FusedJoins) > 0 || c.GroupByAll {
		return false
	}
	return true
}

// sweep removes stages no longer reachable from the plan's pre-rewire roots.
func (d *subplanDeduper) sweep() []Stage {
	live := make(map[string]bool, len(d.stages))
	var walk func(string)
	walk = func(id string) {
		if live[id] {
			return
		}
		live[id] = true
		if i, ok := d.idx[id]; ok {
			for _, ref := range stageEdgeRefs(&d.stages[i]) {
				walk(ref)
			}
		}
	}
	for _, ri := range d.roots {
		walk(d.stages[ri].ID)
	}
	out := make([]Stage, 0, len(d.stages))
	for i := range d.stages {
		if live[d.stages[i].ID] {
			out = append(out, d.stages[i])
		}
	}
	return out
}

func colSet(cols []string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}

func covers(a, b map[string]bool) bool {
	for c := range b {
		if !a[c] {
			return false
		}
	}
	return true
}

func diffCols(a, b map[string]bool) []string {
	var out []string
	for c := range a {
		if !b[c] {
			out = append(out, c)
		}
	}
	return out
}
