// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// A FILTER PUSHED BELOW A DERIVED TABLE'S PROJECTION BINDS THE PUBLISHED
// COLUMN, NEVER A SIBLING RELATION'S — arc JP round 6 closure review, P1
// (#1299).
//
// `SELECT d.* FROM (SELECT i.id, i.oid, j.v FROM jp_i i JOIN jp_j j ON …) d
// WHERE d.v = 6` answered `2,1,5 | 5,3,6` on single/spilled512k/fastpath
// where PostgreSQL answers `1,1,6 | 3,2,6 | 5,3,6`: pushdownPredicates'
// Filter-Project swap (splitFilterForProjectPush) treated the outer alias
// `d` as an unrecognized qualifier and left `d.v = 6` untouched below the
// Project — where `d` names nothing — and a later qualifier strip bound
// `i.v` (an UNSELECTED column jp_i also happens to carry) instead of the
// published `v` (`j.v`). Two site fixes, both scoped to the swap's own
// projRefs (projRefs.blockSwap, filter_project_pushdown.go):
//
//   - classifyProjection no longer treats a QUALIFIED passthrough item
//     (`j.v` published under its own bare name `v`) as needing no rewrite:
//     a sibling relation can carry an unselected column of that same bare
//     name, so it substitutes the qualified source exactly like a rename.
//   - projRefs.resolve's fallback for a qualifier that names neither a
//     relation this Project draws from nor one of its own outputs — the
//     ONLY thing such a qualifier can still mean, in this context, is the
//     alias the ENCLOSING query gives this whole block, never visible to a
//     reference written inside it — now resolves the bare column against
//     the block's own published list instead of leaving the (now
//     meaningless) qualifier in place.
//
// Both changes are gated on blockSwap so the stage DAG's OWN, separate
// filter-through-project respelling walk (ResolveFilterThroughProjects,
// ridden by dagplan's per-stage filter carrier) keeps its prior reading
// exactly: an unresolved qualifier reaching THAT walk is not guaranteed to
// be self. The DAG arms are already wrong on the `sameNameBothSources` and
// `aliased` cells below at a1892b54 for an UNRELATED, pre-existing reason —
// a same-named sibling column pairs wrong on the stage DAG independent of
// this filter (arc JP round 5 closure review N3, filed FC-JP-3 #1326); the
// pins here are that residual's rows, not a new one, and the fix upstream of
// dagplan's own stage split is what let the single-process rows come out
// right at all (the DAG shares the identical optimized logical plan, so the
// corrected `j.v = 6` is what it receives too — dagplan's own defect just
// keeps mispairing i's and j's identically-named columns downstream of it).
func TestArcJP6DerivedStarFilterBindsThePublishedColumnOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the arc JP round-6 P1 cells")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := jpArms(t, ctx)
	jpRun(t, arms, jp6P1Cells, jp6P1Postgres, nil, jp6P1Pins)
}

var jp6P1Cells = []jpCell{
	// A same-name column on BOTH sources: only jp_j.v is published as `v`,
	// but jp_i also carries an UNSELECTED `v` — the shape that answered
	// wrong.
	{"p1/sameNameBothSources", "SELECT d.* FROM (SELECT i.id, i.oid, j.v FROM jp_i i JOIN jp_j j ON j.id = i.id) d WHERE d.v = 6 ORDER BY 1"},
	// One source only: no sibling can carry an ambiguous `v`, so the
	// original "same name below" passthrough was always sound here — a
	// sanity check that the fix leaves this shape untouched.
	{"p1/oneSource", "SELECT d.* FROM (SELECT i.id, i.oid, i.v FROM jp_i i) d WHERE d.v = 6 ORDER BY 1"},
	// The block's own alias spelled something OTHER than `d`: the fix names
	// no specific alias string, so any enclosing name must resolve the same
	// way.
	{"p1/aliased", "SELECT dd.* FROM (SELECT i.id, i.oid, j.v FROM jp_i i JOIN jp_j j ON j.id = i.id) dd WHERE dd.v = 6 ORDER BY 1"},
}

var jp6P1Postgres = map[string]string{
	"p1/sameNameBothSources": "rows=3 1,1,6 | 3,2,6 | 5,3,6",
	"p1/oneSource":           "rows=2 2,1,6 | 5,3,6",
	"p1/aliased":             "rows=3 1,1,6 | 3,2,6 | 5,3,6",
}

// jp6P1Pins: the stage DAG's pre-existing, unrelated column-pairing defect
// (N3, FC-JP-3 #1326) on the two multi-relation cells. A pin that starts
// agreeing with PostgreSQL fails — delete it the moment dagplan's own fix
// lands.
var jp6P1Pins = map[string]map[string]string{
	"p1/sameNameBothSources": {
		"dag":          "rows=3 1,1,5 | 3,2,5 | 5,3,6",
		"dag-shuffled": "rows=3 1,1,5 | 3,2,5 | 5,3,6",
	},
	"p1/aliased": {
		"dag":          "rows=3 1,1,5 | 3,2,5 | 5,3,6",
		"dag-shuffled": "rows=3 1,1,5 | 3,2,5 | 5,3,6",
	},
}
