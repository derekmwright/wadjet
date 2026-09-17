// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"context"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// WithManifestSnapshot attaches snap to ctx. A coordinator entry point that
// handles one statement end to end but builds several physical.Planner instances for
// it — each construction is a separate physical.NewPlanner call, so a
// physical.Planner-instance-scoped snapshot alone cannot span them — calls this ONCE
// near the top, before any planning begins, and passes the resulting
// context to everything downstream. NewPlannerForContext is the pairing
// half: every physical.Planner built from that context onward shares snap.
func WithManifestSnapshot(ctx context.Context, snap *physical.ManifestSnapshot) context.Context {
	return localPlanFacts.WithManifestSnapshot(ctx, snap)
}
