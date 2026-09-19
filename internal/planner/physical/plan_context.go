// SPDX-License-Identifier: MIT

package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// PlanContext gives a caller the local planner's state and derived facts.
// It shares the planner rather than copying statement state or its manifest
// snapshot. Methods that only inspect their arguments also accept a zero value.
type PlanContext struct {
	*Planner
}

// PlanContext returns the context for this planner's current statement.
func (p *Planner) PlanContext() *PlanContext { return &PlanContext{Planner: p} }

// LogicalOptions is this planner's configuration as the logical optimizer
// takes it. Any caller that re-optimizes a plan under this planner — the
// local subquery pipeline, and the stage planner's DAG counterpart — passes
// it to logical.OptimizeWith so the second query is planned by the same
// instance settings as the first (#1223). One accessor rather than a field
// per option: a new Options field then costs nothing at the seam.
func (p *Planner) LogicalOptions() logical.Options {
	if p == nil {
		return logical.Options{}
	}
	return logical.Options{BushyJoinReorder: p.BushyJoinReorder}
}

func (PlanContext) AggregateOutputNames(node *logical.Node) ([]string, bool) {
	return aggregateOutputNames(node)
}

func (PlanContext) GroupKeysPublishedBelow(node *logical.Node) map[string]string {
	return groupKeysPublishedBelow(node)
}

func (PlanContext) WrapsAWindow(node *logical.Node) bool {
	return wrapsAWindow(node)
}

func (PlanContext) ScopePreservingWrapper(node *logical.Node) bool {
	return scopePreservingWrapper(node)
}

// IsSortMergeSource reports whether a local pipeline uses the sort-merge source.
func (PlanContext) IsSortMergeSource(source exec.Source) bool {
	_, ok := source.(*smjSourceAdapter)
	return ok
}

func (PlanContext) ParseSemiAntiNE(filter string) (string, string, bool) {
	return parseSemiAntiNE(filter)
}
