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

func (PlanContext) AggregateOutputNames(node *logical.Node) ([]string, bool) {
	return AggregateOutputNames(node)
}

func (PlanContext) GroupKeysPublishedBelow(node *logical.Node) map[string]string {
	return GroupKeysPublishedBelow(node)
}

func (PlanContext) WrapsAWindow(node *logical.Node) bool {
	return WrapsAWindow(node)
}

func (PlanContext) ScopePreservingWrapper(node *logical.Node) bool {
	return ScopePreservingWrapper(node)
}

// IsSortMergeSource reports whether a local pipeline uses the sort-merge source.
func (PlanContext) IsSortMergeSource(source exec.Source) bool {
	_, ok := source.(*SmjSourceAdapter)
	return ok
}

func (PlanContext) ParseSemiAntiNE(filter string) (string, string, bool) {
	return ParseSemiAntiNE(filter)
}
