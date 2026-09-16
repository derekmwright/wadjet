// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// An aggregate emits group keys by GROUP BY text and aggregates by
// AggSpec.OutputCol; a DAG Project emits no stage. Stage.ProjectExprs must
// compute missing SELECT expressions against those published output names,
// quoting computed group-key text as an identifier (#656, #681).
// Apply the projection after HAVING and before any fused sort.
// Plain renames stay pass-through: DAG consumers resolve back to sources.
// Decline any projection that cannot map onto a published output.

// NameIsPlainColumn reports whether a stage output's name is one an ordinary
// column reference can spell — i.e. whether a consumer can name it at all.
// A group key computed from an expression is emitted under the expression's
// own TEXT ("g + 1"), which re-parses as arithmetic over a column the
// aggregate's output does not carry.
func NameIsPlainColumn(s string) bool {
	if s == "" {
		return false
	}
	n, err := plansql.ParseExpression(s)
	if err != nil {
		return false
	}
	ref, isCol := n.(*plansql.ColRef)
	return isCol && ref.Table == "" && strings.EqualFold(ref.Column, s)
}
