// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestIntervalNodeChoices pins IsIntervalNode's CASE and COALESCE arms
// (#1713c round 3): a choice whose every branch is an interval, a quoted
// literal or NULL, one at least an interval, answers an interval
// (PostgreSQL's select_common_type), so the plan declares it as the interval
// literal and an aggregate refuses it as an argument. A branch of any other
// kind — a column, a number — leaves the choice off the rule.
func TestIntervalNodeChoices(t *testing.T) {
	for _, c := range []struct {
		sql  string
		want bool
	}{
		{"CASE WHEN true THEN INTERVAL '1 day' END", true},
		{"CASE WHEN x THEN INTERVAL '1 day' ELSE NULL END", true},
		{"CASE WHEN x THEN INTERVAL '1 day' ELSE '2 days' END", true},
		{"CASE WHEN x THEN CAST(s AS INTERVAL) ELSE INTERVAL '1 day' * 2 END", true},
		{"CASE WHEN true THEN INTERVAL '1 day' END * 2", true},
		{"-CASE WHEN true THEN INTERVAL '1 day' END", true},
		{"COALESCE(INTERVAL '1 day', NULL)", true},
		{"COALESCE(NULL, INTERVAL '1 day', '2 days')", true},
		{"COALESCE(INTERVAL '1 day', NULL) * 2", true},
		// Off the rule.
		{"CASE WHEN x THEN '1 day' ELSE '2 days' END", false},
		{"CASE WHEN x THEN NULL END", false},
		{"CASE WHEN x THEN INTERVAL '1 day' ELSE s END", false},
		{"CASE WHEN x THEN INTERVAL '1 day' ELSE 1 END", false},
		{"COALESCE(s, INTERVAL '1 day')", false},
		{"COALESCE(NULL, '2 days')", false},
		{"CASE WHEN x THEN ts + INTERVAL '1 day' END", false},
	} {
		n, err := plansql.ParseExpression(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if got := IsIntervalNode(n); got != c.want {
			t.Errorf("IsIntervalNode(%s) = %v, want %v", c.sql, got, c.want)
		}
	}
}
