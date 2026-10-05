// SPDX-License-Identifier: MIT

package logical

import (
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

func sampledScans(n *Node) (sampled, all int) {
	if n == nil {
		return 0, 0
	}
	if n.Type == NodeScan {
		all++
		if n.SampleMethod != "" {
			sampled++
		}
	}
	for _, c := range n.Children {
		s, a := sampledScans(c)
		sampled, all = sampled+s, all+a
	}
	return sampled, all
}

// A SAMPLE SCAN POSTGRESQL NEVER BEGINS IS PLANNED WITHOUT ITS SAMPLER
// (#1411 review r1 B4).
//
// PostgreSQL checks the percentage when the sample scan begins, and its
// planner never begins one under a constant-false WHERE / HAVING (a one-time
// filter) or a LIMIT 0: `… TABLESAMPLE BERNOULLI (101) WHERE false` answers
// no rows there. A scan whose rows can reach the answer keeps its sampler.
func TestDropUnbegunSamples(t *testing.T) {
	prev := tablesampleEvaluator
	t.Cleanup(func() { SetTablesampleEvaluator(prev) })
	SetTablesampleEvaluator(func(arg plansql.Node, _ ...expr.CompileOption) (float64, bool, error) {
		f, err := strconv.ParseFloat(arg.String(), 64)
		return f, false, err
	})
	for _, c := range []struct {
		sql  string
		kept int // sampled scans left in the optimized plan
	}{
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE false", 0},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE 1 = 2", 0},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE NULL", 0},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE id > 0 AND false", 0},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) LIMIT 0", 0},
		{"SELECT count(*) FROM t TABLESAMPLE BERNOULLI (101) HAVING false", 0},
		{"SELECT count(*) FROM (SELECT * FROM t TABLESAMPLE BERNOULLI (101) LIMIT 0) s", 0},
		{"SELECT * FROM u WHERE false UNION ALL SELECT * FROM t TABLESAMPLE BERNOULLI (101) WHERE false", 0},
		// The scan begins: its rows reach the answer.
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101)", 1},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE id < 0", 1},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE true", 1},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) LIMIT 1", 1},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE random() < 0", 1},
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (101) WHERE 1/0 = 1", 1},
	} {
		plan := planFor(t, c.sql)
		if got, all := sampledScans(plan); got != c.kept || all == 0 {
			t.Errorf("%s: %d sampled scans of %d, want %d\n%s", c.sql, got, all, c.kept, plan.PrettyPrint(0))
		}
	}
}
