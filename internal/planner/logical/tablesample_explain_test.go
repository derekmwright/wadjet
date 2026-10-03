// SPDX-License-Identifier: MIT

package logical

import (
	"strings"
	"testing"
)

// EXPLAIN's scan line shows the sample (#1411 review r1 N3): the method and
// the argument as the real the scan samples with — PostgreSQL prints
// `Sampling: bernoulli ('50'::real)` — or NULL. It showed `Scan: t`.
func TestScanLineShowsTablesample(t *testing.T) {
	for _, c := range []struct {
		node Node
		want string
	}{
		{Node{Type: NodeScan, TableName: "t", SampleMethod: "BERNOULLI", SamplePercent: 50},
			"Scan: t TABLESAMPLE BERNOULLI (50)"},
		{Node{Type: NodeScan, TableName: "t", TableAlias: "s", SampleMethod: "SYSTEM", SamplePercent: float64(float32(12.5))},
			"Scan: t AS s TABLESAMPLE SYSTEM (12.5)"},
		{Node{Type: NodeScan, TableName: "t", SampleMethod: "BERNOULLI", SamplePercent: float64(float32(100.000001))},
			"Scan: t TABLESAMPLE BERNOULLI (100)"},
		{Node{Type: NodeScan, TableName: "t", SampleMethod: "BERNOULLI", SampleNull: true},
			"Scan: t TABLESAMPLE BERNOULLI (NULL)"},
		{Node{Type: NodeScan, TableName: "t"}, "Scan: t"},
	} {
		if got := strings.TrimSpace(c.node.PrettyPrint(0)); got != c.want {
			t.Errorf("PrettyPrint = %q, want %q", got, c.want)
		}
	}
}
