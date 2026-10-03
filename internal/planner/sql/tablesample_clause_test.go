// SPDX-License-Identifier: MIT

package sql

import "testing"

func TestHasTablesampleReadsTokens(t *testing.T) {
	for _, c := range []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM t TABLESAMPLE BERNOULLI (50)", true},
		{"SELECT * FROM t tablesample system (0)", true},
		{"SELECT * FROM t WHERE id IN (SELECT id FROM k TABLESAMPLE BERNOULLI (50))", true},
		{"WITH c AS (SELECT * FROM k TableSample BERNOULLI (5)) SELECT * FROM c", true},
		{"SELECT 'TABLESAMPLE' FROM t", false},
		{`SELECT "tablesample" FROM t`, false},
		{"SELECT * FROM t -- TABLESAMPLE BERNOULLI (50)", false},
		{"SELECT * FROM t", false},
	} {
		if got := HasTablesample(c.sql); got != c.want {
			t.Errorf("HasTablesample(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}
