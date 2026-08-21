package tpch

import (
	"context"
	"testing"
)

// Independent review probe: set semantics on the DAG vs DuckDB truth.
func TestZZSetOpProbe(t *testing.T) {
	ctx := context.Background()
	_, dag := setupTwoPathCluster(t, ctx)
	qs := []struct{ name, sql string }{
		{"intersect", `SELECT n_regionkey FROM nation WHERE n_nationkey < 10 INTERSECT SELECT n_regionkey FROM nation WHERE n_nationkey >= 10`},
		{"intersect all", `SELECT n_regionkey FROM nation WHERE n_nationkey < 10 INTERSECT ALL SELECT n_regionkey FROM nation WHERE n_nationkey >= 10`},
		{"except", `SELECT n_regionkey FROM nation EXCEPT SELECT n_regionkey FROM nation WHERE n_regionkey < 3`},
		{"except all dup math", `SELECT n_regionkey FROM nation EXCEPT ALL SELECT n_regionkey FROM nation WHERE n_nationkey < 12`},
		{"empty arm intersect", `SELECT n_regionkey FROM nation INTERSECT SELECT n_regionkey FROM nation WHERE 1=0`},
		{"null membership", `SELECT NULLIF(n_regionkey,1) FROM nation INTERSECT SELECT NULLIF(n_regionkey,1) FROM nation WHERE n_regionkey = 1`},
	}
	for _, q := range qs {
		r, _, err := runArm(t, ctx, dag, q.sql)
		if err != nil {
			t.Errorf("%-22s DAG ERR %v", q.name, err)
			continue
		}
		t.Logf("%-22s DAG rows=%d %v", q.name, len(r), firstN(r, 6))
	}
}

func firstN(r []map[string]any, n int) []map[string]any {
	if len(r) < n {
		return r
	}
	return r[:n]
}
