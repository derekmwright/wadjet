// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestArcPSDisplayScalePlanningDepth: a set operation of a COALESCE over a
// NUMERIC(10,2) column and a literal arm spelled with trailing zeros, under
// 4 … 16 derived tables, prints each value at its own display scale
// (ADR-0024 §1 as amended) at every depth, and depth 16 plans and answers
// under planningDepthBound — the display scale rides the vector, so no plan
// walk grew with it.
func TestArcPSDisplayScalePlanningDepth(t *testing.T) {
	srv := setupSSAuditWireDB(t)
	conn := connectPgconn(t, srv.Addr())
	want := "[-3.50 0.00 0.01 1.5 10.00 2.25 2.500]"
	for _, depth := range []int{4, 8, 12, 14, 16} {
		t.Run(fmt.Sprintf("depth%d", depth), func(t *testing.T) {
			q := `SELECT COALESCE(n, 1.5) AS k FROM ss_t UNION ALL SELECT 2.500 FROM ss_i WHERE id = 1`
			for i := 0; i < depth; i++ {
				q = "SELECT k FROM (" + q + ") d"
			}
			start := time.Now()
			r := conn.ExecParams(context.Background(), q, nil, nil, nil, []int16{0}).Read()
			elapsed := time.Since(start)
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			var values []string
			for _, row := range r.Rows {
				values = append(values, string(row[0]))
			}
			sort.Strings(values)
			if got := fmt.Sprint(values); got != want {
				t.Errorf("depth %d prints %s, want %s (PostgreSQL 17.11)", depth, strings.TrimSpace(got), want)
			}
			t.Logf("depth=%d elapsed=%s", depth, elapsed)
			if depth == 16 && elapsed > planningDepthBound {
				t.Errorf("depth 16 took %s, want < %s", elapsed, planningDepthBound)
			}
		})
	}
}
