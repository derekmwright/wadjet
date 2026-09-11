package pgwire

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"
)

// This records #1034's end-to-end depth probe, with a same-run control. Timing
// is evidence, not a machine-dependent pass threshold; values must agree.
func TestTCPFlagPlanningDepth(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	for _, depth := range []int{4, 8, 12, 14, 16} {
		for _, scalar := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth%d/scalar%t", depth, scalar), func(t *testing.T) {
				q := `SELECT BITWISE_AND(id,3) AS v FROM users`
				if scalar {
					q = `SELECT (SELECT BITWISE_AND(u.id,3) FROM users u WHERE u.id=users.id) AS v FROM users`
				}
				for i := 0; i < depth; i++ {
					q = "SELECT v FROM (" + q + ") d"
				}
				start := time.Now()
				r := conn.ExecParams(context.Background(), q, nil, nil, nil, []int16{0}).Read()
				elapsed := time.Since(start)
				if r.Err != nil {
					t.Fatal(r.Err)
				}
				values := []string{}
				for _, row := range r.Rows {
					values = append(values, string(row[0]))
				}
				sort.Strings(values)
				if fmt.Sprint(values) != "[1 2 3]" {
					t.Fatalf("values=%v", values)
				}
				t.Logf("depth=%d scalar=%t elapsed=%s", depth, scalar, elapsed)
			})
		}
	}
}
