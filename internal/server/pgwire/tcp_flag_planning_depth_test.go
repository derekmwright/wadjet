// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"
)

// This records #1034's end-to-end depth probe, with a same-run control: the
// values must agree at every depth, and each depth's elapsed time is logged.
//
// Depth 16 is also BOUNDED. The declaration walks were exponential in
// derived-table depth (2^depth at v0.25.3, ~5.5^depth after b69c2412 made the
// publishing walks read childDecls: 248 s at depth 16), and a per-walk memo
// makes them linear (declWalk) — milliseconds here. planningDepthBound is two
// orders of magnitude above that, so the bound pins the memo rather than the
// machine, and still fails the exponential walk by minutes.
const planningDepthBound = 2 * time.Second

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
				if depth == 16 && elapsed > planningDepthBound {
					t.Errorf("depth %d (scalar=%t) took %s, want < %s: the declaration walks are not linear in depth",
						depth, scalar, elapsed, planningDepthBound)
				}
			})
		}
	}
}
