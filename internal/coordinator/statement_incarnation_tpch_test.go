//go:build !race

// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestTPCHUnderTheIncarnationCheck: the 22 TPC-H statements on the
// coordinator (no authorization provider, as setupTPCHDistributed builds it),
// on the fast path and on the stage DAG, every one answered, none refused
// 40001 by the table-incarnation check (ADR-0050 §3, amended), and the
// local-route counters are logged so base and tip can be compared.
//
// WADJET_INC_TPCH_REPS=n (default 1) repeats each statement n times and
// logs the median planning time (statement accepted to its plan verified,
// the steps every arm shares) and the median ExecuteSQL wall time.
func TestTPCHUnderTheIncarnationCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster and TPC-H SF0.01")
	}
	ctx, coord := setupTPCHDistributed(t)
	reps := 1
	if v := os.Getenv("WADJET_INC_TPCH_REPS"); v != "" {
		fmt.Sscanf(v, "%d", &reps)
	}
	routes := func(c *Coordinator) int64 {
		return c.NullAwareAntiLocalRoutes() + c.TableLessLocalRoutes() + c.UnbuildableStageLocalRoutes() +
			c.UnreachableOutputLocalRoutes() + c.LateralProjectionLocalRoutes() + c.ScalarProjectionLocalRoutes() +
			c.InSubqueryLocalRoutes() + c.CorrelatedLocalRoutes() + c.DistinctLocalRoutes() +
			c.GroupingSetsLocalRoutes() + c.GroupKeyLocalRoutes() + c.ResidualSidesLocalRoutes() +
			c.PolicedWindowLocalRoutes() + c.WindowOverLateralLocalRoutes() + c.LateralIdentityLocalRoutes()
	}
	median := func(d []time.Duration) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		return d[len(d)/2]
	}
	for _, arm := range []struct {
		name  string
		bytes int64
	}{{"fastpath", DefaultLocalFastPathBytes}, {"dag", 0}} {
		coord.config.LocalFastPathBytes = arm.bytes
		r0, b0 := routes(coord), coord.LocalFastPathBails()
		var sumPlan, sumWall time.Duration
		for q := 1; q <= 22; q++ {
			sql := tpch.TPCHQueries[q].SQL
			var plans, walls []time.Duration
			for i := 0; i < reps; i++ {
				var accepted, verified time.Time
				statementStepTestHook = func(s string) {
					switch s {
					case "accepted":
						accepted = time.Now()
					case "verified":
						verified = time.Now()
					}
				}
				start := time.Now()
				res, err := coord.ExecuteSQL(ctx, sql)
				wall := time.Since(start)
				statementStepTestHook = nil
				if err == nil && res != nil && res.Error != "" {
					err = fmt.Errorf("%s", res.Error)
				}
				if err != nil {
					if sqlerr.StateOf(err) == "40001" || strings.Contains(err.Error(), "re-created under this statement") {
						t.Fatalf("%s Q%02d refused by the incarnation check: %v", arm.name, q, err)
					}
					t.Fatalf("%s Q%02d: %v", arm.name, q, err)
				}
				_ = res.Close()
				plans = append(plans, verified.Sub(accepted))
				walls = append(walls, wall)
			}
			p, w := median(plans), median(walls)
			sumPlan += p
			sumWall += w
			t.Logf("TPCH %s Q%02d plan=%v wall=%v", arm.name, q, p, w)
		}
		t.Logf("TPCH %s total plan=%v wall=%v routes=%d bails=%d", arm.name, sumPlan, sumWall,
			routes(coord)-r0, coord.LocalFastPathBails()-b0)
	}
}
