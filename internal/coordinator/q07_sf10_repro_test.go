//go:build !race

package coordinator

import (
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

// TestQ07SF01InProcess attempts to reproduce the EC2 SF10 stall on
// final_aggregate-14 in-process at SF0.1 distributed (3 in-process workers
// + embedded NATS + MemStore). The deployed log showed final_aggregate-14
// was dispatched via gRPC but never reported task_progress — 13+ min
// silent hang. If this test reproduces, we have a fast local repro to
// bisect against; if it passes, the bug is SF10-scale / data-plane-gRPC /
// real-S3 specific.
func TestQ07SF01InProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in-process Q07 SF0.1 (heavy: ~600K lineitem rows)")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF01)
	q07 := tpch.TPCHQueries[7]

	res, err := coord.ExecuteSQL(ctx, q07.SQL)
	if err != nil {
		t.Fatalf("Q07 ExecuteSQL: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("Q07 error: %s", res.Error)
	}
	rows := res.Rows()
	t.Logf("Q07 in-process distributed SF0.1: %d rows", len(rows))
	if len(rows) == 0 {
		t.Errorf("Q07 returned 0 rows; expected at least one (FRANCE/GERMANY pairs by year)")
	}
}
