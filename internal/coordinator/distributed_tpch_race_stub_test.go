//go:build race

// distributed_tpch_test.go is gated with //go:build !race because the
// embedded nats-server v2.12.6 has a known upstream data race that fires
// in JetStream's concurrent CreateOrUpdateConsumer / Fetch / Ack path
// (see the header comment in that file). The helper
// setupTPCHDistributedAtScale lives there, so consumer test files
// (dynamic_filter_e2e_test.go, q07_sf10_repro_test.go) lose their
// symbol under -race builds.
//
// This stub keeps those consumer tests compiling under -race by
// declaring the symbol; at runtime they t.Skip before exercising the
// racy NATS path. Remove this file once nats-server is upgraded past
// the fix and the !race gate is removed from distributed_tpch_test.go.

package coordinator

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

func setupTPCHDistributedAtScale(t *testing.T, _ tpch.ScaleFactor) (context.Context, *Coordinator) {
	t.Helper()
	t.Skip("setupTPCHDistributedAtScale unavailable under -race (nats-server v2.12.6 upstream race)")
	return nil, nil
}
