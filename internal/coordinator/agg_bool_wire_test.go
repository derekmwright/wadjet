package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// TestDistributedBoolAndEmptyIdentitySurvivesWire is the #354 regression.
//
// distributed.AggSpec.OutputType used to be a plain int with zero meaning
// "the planner did not declare one" — but parquet.TypeID's own zero value
// is TypeBool, so a genuinely declared BOOL_AND output (TypeBool == 0) was
// indistinguishable from an undeclared type. The identity-row gate in
// executor_fragment.go read that as "undeclared" and declined to emit the
// row SQL owes an ungrouped aggregate over an empty input, so the query
// silently lost its one row instead of answering NULL.
//
// orders is loaded at SF0.01 (15000 rows) split across 8 chunks by
// setupTPCHDistributedAtScale, so the scan+partial-aggregate fan-out runs
// several partial tasks feeding the join before the ungrouped final merges
// them — the shape the declared type has to survive the wire through, not
// just a single-task construction.
//
// The empty input has to come from a JOIN that matches nothing, not a
// WHERE-filtered scan: a scan+partial-aggregate task still runs a real
// ungrouped-aggregate kernel over its (filtered-to-zero) rows and legally
// emits its own one-row identity by itself, writing a non-empty output
// file that never touches the len(InputFiles)==0 short-circuit this bug
// lives in. A join with an unmatchable key predicate produces a
// genuinely empty result with NO output files, which is what makes the
// downstream Singleton final_aggregate hit that short-circuit and need
// AggSpec.OutputType to know it may emit the identity row (exactly #329's
// original trigger, "upstream join matched nothing").
func TestDistributedBoolAndEmptyIdentitySurvivesWire(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	res, err := coord.ExecuteSQL(ctx,
		"SELECT BOOL_AND(o_totalprice > 0) AS a0 FROM orders JOIN customer ON o_custkey = c_custkey AND c_custkey < 0")
	if err != nil {
		t.Fatalf("query failed (was: BOOL_AND's declared type read as undeclared, identity row dropped): %v", err)
	}
	if res.Error != "" {
		t.Fatalf("query failed (was: BOOL_AND's declared type read as undeclared, identity row dropped): %s", res.Error)
	}
	rows := mustRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 — an ungrouped BOOL_AND owes SQL one NULL row over the empty set", len(rows))
	}
	if v, ok := rows[0]["a0"]; ok && v != nil {
		t.Errorf("a0 = %v (%T), want NULL — BOOL_AND over zero input", v, v)
	}
}

// TestDistributedBoolOrEmptyIdentitySurvivesWire is the same shape for
// BOOL_OR, pinning that the fix is Func-name-driven (min/max/min_by/max_by
// are the only functions where a wire OutputType of TypeBool can mean
// "undeclared" — see wireAggSpecs) rather than accidentally special-cased
// to bool_and alone.
func TestDistributedBoolOrEmptyIdentitySurvivesWire(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	res, err := coord.ExecuteSQL(ctx,
		"SELECT BOOL_OR(o_totalprice > 0) AS a0 FROM orders JOIN customer ON o_custkey = c_custkey AND c_custkey < 0")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("query failed: %s", res.Error)
	}
	rows := mustRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 — an ungrouped BOOL_OR owes SQL one NULL row over the empty set", len(rows))
	}
	if v, ok := rows[0]["a0"]; ok && v != nil {
		t.Errorf("a0 = %v (%T), want NULL — BOOL_OR over zero input", v, v)
	}
}
