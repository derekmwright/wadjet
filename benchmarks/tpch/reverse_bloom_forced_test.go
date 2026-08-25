package tpch

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestTPCHReverseBloomForcedSF001 runs the whole correctness corpus with both
// reverse-bloom thresholds forced to 100, against the DuckDB ground truth.
//
// In production the optimization needs a build-side estimate over 10M rows
// (50M for inner joins), which no corpus CI can afford to materialize — so
// every gate in this repo answered TPC-H with the optimization switched off by
// arithmetic, and the path shipped untested by all of them. Forcing the
// thresholds down costs about two seconds and puts 30-odd reverse blooms into
// the same 22 queries.
//
// It fails on the parent of #543's fix, on Q21: that query's probeKey arrives
// alias-qualified as "l1.l_orderkey" while the batches carry "l_orderkey", the
// insert side found no column, and the EMPTY bloom was installed anyway and
// rejected every build row — 0 rows where the answer is 1. (The alias never
// resolving is a separate, still-open defect; what this gate holds is that a
// bloom which received no keys is not installed.)
//
// The comparison is the fingerprint gate's, not a row count: a bloom that
// drops SOME rows is the failure this whole area is about, and a count can
// miss that where a digest cannot.
func TestTPCHReverseBloomForcedSF001(t *testing.T) {
	truth, err := GroundTruthSF001()
	if err != nil {
		t.Fatalf("SF0.01 ground truth: %v", err)
	}
	corpus := CorrectnessQueries(SF001)
	if err := truth.CheckCoversCorpus(corpus); err != nil {
		t.Fatalf("fingerprint-sf001.json: %v", err)
	}

	prevSemi, prevInner := physical.ReverseBloomThreshold, physical.ReverseBloomInnerThreshold
	physical.ReverseBloomThreshold = 100
	physical.ReverseBloomInnerThreshold = 100
	t.Cleanup(func() {
		physical.ReverseBloomThreshold = prevSemi
		physical.ReverseBloomInnerThreshold = prevInner
	})

	installedBefore := physical.ReverseBloomsInstalled.Load()
	selfCheckBefore := exec.BloomSelfCheckFailures.Load()
	mismatchBefore := exec.BloomKeyTypeMismatches.Load()

	ctx := context.Background()
	db := ingestDuckDBFixture(t, ctx, duckdbFixtureRows(t))
	for _, q := range corpus {
		t.Run(q.Name, func(t *testing.T) {
			res, err := db.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("query failed: %v\n%s", err, q.SQL)
			}
			sig := SignatureOf(&oracle.Result{Columns: res.Columns, Rows: res.Rows}, q)
			want := truth.Queries[q.Name]
			if ok, detail := want.Match(sig); !ok {
				t.Errorf("diverges from %s %s over %s with the reverse bloom forced on: %s",
					want.Engine, want.EngineVersion, want.Dataset, detail)
			}
		})
	}

	installed := physical.ReverseBloomsInstalled.Load() - installedBefore
	if installed == 0 {
		t.Fatal("no reverse bloom was installed by any query — the thresholds no longer force the path, " +
			"and this gate is comparing the same code twice")
	}
	t.Logf("reverse blooms installed across the corpus: %d", installed)

	// A filter that failed its own self-check, or one disengaged for a key
	// type it could not read, means the query above answered correctly only
	// because the guard caught it. Correct, and still a defect.
	if n := exec.BloomSelfCheckFailures.Load() - selfCheckBefore; n != 0 {
		t.Errorf("%d bloom self-check failures across the corpus", n)
	}
	if n := exec.BloomKeyTypeMismatches.Load() - mismatchBefore; n != 0 {
		t.Errorf("%d bloom key-type mismatches across the corpus", n)
	}
}
