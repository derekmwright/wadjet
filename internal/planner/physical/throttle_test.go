package physical

import "testing"

func TestAnnotateMaxConcurrent_NoBudgetNoAnnotation(t *testing.T) {
	stages := []Stage{
		{ID: "scan-l", Type: StageScan, EstimatedBytes: 100 << 30}, // 100 GiB scan
		{ID: "join-1", Type: StageHashJoin, RightDepStage: "scan-l"},
	}
	annotateMaxConcurrent(stages, 0)
	if got := stages[1].MaxConcurrentPerWorker; got != 0 {
		t.Errorf("zero budget should skip annotation entirely; got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_HeavyBuildGetsCap(t *testing.T) {
	// SF100-ish setup: worker pool budget 14 GiB, lineitem scan 80 GiB.
	// Expected: cap = floor(usable / build) = floor(0.75 * 14 / 80) ~= 0 → 1.
	stages := []Stage{
		{ID: "scan-l", Type: StageScan, EstimatedBytes: 80 << 30},
		{ID: "join-1", Type: StageHashJoin, RightDepStage: "scan-l"},
	}
	annotateMaxConcurrent(stages, 14<<30)
	if got := stages[1].MaxConcurrentPerWorker; got != 1 {
		t.Errorf("SF100 lineitem build should cap at 1; got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_MediumBuildCapsAtTwo(t *testing.T) {
	// Build ~4 GiB; usable = 0.75 * 14 GiB = 10.5 GiB; cap = floor(10.5/4) = 2.
	stages := []Stage{
		{ID: "scan-x", Type: StageScan, EstimatedBytes: 4 << 30},
		{ID: "join-2", Type: StageHashJoin, RightDepStage: "scan-x"},
	}
	annotateMaxConcurrent(stages, 14<<30)
	if got := stages[1].MaxConcurrentPerWorker; got != 2 {
		t.Errorf("mid-size build should cap at 2; got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_SmallBuildNoCap(t *testing.T) {
	// 100 MB scan — well below the 512 MB activation threshold.
	stages := []Stage{
		{ID: "scan-n", Type: StageScan, EstimatedBytes: 100 << 20},
		{ID: "join-n", Type: StageHashJoin, RightDepStage: "scan-n"},
	}
	annotateMaxConcurrent(stages, 14<<30)
	if got := stages[2-1].MaxConcurrentPerWorker; got != 0 {
		t.Errorf("small build below activation threshold should not be annotated; got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_NonJoinStagesUntouched(t *testing.T) {
	// Stages other than HashJoin/BroadcastJoin must never get a cap,
	// even if their EstimatedBytes is large.
	stages := []Stage{
		{ID: "scan-l", Type: StageScan, EstimatedBytes: 80 << 30},
		{ID: "agg-l", Type: "aggregate", EstimatedBytes: 80 << 30},
		{ID: "sort-l", Type: "sort", EstimatedBytes: 80 << 30},
	}
	annotateMaxConcurrent(stages, 14<<30)
	for _, s := range stages {
		if s.MaxConcurrentPerWorker != 0 {
			t.Errorf("non-join stage %q (Type=%q) should not be annotated; got cap=%d",
				s.ID, s.Type, s.MaxConcurrentPerWorker)
		}
	}
}

func TestAnnotateMaxConcurrent_BroadcastJoinViaExchangeReplicate(t *testing.T) {
	// broadcast_join → exchange-replicate → scan: the walker has to
	// hop through exchange-replicate to find the build leaf.
	stages := []Stage{
		{ID: "scan-b", Type: StageScan, EstimatedBytes: 6 << 30},
		{ID: "repl-b", Type: StageExchangeReplicate, Dependencies: []string{"scan-b"}},
		{ID: "join-b", Type: StageBroadcastJoin, RightDepStage: "repl-b"},
	}
	annotateMaxConcurrent(stages, 14<<30)
	if got := stages[2].MaxConcurrentPerWorker; got != 1 {
		t.Errorf("broadcast join with 6GB build should cap at 1 (0.75*14/6=1.75 → 1); got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_DeepBuildChain(t *testing.T) {
	// hash_join → exchange-repartition → aggregate → scan: walker
	// follows Dependencies through three hops to find the leaf scan.
	// This matches Q17's build-side shape (partial agg on lineitem
	// before the shuffle-then-join).
	stages := []Stage{
		{ID: "scan-l", Type: StageScan, EstimatedBytes: 80 << 30},
		{ID: "agg-l", Type: "aggregate", Dependencies: []string{"scan-l"}},
		{ID: "rp-l", Type: StageExchangeRepartition, Dependencies: []string{"agg-l"}},
		{ID: "join-l", Type: StageHashJoin, RightDepStage: "rp-l"},
	}
	annotateMaxConcurrent(stages, 14<<30)
	if got := stages[3].MaxConcurrentPerWorker; got != 1 {
		t.Errorf("deep build chain (agg+repartition) should still find scan and cap at 1; got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_NoEstimateFailOpen(t *testing.T) {
	// Build chain reaches a scan with EstimatedBytes=0 → no info → don't
	// annotate. Fail-closed against guesswork prevents over-throttling
	// on plan shapes the estimator doesn't model.
	stages := []Stage{
		{ID: "scan-z", Type: StageScan, EstimatedBytes: 0},
		{ID: "join-z", Type: StageHashJoin, RightDepStage: "scan-z"},
	}
	annotateMaxConcurrent(stages, 14<<30)
	if got := stages[1].MaxConcurrentPerWorker; got != 0 {
		t.Errorf("missing build estimate should not annotate; got cap=%d", got)
	}
}

func TestAnnotateMaxConcurrent_HighCapNoOp(t *testing.T) {
	// Tiny build vs. huge budget → cap would be huge → we skip
	// annotation entirely (the worker's own MaxConcurrent already
	// bounds it). 1 GB build with 64 GB budget → cap = 48 → skip.
	stages := []Stage{
		{ID: "scan-s", Type: StageScan, EstimatedBytes: 1 << 30},
		{ID: "join-s", Type: StageHashJoin, RightDepStage: "scan-s"},
	}
	annotateMaxConcurrent(stages, 64<<30)
	if got := stages[1].MaxConcurrentPerWorker; got != 0 {
		t.Errorf("cap >= worker default (4) should be a no-op; got cap=%d", got)
	}
}
