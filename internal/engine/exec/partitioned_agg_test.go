package exec

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Partitioned parallel aggregation must produce results identical to the
// serial pipeline for every aggregate class, including NULL group keys and
// NULL inputs, across low- and high-cardinality keys. Seeded-random data;
// serial run is the oracle.
var spillOn = os.Getenv("TEST_SPILL_OFF") != "1"

// A group key the router cannot hash (DECIMAL is not in PartitionSelectors'
// supported set, and neither is a column the batch does not carry) makes
// partitionAndDeliver fall back to consuming the whole batch into whichever
// worker pulled it. That breaks the disjointness PartitionedDisjoint
// asserts, and MergeSink's adopt-don't-merge shortcut then emitted the same
// group once per worker — counts split k ways with the total still correct,
// the #338 signature. The pipeline has to notice the fallback and merge by
// key instead.
func TestPartitionedAggUnroutableKeyMergesGroups(t *testing.T) {
	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeDecimal, Precision: 10, Scale: 2},
		{Name: "val", Type: parquet.TypeInt64},
	}
	const n = 20000
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{"grp": float64(i % 3), "val": int64(1)})
	}

	run := func(workers int) map[string]int64 {
		agg := NewHashAggregate([]string{"grp"}, []AggColumn{
			{Func: AggCount, InputCol: "val", OutputCol: "cnt", OutputType: parquet.TypeInt64},
		})
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: workers}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		out := map[string]int64{}
		emitted := 0
		for {
			b, err := agg.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				emitted++
				out[fmt.Sprintf("%v", r["grp"])] += r["cnt"].(int64)
			}
		}
		if emitted != len(out) {
			t.Errorf("workers=%d emitted %d rows for %d distinct keys — a group was reported once per partial",
				workers, emitted, len(out))
		}
		return out
	}

	before := partitionFallbacks.Load()
	serial := run(1)
	parallel := run(8)
	if partitionFallbacks.Load() == before {
		t.Fatal("no routing fallback fired — this test no longer covers the unroutable-key path")
	}
	if len(parallel) != len(serial) {
		t.Fatalf("group count: parallel %d vs serial %d (%v)", len(parallel), len(serial), parallel)
	}
	var total int64
	for k, want := range serial {
		if got := parallel[k]; got != want {
			t.Errorf("group %q: parallel count %d, serial %d", k, got, want)
		}
		total += parallel[k]
	}
	if total != n {
		t.Errorf("total count %d, want %d", total, n)
	}
}

func TestPartitionedAggMatchesSerial(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "k2", Type: parquet.TypeString, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}
	rng := rand.New(rand.NewSource(42))
	const n = 40000
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{}
		if rng.Intn(20) != 0 {
			r["k1"] = int64(rng.Intn(500))
		}
		if rng.Intn(20) != 0 {
			r["k2"] = fmt.Sprintf("key-%d", rng.Intn(300))
		}
		if rng.Intn(10) != 0 {
			r["v"] = int64(rng.Intn(100000)) - 50000
		}
		if rng.Intn(10) != 0 {
			r["s"] = fmt.Sprintf("val-%04d", rng.Intn(3000))
		}
		rows[i] = r
	}

	aggsOf := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeFloat64},
			{Func: AggMin, InputCol: "v", OutputCol: "mn", OutputType: parquet.TypeFloat64},
			{Func: AggMax, InputCol: "s", OutputCol: "mxs", OutputType: parquet.TypeFloat64},
			{Func: AggCountDistinct, InputCol: "v", OutputCol: "dv", OutputType: parquet.TypeInt64},
			{Func: AggCountDistinct, InputCol: "s", OutputCol: "ds", OutputType: parquet.TypeInt64},
			{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		}
	}

	run := func(workers int, withSpill, parallelEmit bool) map[string]map[string]any {
		prev := parallelEmitToggle.Set(parallelEmit)
		defer parallelEmitToggle.Set(prev)
		agg := NewHashAggregate([]string{"k1", "k2"}, aggsOf())
		if withSpill {
			tracker := memory.NewTracker("t", 64<<20)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			agg.Spill = sm
		}
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: workers}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		out := map[string]map[string]any{}
		dups := 0
		for {
			b, err := agg.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				key := fmt.Sprintf("%v|%v", r["k1"], r["k2"])
				if prev, ok := out[key]; ok {
					dups++
					if dups <= 3 {
						t.Logf("DUP key %q: prev=%v new=%v", key, prev["cnt"], r["cnt"])
					}
				}
				out[key] = r
			}
		}
		if dups > 0 {
			t.Logf("total duplicate keys in stream: %d (workers=%d)", dups, workers)
		}
		return out
	}

	serial := run(1, false, false)

	// Both emit phases are checked against the same serial oracle: the
	// adopted partitions streamed one at a time (parallelEmit=false) and
	// fanned across one goroutine each (parallelEmit=true).
	for _, parallelEmit := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallel_emit=%v", parallelEmit), func(t *testing.T) {
			beforeParts := PartitionedAggRuns.Load()
			beforeEmit := ParallelEmitRuns.Load()
			par := run(8, spillOn, parallelEmit)
			if PartitionedAggRuns.Load() == beforeParts && partitionedAggToggle.On() {
				t.Fatal("partitioned mode did not engage")
			}
			if got := ParallelEmitRuns.Load() != beforeEmit; got != parallelEmit {
				t.Fatalf("parallel emit engaged=%v, want %v", got, parallelEmit)
			}

			t.Logf("fallback consumptions: %d", partitionFallbacks.Load())
			if len(par) != len(serial) {
				t.Fatalf("group counts differ: parallel %d vs serial %d", len(par), len(serial))
			}
			keys := make([]string, 0, len(serial))
			for k := range serial {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			bad := 0
			for _, k := range keys {
				pr, ok := par[k]
				if !ok {
					bad++
					if bad <= 5 {
						t.Errorf("group %q missing from parallel result", k)
					}
					continue
				}
				for col, sv := range serial[k] {
					if fmt.Sprintf("%v", pr[col]) != fmt.Sprintf("%v", sv) {
						bad++
						if bad <= 8 {
							t.Errorf("group %q col %s: parallel %v vs serial %v", k, col, pr[col], sv)
						}
					}
				}
			}
			if bad > 8 {
				t.Errorf("... and %d more mismatches", bad-8)
			}
		})
	}
}
