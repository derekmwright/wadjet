package exec

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The generic SoA path defers group-key boxing: no extras/keyValues/h.keys
// at consume, keys reconstructed from the binary serializedKeys at output
// (decodeSerializedKeyIntoColumns) and at spill-drain time
// (decodeSerializedKey via the cursor). This test pins both decode paths
// against expectations computed directly from the input, over a
// three-column mixed-type key (int64, string, int32) with NULLs in every
// key column — the shape (ClickBench Q19) that motivated the deferral.
func TestGenericSoADeferredKeyBoxing(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "k2", Type: parquet.TypeString, Nullable: true},
		{Name: "k3", Type: parquet.TypeInt32, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	rng := rand.New(rand.NewSource(7))
	const n = 30000
	rows := make([]map[string]any, n)
	type exp struct {
		cnt int64
		sum int64
		has bool
	}
	want := map[string]*exp{}
	keyOf := func(r map[string]any) string {
		return fmt.Sprintf("%v|%v|%v", r["k1"], r["k2"], r["k3"])
	}
	for i := range rows {
		r := map[string]any{}
		if rng.Intn(15) != 0 {
			r["k1"] = int64(rng.Intn(200))
		}
		if rng.Intn(15) != 0 {
			r["k2"] = fmt.Sprintf("key-%d", rng.Intn(150))
		}
		if rng.Intn(15) != 0 {
			r["k3"] = int32(rng.Intn(50))
		}
		if rng.Intn(8) != 0 {
			r["v"] = int64(rng.Intn(1000))
		}
		rows[i] = r
		e := want[keyOf(r)]
		if e == nil {
			e = &exp{}
			want[keyOf(r)] = e
		}
		if v, ok := r["v"]; ok {
			e.cnt++ // COUNT(v) counts non-null v
			e.sum += v.(int64)
			e.has = true
		}
	}

	aggs := []AggColumn{
		{Func: AggCount, InputCol: "v", OutputCol: "cnt", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
	}

	check := func(t *testing.T, agg *HashAggregate) {
		t.Helper()
		got := 0
		for {
			b, err := agg.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				key := fmt.Sprintf("%v|%v|%v", r["k1"], r["k2"], r["k3"])
				e := want[key]
				if e == nil {
					t.Fatalf("unexpected group %q: %v", key, r)
				}
				var cnt, sum int64
				if c, ok := r["cnt"].(int64); ok {
					cnt = c
				}
				switch s := r["sv"].(type) {
				case int64:
					sum = s
				case float64:
					sum = int64(s)
				}
				if cnt != e.cnt {
					t.Fatalf("group %q cnt = %d, want %d", key, cnt, e.cnt)
				}
				if e.has && sum != e.sum {
					t.Fatalf("group %q sum = %d, want %d", key, sum, e.sum)
				}
				got++
			}
		}
		if got != len(want) {
			t.Fatalf("emitted %d groups, want %d", got, len(want))
		}
	}

	t.Run("output-decode", func(t *testing.T) {
		agg := NewHashAggregate([]string{"k1", "k2", "k3"}, aggs)
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: 1}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !agg.useGenericSoA {
			t.Fatal("expected the generic SoA path")
		}
		if !agg.deferGenericKeyBoxing {
			t.Fatal("expected deferred key boxing to engage")
		}
		if len(agg.keys) != 0 {
			t.Fatalf("h.keys populated (%d) despite deferral", len(agg.keys))
		}
		check(t, agg)
	})

	// Spill-drain path, mirroring the morsel-parallel clone workflow: a
	// clone with a tiny PartialDrainBytes bound self-drains mid-consume
	// through the partial cursor — whose key arena and head values must
	// come from decodeSerializedKey for deferred groups — then MergeSink
	// hands the runs to the primary and Finalize's k-way merge recombines
	// them with the in-memory remainder.
	t.Run("spill-cursor-decode", func(t *testing.T) {
		tracker := memory.NewTracker("test", 1<<30)
		spill, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		primary := NewHashAggregate([]string{"k1", "k2", "k3"}, aggs)
		primary.Spill = spill
		ctx := context.Background()
		if err := primary.Init(ctx); err != nil {
			t.Fatal(err)
		}
		clone := primary.CloneSink().(*HashAggregate)
		clone.Spill = spill.TrackingOnlyView()
		clone.PartialDrainBytes = 32 << 10
		if err := clone.Init(ctx); err != nil {
			t.Fatal(err)
		}
		const chunk = 2000
		for base := 0; base < n; base += chunk {
			if err := clone.Consume(ctx, batch.FromRows(schema, rows[base:base+chunk])); err != nil {
				t.Fatal(err)
			}
		}
		if !clone.deferGenericKeyBoxing {
			t.Fatal("expected deferred key boxing to engage on the clone")
		}
		if len(clone.drainedRuns) < 2 {
			t.Fatalf("expected multiple self-drains, got %d", len(clone.drainedRuns))
		}
		primary.MergeSink(clone)
		if err := primary.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		check(t, primary)
	})
}
