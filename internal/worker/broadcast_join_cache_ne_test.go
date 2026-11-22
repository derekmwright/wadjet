// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The `<>` semi/anti distinct-pair build is enabled from BOTH declared value
// types (exec.HashJoin.semiAntiNEEligible, #1681 line 1681c), so the probe
// declaration decides the cached build's representation. Two broadcast probes
// in one query over the same build files, keys and residual — a stage's
// declared primary join and an absorbed fused join, which has no probe
// declaration — must therefore not share one cached build: each must run on
// the build it would have constructed alone.
//
// Fixture: build (id, val) = (1,3) (2,3) (2,4) (3,5); `EXISTS (... id = id
// AND pv <> val)`. For a float probe (1,3.0) (1,3.5) (2,3.0) (3,5.0) (4,1.0)
// PostgreSQL 17.11 compares int8 with float8 in float8: rows 2 and 3 match
// (3 <> 3.0 is false, 3 <> 3.5 true, {3,4} <> 3.0 true, 5 <> 5.0 false, no
// key 4), so the semi join answers 2 rows. The integer probe (1,3) (1,4)
// (2,3) (3,5) also answers 2 rows.
func TestBroadcastJoinCacheKeyCarriesTheDistinctPairDecision(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-bcast-ne-key"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	data := makeBuildWshf(t, [][2]int64{{1, 3}, {2, 3}, {2, 4}, {3, 5}})
	const buildKey = "in/bcast-ne/build-0.wshf"
	if _, err := store.Put(ctx, bucket, buildKey, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	intProbeDecl := []distributed.ColumnSpec{
		{Name: "id", Type: int(parquet.TypeInt64)},
		{Name: "pv", Type: int(parquet.TypeInt64)},
	}
	floatProbeDecl := []distributed.ColumnSpec{
		{Name: "id", Type: int(parquet.TypeInt64)},
		{Name: "pv", Type: int(parquet.TypeFloat64)},
	}
	spec := func(probeDecl []distributed.ColumnSpec) distributed.OpSpec {
		return distributed.OpSpec{
			Type:        distributed.OpBroadcastProbe,
			JoinType:    "semi",
			LeftKeys:    []string{"id"},
			RightKeys:   []string{"id"},
			BuildAlias:  "build",
			BuildFiles:  []string{buildKey},
			BuildBucket: bucket,
			JoinFilter:  "pv <> val",
			BuildSchema: []distributed.ColumnSpec{
				{Name: "id", Type: int(parquet.TypeInt64)},
				{Name: "val", Type: int(parquet.TypeInt64)},
			},
			ProbeSchema: probeDecl,
		}
	}
	primary := spec(intProbeDecl)     // a stage's primary join: declared, eligible
	absorbed := spec(nil)             // a fused / chained join: no probe declaration
	floatDecl := spec(floatProbeDecl) // declared, but the pair is not exact

	probeSchema := func(pt parquet.TypeID) []parquet.Column {
		return []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64, Nullable: true},
			{Name: "pv", Type: pt, Nullable: true},
		}
	}
	floatProbe := func() *batch.RecordBatch {
		ids := []int64{1, 1, 2, 3, 4}
		pvs := []float64{3.0, 3.5, 3.0, 5.0, 1.0}
		b := batch.NewRecordBatch(probeSchema(parquet.TypeFloat64), len(ids))
		for i := range ids {
			b.Columns[0].Int64Data[i] = ids[i]
			b.Columns[0].Nulls.SetValid(i)
			b.Columns[1].Float64Data[i] = pvs[i]
			b.Columns[1].Nulls.SetValid(i)
		}
		return b
	}
	intProbe := func() *batch.RecordBatch {
		ids := []int64{1, 1, 2, 3}
		pvs := []int64{3, 4, 3, 5}
		b := batch.NewRecordBatch(probeSchema(parquet.TypeInt64), len(ids))
		for i := range ids {
			b.Columns[0].Int64Data[i] = ids[i]
			b.Columns[0].Nulls.SetValid(i)
			b.Columns[1].Int64Data[i] = pvs[i]
			b.Columns[1].Nulls.SetValid(i)
		}
		return b
	}

	task := distributed.Task{ID: "t-bcast-ne", QueryID: "q-bcast-ne", DataBucket: bucket, ResultBucket: bucket}

	// run builds `first` and holds it (as a fragment holds every op it built
	// until the task ends), then builds `second` and probes it with in.
	type outcome struct {
		rows        int
		err         error
		entries     int  // cache entries while both are held
		secondNE    bool // the representation `second` probes against
		secondAlone bool // the representation `second` builds on its own
	}
	run := func(first, second distributed.OpSpec, in *batch.RecordBatch) outcome {
		t.Helper()
		ex := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		_, cleanFirst, err := ex.buildFragmentJoinProbe(ctx, task, first)
		if err != nil {
			t.Fatalf("first build: %v", err)
		}
		defer cleanFirst()
		ops, cleanSecond, err := ex.buildFragmentJoinProbe(ctx, task, second)
		if err != nil {
			t.Fatalf("second build: %v", err)
		}
		defer cleanSecond()
		var o outcome
		o.entries = ex.broadcastCache.Count()
		ex.broadcastCache.mu.Lock()
		if e, ok := ex.broadcastCache.entries[computeBroadcastJoinKey(task.QueryID, second)]; ok && e.hj != nil {
			o.secondNE = e.hj.NEActive()
		}
		ex.broadcastCache.mu.Unlock()

		alone := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
		_, cleanAlone, err := alone.buildFragmentJoinProbe(ctx, task, second)
		if err != nil {
			t.Fatalf("second built alone: %v", err)
		}
		alone.broadcastCache.mu.Lock()
		for _, e := range alone.broadcastCache.entries {
			o.secondAlone = e.hj != nil && e.hj.NEActive()
		}
		alone.broadcastCache.mu.Unlock()
		cleanAlone()

		probe := ops[0]
		if err := probe.Init(ctx); err != nil {
			t.Fatalf("probe init: %v", err)
		}
		out, err := probe.Execute(ctx, in)
		if err != nil {
			o.err = err
			return o
		}
		if out != nil {
			o.rows = out.ActiveLen()
		}
		return o
	}

	cases := []struct {
		name          string
		first, second distributed.OpSpec
		in            func() *batch.RecordBatch
	}{
		// The absorbed join's probe stream carries a float value; alone it
		// takes the generic filtered build and answers 2 rows.
		{"declared-primary-then-absorbed-float-probe", primary, absorbed, floatProbe},
		// Both declared, different probe value types.
		{"declared-int-then-declared-float", primary, floatDecl, floatProbe},
		// The absorbed join built first: the declared primary join must
		// still get the distinct-pair build it would build alone.
		{"absorbed-then-declared-primary", absorbed, primary, intProbe},
		// The primary built first: the absorbed join must not probe a
		// distinct-pair build it would not have built.
		{"declared-primary-then-absorbed-int-probe", primary, absorbed, intProbe},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := run(tc.first, tc.second, tc.in())
			if o.err != nil {
				t.Errorf("probe refused: %v (cache entries while both held: %d; the second join shares the first's build)", o.err, o.entries)
			} else if o.rows != 2 {
				t.Errorf("rows = %d, want 2 (PostgreSQL 17.11)", o.rows)
			}
			if o.secondNE != o.secondAlone {
				t.Errorf("the second join probes a build with distinct pair=%v, where alone it builds distinct pair=%v (cache entries while both held: %d)",
					o.secondNE, o.secondAlone, o.entries)
			}
			if o.entries != 2 {
				t.Errorf("cache entries while both held = %d, want 2: two joins whose builds differ share one key", o.entries)
			}
		})
	}

	// Joins that build the same thing still share: two probe tasks of one
	// declared join, and two absorbed joins.
	for _, s := range []distributed.OpSpec{primary, absorbed} {
		if computeBroadcastJoinKey("q", s) != computeBroadcastJoinKey("q", s) {
			t.Errorf("identical specs produced different keys")
		}
	}
	if computeBroadcastJoinKey("q", primary) == computeBroadcastJoinKey("q", absorbed) {
		t.Errorf("a declared and an undeclared probe share a key")
	}
}
