package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Aggregate-level coverage for the packed composite-key path: which shapes
// route to it, that a NULL group key still migrates the whole aggregate to
// the generic path before consumption, and that serial / partitioned /
// spilled / parallel-emit runs all agree for a packed shape.

func packedSumAggs() []AggColumn {
	return []AggColumn{
		{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		{Func: AggMin, InputCol: "v", OutputCol: "mn", OutputType: parquet.TypeInt64},
		{Func: AggMax, InputCol: "v", OutputCol: "mx", OutputType: parquet.TypeInt64},
		{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
	}
}

// TestPackedGroupKeyRouting pins which GROUP BY shapes land on which key
// path. The packed path must cover every shape the dual-int path covered
// (any two int-class columns) plus the wider ones, and must decline
// everything it cannot pack bit-exactly.
func TestPackedGroupKeyRouting(t *testing.T) {
	type expect struct {
		packed, intKey, compact, generic bool
	}
	cases := []struct {
		name    string
		cols    []parquet.Column
		groupBy []string
		want    expect
	}{
		{
			name: "two int64 (the old dual-int shape)",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt64},
				{Name: "b", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "b"},
			want:    expect{packed: true},
		},
		{
			name: "int64 + int32",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt64},
				{Name: "b", Type: parquet.TypeInt32}, {Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "b"},
			want:    expect{packed: true},
		},
		{
			name: "three int32",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt32},
				{Name: "b", Type: parquet.TypeInt32}, {Name: "c", Type: parquet.TypeInt32},
				{Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "b", "c"},
			want:    expect{packed: true},
		},
		{
			name: "four int32 = 16B exactly",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt32},
				{Name: "b", Type: parquet.TypeInt32}, {Name: "c", Type: parquet.TypeInt32},
				{Name: "d", Type: parquet.TypeInt32}, {Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "b", "c", "d"},
			want:    expect{packed: true},
		},
		{
			name: "two int64 + int32 = 20B declines to generic",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt64},
				{Name: "b", Type: parquet.TypeInt64}, {Name: "c", Type: parquet.TypeInt32},
				{Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "b", "c"},
			want:    expect{generic: true},
		},
		{
			name: "int64 + string declines to generic",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt64},
				{Name: "s", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "s"},
			want:    expect{generic: true},
		},
		{
			name: "single int column keeps the single-int path",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a"},
			want:    expect{intKey: true},
		},
		{
			name: "int32 + bool routes to compact",
			cols: []parquet.Column{{Name: "a", Type: parquet.TypeInt32},
				{Name: "b", Type: parquet.TypeBool}, {Name: "v", Type: parquet.TypeInt64}},
			groupBy: []string{"a", "b"},
			want:    expect{compact: true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHashAggregate(tc.groupBy, packedSumAggs())
			ctx := context.Background()
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			row := map[string]any{}
			for _, c := range tc.cols {
				row[c.Name] = zeroValueFor(c.Type)
			}
			if err := h.Consume(ctx, batch.FromRows(tc.cols, []map[string]any{row})); err != nil {
				t.Fatal(err)
			}
			got := expect{
				packed:  h.usePackedGroupKey,
				intKey:  h.useIntGroupKey,
				compact: h.useCompactGroupKey,
				generic: h.useGenericSoA,
			}
			if got != tc.want {
				t.Fatalf("routing = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func zeroValueFor(t parquet.TypeID) any {
	switch t {
	case parquet.TypeInt32:
		return int32(1)
	case parquet.TypeInt64:
		return int64(1)
	case parquet.TypeString:
		return "x"
	case parquet.TypeBool:
		return true
	}
	return int64(1)
}

// With the kill switch off, the widened (3+ column) shapes must fall back to
// their pre-change routing; two-column shapes are unaffected because for them
// the packed table is a pure data-structure swap of the dual-int machinery.
func TestPackedGroupKeyToggleOffRestoresOldRouting(t *testing.T) {
	prev := packedKeysToggle.Set(false)
	defer packedKeysToggle.Set(prev)

	cols := []parquet.Column{{Name: "a", Type: parquet.TypeInt32},
		{Name: "b", Type: parquet.TypeInt32}, {Name: "c", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeInt64}}
	ctx := context.Background()

	three := NewHashAggregate([]string{"a", "b", "c"}, packedSumAggs())
	if err := three.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer three.Close()
	if err := three.Consume(ctx, batch.FromRows(cols, []map[string]any{
		{"a": int32(1), "b": int32(2), "c": int32(3), "v": int64(4)},
	})); err != nil {
		t.Fatal(err)
	}
	if three.usePackedGroupKey {
		t.Fatal("3-column shape took the packed path with the switch off")
	}
	if !three.useGenericSoA {
		t.Fatal("3-column shape did not fall back to the generic SoA path")
	}

	two := NewHashAggregate([]string{"a", "b"}, packedSumAggs())
	if err := two.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	if err := two.Consume(ctx, batch.FromRows(cols, []map[string]any{
		{"a": int32(1), "b": int32(2), "c": int32(3), "v": int64(4)},
	})); err != nil {
		t.Fatal(err)
	}
	if !two.usePackedGroupKey {
		t.Fatal("2-column shape must stay on the packed table regardless of the switch")
	}
}

// The migrate-on-null mechanism (2026-06-12) must still fire for packed
// keys, and for EVERY key column — it is what makes an in-table NULL
// impossible, and therefore what lets the packing reserve no bit pattern.
func TestPackedGroupKeyNullMigration(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c", Type: parquet.TypeInt32, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	h := NewHashAggregate([]string{"a", "b", "c"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// First batch: no nulls, packed path engages.
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"a": int32(1), "b": int32(2), "c": int32(3), "v": int64(10)},
		{"a": int32(4), "b": int32(5), "c": int32(6), "v": int64(20)},
	})); err != nil {
		t.Fatal(err)
	}
	if !h.usePackedGroupKey {
		t.Fatal("expected the packed path on the first batch")
	}
	// Second batch introduces a NULL in the THIRD column: migration must fire
	// (the old dual-int check only looked at two columns), existing groups
	// must survive, and post-migration rows must merge with them.
	if err := h.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"a": int32(1), "b": int32(2), "c": nil, "v": int64(1)},
		{"a": int32(1), "b": int32(2), "c": int32(3), "v": int64(5)},
		{"a": nil, "b": nil, "c": nil, "v": int64(7)},
	})); err != nil {
		t.Fatal(err)
	}
	if h.usePackedGroupKey {
		t.Fatal("NULL group key did not migrate the aggregate off the packed path")
	}

	rows := aggRows(t, h)
	got := map[string]any{}
	for _, r := range rows {
		got[fmt.Sprintf("%v|%v|%v", r["a"], r["b"], r["c"])] = r["s"]
	}
	want := map[string]any{
		"1|2|3":             int64(15),
		"4|5|6":             int64(20),
		"1|2|<nil>":         int64(1),
		"<nil>|<nil>|<nil>": int64(7),
	}
	if len(got) != len(want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("group %s = %v, want %v", k, got[k], w)
		}
	}
}

// packedParityRows is a deterministic 3-narrow-int-column data set with a
// realistic mix of repeated and near-unique keys.
func packedParityRows(n int) ([]parquet.Column, []map[string]any) {
	schema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt32},
		{Name: "k2", Type: parquet.TypeInt32},
		{Name: "k3", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rng := rand.New(rand.NewSource(1337))
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"k1": int32(rng.Intn(97)) - 48, // spans negative values
			"k2": int32(rng.Intn(31)),
			"k3": int32(rng.Intn(500)),
			"v":  int64(rng.Intn(100000)) - 50000,
		}
	}
	return schema, rows
}

// Serial, partitioned-parallel, spilling and parallel-emit runs must all
// produce the same groups and values for a packed shape. Mirrors
// TestPartitionedAggMatchesSerial, which covers the generic path.
func TestPackedGroupKeyParityAcrossModes(t *testing.T) {
	schema, rows := packedParityRows(40000)

	run := func(workers int, withSpill, parallelEmit bool) map[string]map[string]any {
		prev := parallelEmitToggle.Set(parallelEmit)
		defer parallelEmitToggle.Set(prev)
		agg := NewHashAggregate([]string{"k1", "k2", "k3"}, packedSumAggs())
		if withSpill {
			tracker := memory.NewTracker("packed", 64<<20)
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
				if _, dup := out[key]; dup {
					t.Fatalf("duplicate group %q in emitted stream", key)
				}
				out[key] = r
			}
		}
		return out
	}

	serial := run(1, false, false)
	if len(serial) == 0 {
		t.Fatal("no groups")
	}
	for _, tc := range []struct {
		name         string
		workers      int
		spill        bool
		parallelEmit bool
	}{
		{"parallel", 8, false, false},
		{"parallel+spill", 8, true, false},
		{"parallel+emit-drain", 8, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertSameGroups(t, serial, run(tc.workers, tc.spill, tc.parallelEmit))
		})
	}
}

// The external-merge spill path for a packed shape: the drain cursor writes
// run files keyed by its own sort-key encoding, and the k-way merge must
// reconstruct exactly the in-memory answer (including the typed group
// columns it re-emits).
func TestPackedGroupKeyExternalMergeSpill(t *testing.T) {
	schema, rows := packedParityRows(8000)
	ctx := context.Background()

	batches := make([]*batch.RecordBatch, 0, 8)
	for off := 0; off < len(rows); off += 1000 {
		end := off + 1000
		if end > len(rows) {
			end = len(rows)
		}
		batches = append(batches, batch.FromRows(schema, rows[off:end]))
	}

	drain := func(h *HashAggregate) map[string]map[string]any {
		for _, b := range batches {
			if err := h.Consume(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		if err := h.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		out := map[string]map[string]any{}
		for {
			b, err := h.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				out[fmt.Sprintf("%v|%v|%v", r["k1"], r["k2"], r["k3"])] = r
			}
		}
		h.Close()
		return out
	}

	ref := drain(NewHashAggregate([]string{"k1", "k2", "k3"}, packedSumAggs()))

	tracker := memory.NewTracker("packed-spill", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	spilling := NewHashAggregate([]string{"k1", "k2", "k3"}, packedSumAggs())
	spilling.Spill = sm
	tracker.ForceReserve(900)
	got := drain(spilling)

	assertSameGroups(t, ref, got)
}

// Adopted-partition drain (the G4 parallel emit) over a packed shape: the
// per-unit emission unpacks composite keys concurrently, and every unit's
// rows must match the serial drain.
func TestPackedGroupKeyAdoptedPartitionDrain(t *testing.T) {
	schema, rows := packedParityRows(20000)
	ctx := context.Background()

	build := func() *HashAggregate {
		prim := NewHashAggregate([]string{"k1", "k2", "k3"}, packedSumAggs())
		prim.PartitionedDisjoint = true
		if err := prim.Init(ctx); err != nil {
			t.Fatal(err)
		}
		const nUnits = 4
		units := []*HashAggregate{prim}
		for i := 1; i < nUnits; i++ {
			c := prim.CloneSink().(*HashAggregate)
			c.PartitionedDisjoint = true
			if err := c.Init(ctx); err != nil {
				t.Fatal(err)
			}
			units = append(units, c)
		}
		// Disjoint feed: a row's unit is decided by its whole key tuple.
		perUnit := make([][]map[string]any, nUnits)
		for _, r := range rows {
			u := (int(r["k1"].(int32))*31 + int(r["k2"].(int32))*7 + int(r["k3"].(int32)))
			if u < 0 {
				u = -u
			}
			perUnit[u%nUnits] = append(perUnit[u%nUnits], r)
		}
		for u, urows := range perUnit {
			for off := 0; off < len(urows); off += batch.DefaultBatchSize {
				end := off + batch.DefaultBatchSize
				if end > len(urows) {
					end = len(urows)
				}
				if err := units[u].Consume(ctx, batch.FromRows(schema, urows[off:end])); err != nil {
					t.Fatal(err)
				}
			}
		}
		for i := 1; i < nUnits; i++ {
			prim.MergeSink(units[i])
		}
		if err := prim.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		return prim
	}

	drain := func(h *HashAggregate) map[string]map[string]any {
		out := map[string]map[string]any{}
		for {
			b, err := h.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				key := fmt.Sprintf("%v|%v|%v", r["k1"], r["k2"], r["k3"])
				if _, dup := out[key]; dup {
					t.Fatalf("duplicate group %q in emitted stream", key)
				}
				out[key] = r
			}
		}
		h.Close()
		return out
	}

	var serial, parallel map[string]map[string]any
	withParallelEmit(t, false, func() { serial = drain(build()) })
	before := ParallelEmitRuns.Load()
	withParallelEmit(t, true, func() { parallel = drain(build()) })
	if ParallelEmitRuns.Load() == before {
		t.Fatal("parallel emit did not engage")
	}
	assertSameGroups(t, serial, parallel)
}

// Mixed-mode MergeSink: one clone saw a NULL key and migrated to the
// generic binary-key path, the other stayed packed. The migration the merge
// triggers on the packed side must produce keys in exactly the format the
// generic side used, per column — otherwise shared groups silently
// duplicate (the single-int sibling of this test is
// TestMergeSink_MixedModeAfterNullMigration).
func TestPackedGroupKeyMergeSinkMixedModeAfterNullMigration(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c", Type: parquet.TypeInt32, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	parent := NewHashAggregate([]string{"a", "b", "c"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	if err := parent.Init(ctx); err != nil {
		t.Fatal(err)
	}

	withNulls := parent.CloneSink().(*HashAggregate)
	if err := withNulls.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := withNulls.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"a": nil, "b": int32(2), "c": int32(3), "v": int64(1)},
		{"a": int32(7), "b": int32(8), "c": int32(9), "v": int64(2)},
	})); err != nil {
		t.Fatal(err)
	}
	if withNulls.usePackedGroupKey {
		t.Fatal("null-bearing clone should have migrated off the packed path")
	}

	packed := parent.CloneSink().(*HashAggregate)
	if err := packed.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := packed.Consume(ctx, batch.FromRows(schema, []map[string]any{
		{"a": int32(7), "b": int32(8), "c": int32(9), "v": int64(40)},
		{"a": int32(1), "b": int32(1), "c": int32(1), "v": int64(5)},
	})); err != nil {
		t.Fatal(err)
	}
	if !packed.usePackedGroupKey {
		t.Fatal("null-free clone should be on the packed path")
	}

	parent.MergeSink(withNulls)
	parent.MergeSink(packed)

	rows := aggRows(t, parent)
	got := map[string]any{}
	for _, r := range rows {
		got[fmt.Sprintf("%v|%v|%v", r["a"], r["b"], r["c"])] = r["s"]
	}
	want := map[string]any{
		"<nil>|2|3": int64(1),
		"7|8|9":     int64(42), // merged across both clones, not duplicated
		"1|1|1":     int64(5),
	}
	if len(got) != len(want) {
		t.Fatalf("groups = %v, want %v (duplicates mean the migrated key format diverged)", got, want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("group %s = %v, want %v", k, got[k], w)
		}
	}
}

// A packed-keyed MergeSink between two non-disjoint sinks takes
// mergePackedGroupSoA: overlapping keys must combine, disjoint ones must all
// survive, and the emitted key columns must decode back to the originals.
func TestPackedGroupKeyMergeSinkOverlap(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ctx := context.Background()
	mk := func(rows []map[string]any) *HashAggregate {
		h := NewHashAggregate([]string{"a", "b"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
			{Func: AggMin, InputCol: "v", OutputCol: "mn", OutputType: parquet.TypeInt64},
		})
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		return h
	}
	left := mk([]map[string]any{
		{"a": int64(-1), "b": int32(-2), "v": int64(5)},
		{"a": int64(10), "b": int32(20), "v": int64(7)},
	})
	right := mk([]map[string]any{
		{"a": int64(-1), "b": int32(-2), "v": int64(3)},
		{"a": int64(99), "b": int32(-99), "v": int64(11)},
	})
	if !left.usePackedGroupKey || !right.usePackedGroupKey {
		t.Fatal("expected both sinks on the packed path")
	}
	left.MergeSink(right)

	rows := aggRows(t, left)
	sums := map[string][2]int64{}
	for _, r := range rows {
		sums[fmt.Sprintf("%v|%v", r["a"], r["b"])] = [2]int64{r["s"].(int64), r["mn"].(int64)}
	}
	want := map[string][2]int64{
		"-1|-2":  {8, 3},
		"10|20":  {7, 7},
		"99|-99": {11, 11},
	}
	if len(sums) != len(want) {
		keys := make([]string, 0, len(sums))
		for k := range sums {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Fatalf("merged groups = %v, want %v", keys, want)
	}
	for k, w := range want {
		if sums[k] != w {
			t.Errorf("group %s = %v, want %v", k, sums[k], w)
		}
	}
}
