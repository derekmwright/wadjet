package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The reverse bloom filters the BUILD side of a join with a bloom built from
// the PROBE side's keys, so the two sides of one filter are two different
// pieces of code. They have to hash the same byte stream for the same value,
// and for every non-integer key type they did not: the insert side hashed the
// raw col.BytesData.Value(row) while the probe side hashed the join's
// canonical [null-flag][value] key. A bloom has no false negatives, so the
// only way to see that from the outside is rows quietly vanishing (#543).
//
// These tests probe a bloom with the very values that were inserted into it.
// Every one of them must survive.

// bloomTypeCase is one key type and two disjoint sets of values of it.
type bloomTypeCase struct {
	name string
	col  parquet.Column
	in   func(i int) any // values inserted into the bloom
	out  func(i int) any // values that were never inserted
}

func bloomTypeCases() []bloomTypeCase {
	col := func(name string, t parquet.TypeID) parquet.Column {
		return parquet.Column{Name: name, Type: t, Nullable: true}
	}
	dec := parquet.Column{Name: "k", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}
	return []bloomTypeCase{
		{"int32", col("k", parquet.TypeInt32), func(i int) any { return int32(i) }, func(i int) any { return int32(1_000_000 + i) }},
		{"int64", col("k", parquet.TypeInt64), func(i int) any { return int64(i) }, func(i int) any { return int64(1_000_000 + i) }},
		{"float32", col("k", parquet.TypeFloat32), func(i int) any { return float32(i) + 0.5 }, func(i int) any { return float32(1_000_000+i) + 0.25 }},
		{"float64", col("k", parquet.TypeFloat64), func(i int) any { return float64(i) + 0.5 }, func(i int) any { return float64(1_000_000+i) + 0.25 }},
		{"string", col("k", parquet.TypeString), func(i int) any { return fmt.Sprintf("key-%06d", i) }, func(i int) any { return fmt.Sprintf("miss-%06d", i) }},
		{"bytes", col("k", parquet.TypeBytes), func(i int) any { return []byte(fmt.Sprintf("key-%06d", i)) }, func(i int) any { return []byte(fmt.Sprintf("miss-%06d", i)) }},
		{"timestamp", col("k", parquet.TypeTimestamp), func(i int) any { return int64(1_600_000_000_000 + i) }, func(i int) any { return int64(1_700_000_000_000 + i) }},
		{"ipv4", col("k", parquet.TypeIPv4), func(i int) any { return fmt.Sprintf("10.0.%d.%d", i/256, i%256) }, func(i int) any { return fmt.Sprintf("192.168.%d.%d", i/256, i%256) }},
		{"ipv6", col("k", parquet.TypeIPv6), func(i int) any { return fmt.Sprintf("2001:db8::%x", i+1) }, func(i int) any { return fmt.Sprintf("fd00::%x", i+1) }},
		{"cidr", col("k", parquet.TypeCIDR), func(i int) any { return fmt.Sprintf("10.%d.%d.0/24", i/256, i%256) }, func(i int) any { return fmt.Sprintf("172.%d.%d.0/24", 16+i/256, i%256) }},
		{"mac", col("k", parquet.TypeMAC), func(i int) any { return fmt.Sprintf("00:11:22:33:%02x:%02x", i/256, i%256) }, func(i int) any { return fmt.Sprintf("aa:bb:cc:dd:%02x:%02x", i/256, i%256) }},
		{"port", col("k", parquet.TypePort), func(i int) any { return int32(1024 + i) }, func(i int) any { return int32(40000 + i) }},
		{"protocol", col("k", parquet.TypeProtocol), func(i int) any { return int32(i % 200) }, func(i int) any { return int32(200 + i%50) }},
		{"duration", col("k", parquet.TypeDuration), func(i int) any { return int64(i * 1000) }, func(i int) any { return int64(9_000_000 + i) }},
		{"uuid", col("k", parquet.TypeUUID), func(i int) any { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i) }, func(i int) any { return fmt.Sprintf("ffffffff-0000-0000-0000-%012d", i) }},
		{"date", col("k", parquet.TypeDate), func(i int) any { return int32(19000 + i) }, func(i int) any { return int32(1000 + i) }},
		{"decimal", dec, func(i int) any { return float64(i) + 0.125 }, func(i int) any { return float64(500_000+i) + 0.375 }},
		{"bool", col("k", parquet.TypeBool), func(i int) any { return i%2 == 0 }, nil}, // only two values: no disjoint set
	}
}

// bloomKeyBatch materializes n rows of one typed column under the given name.
func bloomKeyBatch(tb testing.TB, c parquet.Column, name string, n int, val func(i int) any) *batch.RecordBatch {
	tb.Helper()
	c.Name = name
	b := batch.NewRecordBatch([]parquet.Column{c}, n)
	for i := 0; i < n; i++ {
		b.Columns[0].SetValue(i, val(i))
	}
	return b
}

// TestBloomKeyEncodingAgreesForEveryKeyType is #543's regression gate. For
// every type a join key can have, the values inserted into a bloom must all
// probe true through the operator that filters with it — and the operator must
// still reject values that were never inserted, or "everything survives" would
// pass vacuously.
func TestBloomKeyEncodingAgreesForEveryKeyType(t *testing.T) {
	const n = 512
	ctx := context.Background()
	for _, tc := range bloomTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			// Insert side and probe side deliberately name the column
			// differently: a reverse bloom is built from the probe side's
			// column and applied to the build side's.
			ins := bloomKeyBatch(t, tc.col, "probe_key", n, tc.in)
			bb := NewBloomBuilder(n)
			if bb == nil {
				t.Fatal("NewBloomBuilder returned nil for a non-empty build")
			}
			if err := bb.Add(ins, "probe_key"); err != nil {
				t.Fatalf("add: %v", err)
			}
			if bb.Inserted() != n {
				t.Fatalf("inserted %d keys, want %d", bb.Inserted(), n)
			}
			op := bb.FilterOp("build_key")
			if err := op.SelfCheck(); err != nil {
				t.Fatalf("self-check: %v", err)
			}

			hit := bloomKeyBatch(t, tc.col, "build_key", n, tc.in)
			out, err := op.Execute(ctx, hit)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			got := 0
			if out != nil {
				got = out.ActiveLen()
			}
			if got != n {
				t.Fatalf("bloom rejected %d of %d rows carrying the keys it was built from", n-got, n)
			}

			if tc.out == nil {
				return
			}
			miss := bloomKeyBatch(t, tc.col, "build_key", n, tc.out)
			op2 := bb.FilterOp("build_key")
			out2, err := op2.Execute(ctx, miss)
			if err != nil {
				t.Fatalf("execute (disjoint): %v", err)
			}
			passed := 0
			if out2 != nil {
				passed = out2.ActiveLen()
			}
			// ~1% false positive rate by construction; anything near n means
			// the filter is not filtering and the test above proved nothing.
			if passed > n/4 {
				t.Fatalf("bloom passed %d of %d rows it never saw — it is not filtering", passed, n)
			}
		})
	}
}

// TestBloomAddBatchAgreesWithTheProbeSide is the issue's own reproduction:
// four IDENTICAL string-keyed rows on both sides, through the free-function
// build helper and a plainly constructed operator. Before the encoders were
// unified, 0 of the 4 survived.
func TestBloomAddBatchAgreesWithTheProbeSide(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma", "delta"}
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeString, Nullable: true}}

	build := batch.NewRecordBatch(schema, len(keys))
	for i, k := range keys {
		build.Columns[0].SetValue(i, k)
	}
	bloom, mask := BuildBloomFromBatches([]*batch.RecordBatch{build}, "k")
	if bloom == nil {
		t.Fatal("BuildBloomFromBatches returned no bloom")
	}

	probe := batch.NewRecordBatch(schema, len(keys))
	for i, k := range keys {
		probe.Columns[0].SetValue(i, k)
	}
	op := NewBloomFilterOp(bloom, mask, []string{"k"}, false)
	out, err := op.Execute(context.Background(), probe)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	got := 0
	if out != nil {
		got = out.ActiveLen()
	}
	if got != len(keys) {
		t.Fatalf("%d of %d identical rows survived the bloom; want all of them", got, len(keys))
	}
}

// TestBloomAddBatchHandlesEveryStorageShape guards a second half of the same
// defect: the insert side classified only five types as integer keys and read
// col.BytesData for every other one, so a TIMESTAMP / IPv4 / MAC / DURATION /
// FLOAT / DECIMAL / BOOL key indexed a BytesColumn its vector does not have.
func TestBloomAddBatchHandlesEveryStorageShape(t *testing.T) {
	for _, tc := range bloomTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := bloomKeyBatch(t, tc.col, "k", 32, tc.in)
			bloom, mask := NewBloomSized(32)
			BloomAddBatch(bloom, mask, b, "k") // must not panic
			any := false
			for _, w := range bloom {
				if w != 0 {
					any = true
					break
				}
			}
			if !any {
				t.Fatal("no bits set: the insert side hashed nothing")
			}
		})
	}
}

// TestBloomSelfCheckCatchesABrokenFilter proves the self-check is not
// tautological: a bloom whose bits have been lost fails it, loudly, instead of
// rejecting every row in silence.
func TestBloomSelfCheckCatchesABrokenFilter(t *testing.T) {
	const n = 128
	b := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
		"k", n, func(i int) any { return fmt.Sprintf("key-%04d", i) })
	bb := NewBloomBuilder(n)
	if err := bb.Add(b, "k"); err != nil {
		t.Fatalf("add: %v", err)
	}
	op := bb.FilterOp("k")
	if err := op.SelfCheck(); err != nil {
		t.Fatalf("self-check on a sound bloom: %v", err)
	}

	bits, _ := bb.Bloom()
	for i := range bits {
		bits[i] = 0
	}
	if err := op.SelfCheck(); err == nil {
		t.Fatal("self-check passed a bloom that cannot match any of its own keys")
	}
}

// TestBloomFilterDisengagesWhenItRejectsEverything is the adaptive bypass's
// missing half. The bypass only ever disengaged a filter rejecting LESS than
// 5% of rows; ~100% rejection — the shape a broken filter has — was invisible
// to it, and doubly so because a fully rejected batch returned before the
// accounting that fed the rule.
func TestBloomFilterDisengagesWhenItRejectsEverything(t *testing.T) {
	const n = 256
	ins := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
		"k", n, func(i int) any { return fmt.Sprintf("key-%04d", i) })
	bb := NewBloomBuilder(n)
	if err := bb.Add(ins, "k"); err != nil {
		t.Fatalf("add: %v", err)
	}
	op := bb.FilterOp("k")

	// Break the filter after the op was made, the way an encoder divergence
	// would: every probe now misses.
	bits, _ := bb.Bloom()
	for i := range bits {
		bits[i] = 0
	}

	before := BloomSelfCheckFailures.Load()
	probe := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
		"k", n, func(i int) any { return fmt.Sprintf("key-%04d", i) })
	out, err := op.Execute(context.Background(), probe)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out == nil || out.ActiveLen() != n {
		got := 0
		if out != nil {
			got = out.ActiveLen()
		}
		t.Fatalf("first batch returned %d of %d rows; a filter that fails its own self-check must pass rows through", got, n)
	}
	if BloomSelfCheckFailures.Load() == before {
		t.Fatal("self-check failure was not counted")
	}

	// And it stays disengaged.
	probe2 := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
		"k", n, func(i int) any { return fmt.Sprintf("key-%04d", i) })
	out2, err := op.Execute(context.Background(), probe2)
	if err != nil {
		t.Fatalf("execute 2: %v", err)
	}
	if out2 == nil || out2.ActiveLen() != n {
		t.Fatal("filter re-engaged after failing its self-check")
	}
}

// TestBloomFilterDisengagesOnKeyTypeMismatch covers the third derivation the
// builder removed. The operator's integer fast path indexes Int64Data; pointing
// it at a STRING column read a slice that vector does not have.
func TestBloomFilterDisengagesOnKeyTypeMismatch(t *testing.T) {
	const n = 64
	ins := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		"k", n, func(i int) any { return int64(i) })
	bb := NewBloomBuilder(n)
	if err := bb.Add(ins, "k"); err != nil {
		t.Fatalf("add: %v", err)
	}
	op := bb.FilterOp("k")

	before := BloomKeyTypeMismatches.Load()
	probe := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
		"k", n, func(i int) any { return fmt.Sprintf("key-%04d", i) })
	out, err := op.Execute(context.Background(), probe)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out == nil || out.ActiveLen() != n {
		t.Fatal("a type-mismatched filter must pass every row through, not filter with an encoding that cannot match")
	}
	if BloomKeyTypeMismatches.Load() == before {
		t.Fatal("key type mismatch was not counted")
	}
}

// TestBloomBuilderRefusesAMidBuildTypeChange: one bloom, one encoding. Two
// types across the batches means no single encoding is right for both.
func TestBloomBuilderRefusesAMidBuildTypeChange(t *testing.T) {
	bb := NewBloomBuilder(16)
	a := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		"k", 8, func(i int) any { return int64(i) })
	if err := bb.Add(a, "k"); err != nil {
		t.Fatalf("add int64: %v", err)
	}
	b := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeString, Nullable: true},
		"k", 8, func(i int) any { return fmt.Sprintf("%d", i) })
	if err := bb.Add(b, "k"); err == nil {
		t.Fatal("builder accepted a second key type for one bloom")
	}
}

// TestBloomBuilderSkipsNullKeys: a NULL key matches nothing, itself included,
// which is what the probe side does with one — so it must not go into the
// bloom either.
func TestBloomBuilderSkipsNullKeys(t *testing.T) {
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeString, Nullable: true}}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, "a")
	b.Columns[0].SetValue(1, nil)
	b.Columns[0].SetValue(2, "c")
	b.Columns[0].SetValue(3, nil)

	bb := NewBloomBuilder(4)
	if err := bb.Add(b, "k"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if bb.Inserted() != 2 {
		t.Fatalf("inserted %d keys, want 2 (NULLs skipped)", bb.Inserted())
	}
	if err := bb.FilterOp("k").SelfCheck(); err != nil {
		t.Fatalf("self-check: %v", err)
	}
}

// TestBloomBuilderResolvedReportsAMissingColumn: a bloom built from a column
// no batch carried rejects everything. Installing one on an anti-join's build
// side would invent unmatched probe rows, so the bridge asks first.
func TestBloomBuilderResolvedReportsAMissingColumn(t *testing.T) {
	b := bloomKeyBatch(t, parquet.Column{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		"k", 8, func(i int) any { return int64(i) })
	bb := NewBloomBuilder(8)
	if err := bb.Add(b, "not_here"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if bb.Resolved() {
		t.Fatal("builder reported a column it never found as resolved")
	}
	if bb.Inserted() != 0 {
		t.Fatalf("inserted %d keys from a missing column", bb.Inserted())
	}
}
