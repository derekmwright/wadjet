package exec

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// benchBatchSource serves pre-built batches to the parallel pipeline. Safe for
// concurrent Next (every worker pulls from the same source), and the batches
// are pool-free so the pipeline's Release calls are no-ops and the same data
// can be replayed across b.N iterations.
type benchBatchSource struct {
	batches []*batch.RecordBatch
	pos     atomic.Int64
}

func (s *benchBatchSource) Init(context.Context) error { s.pos.Store(0); return nil }

func (s *benchBatchSource) Next(context.Context) (*batch.RecordBatch, error) {
	i := s.pos.Add(1) - 1
	if int(i) >= len(s.batches) {
		return nil, nil
	}
	return s.batches[i], nil
}

func (s *benchBatchSource) Close() error { return nil }

// runPartitionedAgg drives one partitioned parallel aggregation over batches
// and drains the result, returning the group count.
func runPartitionedAgg(tb testing.TB, groupCols []string, aggs []AggColumn, batches []*batch.RecordBatch, workers int) int {
	tb.Helper()
	ctx := context.Background()
	agg := NewHashAggregate(groupCols, aggs)
	src := &benchBatchSource{batches: batches}
	pipe := &Pipeline{Source: src, Sink: agg, Workers: workers}
	if err := pipe.Run(ctx); err != nil {
		tb.Fatal(err)
	}
	groups := 0
	for {
		out, err := agg.Next(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		if out == nil {
			break
		}
		groups += out.ActiveLen()
	}
	agg.Close()
	return groups
}

func sumCountAggs() []AggColumn {
	return []AggColumn{
		{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	}
}

type hashOnceShape struct {
	name    string
	cols    []string
	batches []*batch.RecordBatch
	keys    int64
}

// hashOnceShapes mirror the ClickBench queries that drive high-cardinality
// aggregation: a single ~90-byte URL key (Q34/Q35), a two-int64 composite
// (Q33), and a single dense int64 key.
func hashOnceShapes(numKeys int) []hashOnceShape {
	return []hashOnceShape{
		{"string-url", []string{"url"}, urlKeyBatches(numKeys), int64(numKeys)},
		{"packed-two-int64", []string{"wid", "ip"}, twoIntKeyBatches(numKeys), int64(numKeys)},
		{"single-int64", []string{"k"}, singleIntKeyBatches(numKeys), int64(numKeys)},
	}
}

// BenchmarkAggRouteAndConsume is the isolated G5 measurement: the partition
// router plus the owning sink's probe loop, and nothing else. Both halves used
// to hash every row independently — the same ~90-byte URL through two full
// passes — and the router picked an owner with a hardware 64-bit `% parts`
// divide. The `hash_once=false` arm restores the double hash via the kill
// switch; the multiply-shift owner selection is unconditional and present in
// both arms.
//
// Single-goroutine and NDV-pre-sized on purpose. The whole-pipeline arm
// (BenchmarkPartitionedAggPipeline) spends most of its time in table growth,
// the parallel emit and the allocator, which drowns an instruction-path change
// in scheduler noise; this one routes and probes the same rows with the growth
// and the goroutines taken out.
// The x4 arms replay the input four times so 75% of the probes are lookups
// into a full table — the real Q34 ratio (100M rows / 18M keys), and the
// regime where a saved key hash is not competing with an arena copy.
func BenchmarkAggRouteAndConsume(b *testing.B) {
	const parts = 8
	ctx := context.Background()
	for _, sh := range hashOnceShapes(1 << 20) {
		for _, passes := range []int{1, 4} {
			for _, on := range []bool{true, false} {
				name := fmt.Sprintf("%s/x%d/hash_once=%v", sh.name, passes, on)
				b.Run(name, func(b *testing.B) {
					prev := hashOnceToggle.Set(on)
					defer hashOnceToggle.Set(prev)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						router := NewHashAggregate(sh.cols, sumCountAggs())
						router.GroupNDVHint = sh.keys
						sink := NewHashAggregate(sh.cols, sumCountAggs())
						sink.GroupNDVHint = sh.keys
						if err := router.Init(ctx); err != nil {
							b.Fatal(err)
						}
						if err := sink.Init(ctx); err != nil {
							b.Fatal(err)
						}
						var sc partitionScratch
						for p := 0; p < passes; p++ {
							for _, bb := range sh.batches {
								rt := router.PartitionSelectors(bb, parts, &sc)
								if rt == nil {
									b.Fatal("router refused to partition")
								}
								for j := range rt.sels {
									if len(rt.sels[j]) == 0 {
										continue
									}
									if err := consumeRouted(ctx, sink, selView(bb, rt.sels[j]), rt.hashes[j], rt.plan); err != nil {
										b.Fatal(err)
									}
								}
								routedBufPool.Put(rt.buf)
							}
						}
						router.Close()
						sink.Close()
					}
				})
			}
		}
	}
}

// BenchmarkPartitionedAggPipeline is the whole-pipeline control for
// BenchmarkAggRouteAndConsume: real workers, real queues, real emit.
func BenchmarkPartitionedAggPipeline(b *testing.B) {
	const workers = 8
	for _, sh := range hashOnceShapes(1 << 20) {
		for _, on := range []bool{true, false} {
			b.Run(fmt.Sprintf("%s/hash_once=%v", sh.name, on), func(b *testing.B) {
				prev := hashOnceToggle.Set(on)
				defer hashOnceToggle.Set(prev)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					runPartitionedAgg(b, sh.cols, sumCountAggs(), sh.batches, workers)
				}
			})
		}
	}
}

// twoIntKeyBatches builds near-unique (WatchID, ClientIP)-shaped rows.
func twoIntKeyBatches(numKeys int) []*batch.RecordBatch {
	const batchRows = 2048
	schema := []parquet.Column{
		{Name: "wid", Type: parquet.TypeInt64},
		{Name: "ip", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	batches := make([]*batch.RecordBatch, 0, numKeys/batchRows)
	for base := 0; base < numKeys; base += batchRows {
		bb := batch.NewRecordBatch(schema, batchRows)
		for i := 0; i < batchRows; i++ {
			row := int64(base + i)
			bb.Columns[0].SetValue(i, row*2654435761)
			bb.Columns[1].SetValue(i, row)
			bb.Columns[2].SetValue(i, float64(row))
		}
		batches = append(batches, bb)
	}
	return batches
}

// singleIntKeyBatches builds near-unique dense int64 keys — the shape whose
// slot layout depends on fibHash's low bits staying a bijection.
func singleIntKeyBatches(numKeys int) []*batch.RecordBatch {
	const batchRows = 2048
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	batches := make([]*batch.RecordBatch, 0, numKeys/batchRows)
	for base := 0; base < numKeys; base += batchRows {
		bb := batch.NewRecordBatch(schema, batchRows)
		for i := 0; i < batchRows; i++ {
			row := int64(base + i)
			bb.Columns[0].SetValue(i, row)
			bb.Columns[1].SetValue(i, float64(row))
		}
		batches = append(batches, bb)
	}
	return batches
}

// probeSink keeps the look-ahead loads from being optimized away.
var probeSink int32

// BenchmarkPackedProbeShapes is the measurement behind G5's third sub-item:
// does restructuring phase 1 of the consume loop into two passes (a branchless
// slot-index pass over the whole batch, then the probe pass) beat the
// dependent one-row-at-a-time chain, and does an explicit look-ahead load help
// once the table exceeds L2?
//
// Arms:
//
//	onepass    today's shape: hash -> mask -> probe, per row
//	twopass    idx[] for the whole 2048-row block, then probe from idx[]
//	lookahead  twopass plus a dummy load of entries[idx[i+D]]
//
// Sizes: 1M keys (24 MB of entries, ~L2/L3 resident) and 20M keys (480 MB,
// main memory).
func BenchmarkPackedProbeShapes(b *testing.B) {
	const block = 2048
	for _, n := range []int{1 << 20, 20 << 20} {
		keys := make([]packedKey, n)
		for i := range keys {
			keys[i] = packedKey{lo: uint64(i) * 2654435761, hi: uint64(i)}
		}
		tbl := newPackedHashTable(n)
		for i := range keys {
			tbl.GetOrInsertNoGrow(keys[i].lo, keys[i].hi, int32(i))
		}
		idx := make([]uint64, block)

		b.Run(fmt.Sprintf("n=%dM/onepass", n>>20), func(b *testing.B) {
			b.ResetTimer()
			for it := 0; it < b.N; it++ {
				var acc int32
				for base := 0; base < n; base += block {
					end := base + block
					for i := base; i < end; i++ {
						v, _ := tbl.Get(keys[i].lo, keys[i].hi)
						acc |= v
					}
				}
				probeSink = acc
			}
		})

		b.Run(fmt.Sprintf("n=%dM/twopass", n>>20), func(b *testing.B) {
			b.ResetTimer()
			for it := 0; it < b.N; it++ {
				var acc int32
				for base := 0; base < n; base += block {
					for i := 0; i < block; i++ {
						k := keys[base+i]
						idx[i] = packedHash(k.lo, k.hi) & tbl.mask
					}
					for i := 0; i < block; i++ {
						k := keys[base+i]
						j := idx[i]
						for {
							e := &tbl.entries[j]
							if e.val == packedHashEmpty {
								break
							}
							if e.lo == k.lo && e.hi == k.hi {
								acc |= e.val
								break
							}
							j = (j + 1) & tbl.mask
						}
					}
				}
				probeSink = acc
			}
		})

		for _, dist := range []int{8, 16} {
			b.Run(fmt.Sprintf("n=%dM/lookahead_d=%d", n>>20, dist), func(b *testing.B) {
				b.ResetTimer()
				for it := 0; it < b.N; it++ {
					var acc int32
					for base := 0; base < n; base += block {
						for i := 0; i < block; i++ {
							k := keys[base+i]
							idx[i] = packedHash(k.lo, k.hi) & tbl.mask
						}
						for i := 0; i < block; i++ {
							if i+dist < block {
								acc |= tbl.entries[idx[i+dist]].val & 0
							}
							k := keys[base+i]
							j := idx[i]
							for {
								e := &tbl.entries[j]
								if e.val == packedHashEmpty {
									break
								}
								if e.lo == k.lo && e.hi == k.hi {
									acc |= e.val
									break
								}
								j = (j + 1) & tbl.mask
							}
						}
					}
					probeSink = acc
				}
			})
		}
	}
}

// BenchmarkStrHashProvidedHash isolates the string-key saving: probing with a
// caller-supplied hash versus recomputing strHash over ~90 bytes.
func BenchmarkStrHashProvidedHash(b *testing.B) {
	const numKeys = 1 << 20
	prefix := "https://example.com/watch/section/" + strings.Repeat("x", 40) + "/id="
	keys := make([][]byte, numKeys)
	hashes := make([]uint64, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%s%012d", prefix, i))
		hashes[i] = strHash(keys[i])
	}
	ht := newStrHashTable(numKeys)
	for j, k := range keys {
		ht.GetOrInsert(k, int32(j))
	}

	b.Run("recompute", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var acc int32
			for _, k := range keys {
				v, _, _ := ht.GetOrInsertRef(k, -1)
				acc |= v
			}
			probeSink = acc
		}
	})
	b.Run("provided", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var acc int32
			for j, k := range keys {
				v, _, _ := ht.GetOrInsertRefAt(k, hashes[j], -1)
				acc |= v
			}
			probeSink = acc
		}
	})
}
