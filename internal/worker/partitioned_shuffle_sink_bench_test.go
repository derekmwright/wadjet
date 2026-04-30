package worker

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// BenchmarkPartitionedShuffleConsume measures pure Consume cost on a
// realistic SF1-lineitem-like batch (16 columns, 2048 rows, partition key
// is l_orderkey-shaped int64).  The benchmark exercises the whole
// Consume → appendRowsBulk → growBatchTo path; the input batch is
// reused across iterations so we are NOT measuring batch creation.
//
// Use this to validate refactors of the shuffle sink without the
// noise of running a full TPC-H query.
func BenchmarkPartitionedShuffleConsume(b *testing.B) {
	for _, np := range []int{2, 4, 16, 24} {
		b.Run("parts="+strconv.Itoa(np), func(b *testing.B) {
			schema := []parquet.Column{
				{Name: "l_orderkey", Type: parquet.TypeInt64},
				{Name: "l_partkey", Type: parquet.TypeInt64},
				{Name: "l_suppkey", Type: parquet.TypeInt64},
				{Name: "l_linenumber", Type: parquet.TypeInt32},
				{Name: "l_quantity", Type: parquet.TypeFloat64},
				{Name: "l_extendedprice", Type: parquet.TypeFloat64},
				{Name: "l_discount", Type: parquet.TypeFloat64},
				{Name: "l_tax", Type: parquet.TypeFloat64},
				{Name: "l_returnflag", Type: parquet.TypeString},
				{Name: "l_linestatus", Type: parquet.TypeString},
				{Name: "l_shipdate", Type: parquet.TypeDate},
				{Name: "l_commitdate", Type: parquet.TypeDate},
				{Name: "l_receiptdate", Type: parquet.TypeDate},
				{Name: "l_shipinstruct", Type: parquet.TypeString},
				{Name: "l_shipmode", Type: parquet.TypeString},
				{Name: "l_comment", Type: parquet.TypeString},
			}

			const nRows = 2048
			rb := batch.NewRecordBatch(schema, nRows)
			for col := 0; col < len(schema); col++ {
				v := rb.Columns[col]
				switch v.Type {
				case parquet.TypeInt64:
					for i := 0; i < nRows; i++ {
						v.Int64Data[i] = int64(i*7919 + col*13)
					}
				case parquet.TypeInt32:
					for i := 0; i < nRows; i++ {
						v.Int32Data[i] = int32(i)
					}
				case parquet.TypeFloat64:
					for i := 0; i < nRows; i++ {
						v.Float64Data[i] = float64(i) * 1.1
					}
				case parquet.TypeDate:
					for i := 0; i < nRows; i++ {
						v.Int32Data[i] = int32(20000 + i%365)
					}
				case parquet.TypeString:
					for i := 0; i < nRows; i++ {
						v.BytesData.Set(i, []byte("abcdefghij"))
					}
				}
			}
			rb.Len = nRows
			ctx := context.Background()

			// Reuse one sink across many Consume calls. The flush threshold
			// keeps per-partition row buffers bounded; production hits the
			// flush every few batches at SF1+. We measure steady-state per-
			// Consume cost AFTER the buffers reach their working size.
			dir := b.TempDir()
			sink := newPartitionedShuffleSink(filepath.Join(dir, "sf"), []string{"l_orderkey"}, np, schema)
			if err := sink.Init(ctx); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = sink.Close() }()

			// Warm-up: hit each partition once so the per-partition rowBuf
			// is allocated and the benchmark loop sees only the Consume cost.
			for i := 0; i < 4; i++ {
				if err := sink.Consume(ctx, rb); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := sink.Consume(ctx, rb); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
