package worker

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/klauspost/compress/zstd"
)

// buildLineitemWSHF encodes real TPC-H lineitem rows (datagen
// distributions: skewed keys, low-cardinality flags/dates, random-word
// comments) into a WSHF payload — the faithful stand-in for the
// scan→repartition exchange payloads that dominate S3 shuffle bytes.
func buildLineitemWSHF(tb testing.TB) []byte {
	tb.Helper()
	tables := tpch.Generate(tpch.SF001)
	rows := tables["lineitem"]
	if len(rows) == 0 {
		tb.Fatal("datagen returned no lineitem rows")
	}

	schema := []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_suppkey", Type: parquet.TypeInt64},
		{Name: "l_linenumber", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
		{Name: "l_extendedprice", Type: parquet.TypeFloat64},
		{Name: "l_discount", Type: parquet.TypeFloat64},
		{Name: "l_tax", Type: parquet.TypeFloat64},
		{Name: "l_returnflag", Type: parquet.TypeString},
		{Name: "l_linestatus", Type: parquet.TypeString},
		{Name: "l_shipdate", Type: parquet.TypeString},
		{Name: "l_shipinstruct", Type: parquet.TypeString},
		{Name: "l_shipmode", Type: parquet.TypeString},
		{Name: "l_comment", Type: parquet.TypeString},
	}

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		tb.Fatalf("writeHeader: %v", err)
	}
	for base := 0; base < len(rows); base += batch.DefaultBatchSize {
		n := len(rows) - base
		if n > batch.DefaultBatchSize {
			n = batch.DefaultBatchSize
		}
		rb := batch.NewRecordBatch(schema, n)
		for i := 0; i < n; i++ {
			r := rows[base+i]
			rb.Columns[0].Int64Data[i] = int64(r["l_orderkey"].(int32))
			rb.Columns[1].Int64Data[i] = int64(r["l_partkey"].(int32))
			rb.Columns[2].Int64Data[i] = int64(r["l_suppkey"].(int32))
			rb.Columns[3].Int64Data[i] = int64(r["l_linenumber"].(int32))
			rb.Columns[4].Float64Data[i] = r["l_quantity"].(float64)
			rb.Columns[5].Float64Data[i] = r["l_extendedprice"].(float64)
			rb.Columns[6].Float64Data[i] = r["l_discount"].(float64)
			rb.Columns[7].Float64Data[i] = r["l_tax"].(float64)
			rb.Columns[8].BytesData.Set(i, []byte(r["l_returnflag"].(string)))
			rb.Columns[9].BytesData.Set(i, []byte(r["l_linestatus"].(string)))
			rb.Columns[10].BytesData.Set(i, []byte(r["l_shipdate"].(string)))
			rb.Columns[11].BytesData.Set(i, []byte(r["l_shipinstruct"].(string)))
			rb.Columns[12].BytesData.Set(i, []byte(r["l_shipmode"].(string)))
			rb.Columns[13].BytesData.Set(i, []byte(r["l_comment"].(string)))
		}
		if err := sw.writeChunk(rb.Columns, nil, n); err != nil {
			tb.Fatalf("writeChunk: %v", err)
		}
	}
	return buf.Bytes()
}

// BenchmarkWSHCCompressionCodecs sizes the exchange zstd-on-wire lever
// (barrier-overlap arc step 3): s2 (today's WSHC codec) vs zstd levels on
// identical real-distribution WSHF bytes. Reports ratio (compressed/raw)
// and encode throughput. Decode side benchmarked separately below.
func BenchmarkWSHCCompressionCodecs(b *testing.B) {
	raw := buildLineitemWSHF(b)
	b.Logf("raw WSHF payload: %d bytes", len(raw))

	b.Run("s2", func(b *testing.B) {
		var out []byte
		for i := 0; i < b.N; i++ {
			out = CompressShuffleData(raw)
		}
		b.SetBytes(int64(len(raw)))
		b.ReportMetric(float64(len(out))/float64(len(raw)), "ratio")
	})

	for _, lvl := range []struct {
		name string
		lv   zstd.EncoderLevel
	}{
		{"zstd-fastest", zstd.SpeedFastest},
		{"zstd-default", zstd.SpeedDefault},
		{"zstd-better", zstd.SpeedBetterCompression},
	} {
		b.Run(lvl.name, func(b *testing.B) {
			var out bytes.Buffer
			for i := 0; i < b.N; i++ {
				out.Reset()
				w, err := zstd.NewWriter(&out, zstd.WithEncoderLevel(lvl.lv), zstd.WithEncoderConcurrency(1))
				if err != nil {
					b.Fatalf("zstd writer: %v", err)
				}
				if _, err := w.Write(raw); err != nil {
					b.Fatalf("zstd write: %v", err)
				}
				if err := w.Close(); err != nil {
					b.Fatalf("zstd close: %v", err)
				}
			}
			b.SetBytes(int64(len(raw)))
			b.ReportMetric(float64(out.Len())/float64(len(raw)), "ratio")
		})
	}
}

// BenchmarkWSHCDecompressionCodecs: consumer-side decode cost of the same
// payloads — the side that sits on the probe/scan critical path.
func BenchmarkWSHCDecompressionCodecs(b *testing.B) {
	raw := buildLineitemWSHF(b)

	s2c := CompressShuffleData(raw)
	if bytes.Equal(s2c, raw) {
		b.Fatal("s2 did not compress (heuristic returned raw)")
	}

	zw, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	zc := zw.EncodeAll(raw, nil)
	zw.Close()

	b.Run("s2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			out, err := DecompressShuffleData(s2c)
			if err != nil {
				b.Fatalf("s2 decompress: %v", err)
			}
			if len(out) != len(raw) {
				b.Fatalf("s2 round-trip length %d != %d", len(out), len(raw))
			}
		}
		b.SetBytes(int64(len(raw)))
	})

	b.Run("zstd-fastest", func(b *testing.B) {
		zr, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			b.Fatalf("zstd reader: %v", err)
		}
		defer zr.Close()
		for i := 0; i < b.N; i++ {
			out, err := zr.DecodeAll(zc, nil)
			if err != nil {
				b.Fatalf("zstd decompress: %v", err)
			}
			if len(out) != len(raw) {
				b.Fatalf("zstd round-trip length %d != %d", len(out), len(raw))
			}
		}
		b.SetBytes(int64(len(raw)))
	})
}
