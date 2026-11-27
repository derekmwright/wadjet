// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The compact builds' cost (#1706, line 1706p): a key-only semi build over 1M
// int keys and over 1M ~40-byte string keys (the parallel route on this
// host), and the NE build over 1M int rows, each under an unbudgeted tracker
// so the accounting runs but never refuses. Self-contained so the same file
// runs on the base.

type cbbReplay struct {
	batches []*batch.RecordBatch
	i       int
}

func (s *cbbReplay) Init(context.Context) error { s.i = 0; return nil }
func (s *cbbReplay) Close() error               { return nil }
func (s *cbbReplay) Next(context.Context) (*batch.RecordBatch, error) {
	if s.i >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.i]
	s.i++
	return b, nil
}

func cbbBatches(n int, str, ne bool) ([]*batch.RecordBatch, []parquet.Column) {
	kt := parquet.TypeInt64
	if str {
		kt = parquet.TypeString
	}
	schema := []parquet.Column{{Name: "k", Type: kt}}
	if ne {
		schema = append(schema, parquet.Column{Name: "v", Type: parquet.TypeInt64})
	}
	var out []*batch.RecordBatch
	for start := 0; start < n; start += batch.DefaultBatchSize {
		m := min(batch.DefaultBatchSize, n-start)
		b := batch.NewRecordBatch(schema, m)
		for j := 0; j < m; j++ {
			i := start + j
			if str {
				b.Columns[0].BytesData.SetString(j, fmt.Sprintf("key-%036d", i))
			} else {
				b.Columns[0].Int64Data[j] = int64(i / 2)
			}
			if ne {
				b.Columns[1].Int64Data[j] = int64(i % 3)
			}
		}
		b.Len = m
		out = append(out, b)
	}
	return out, schema
}

func BenchmarkCompactBuild(b *testing.B) {
	cases := []struct {
		name    string
		str, ne bool
	}{
		{"keyonly-int-1M", false, false},
		{"keyonly-str40-1M", true, false},
		{"ne-int-1M", false, true},
	}
	for _, c := range cases {
		batches, schema := cbbBatches(1_000_000, c.str, c.ne)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
				if c.ne {
					hj.SemiAntiFilter = func(*batch.RecordBatch, int, *batch.RecordBatch, int) bool { return true }
					hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
					hj.ProbeSchemaHint = []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "pv", Type: parquet.TypeInt64}}
					hj.BuildSchemaHint = schema
				} else {
					hj.SemiAntiKeyOnly = true
				}
				hj.BuildRowHint = 1_000_000
				hj.MemTracker = memory.NewTracker("bench", 0)
				if err := hj.Build(context.Background(), &cbbReplay{batches: batches}); err != nil {
					b.Fatal(err)
				}
				if c.ne && !hj.NEActive() {
					b.Fatal("NE did not engage")
				}
				hj.Close()
			}
		})
	}
}
