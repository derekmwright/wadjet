// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkPoolConcurrentStatements: small statements on ONE DB, 1 or 8 at a
// time. Since 1685e the statements of a DB charge one tracker and register on
// one relief registry, so concurrent statements contend on its atomics and
// its lock; this measures that cost (ns/op is one statement). The budget is
// large enough that nothing spills, so the figure is the accounting path,
// not spill I/O. Public API only, so it runs on trees before the pool too.
func BenchmarkPoolConcurrentStatements(b *testing.B) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "bench",
		MemoryBudget: 256 << 20, SpillDir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		b.Fatal(err)
	}
	rows := make([]map[string]any, 4000)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i), "pad": fmt.Sprintf("%08d", i) + strings.Repeat("x", 56)}
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 4001, RowGroupSize: 1024})
	if err := ing.Ingest(ctx, rows); err != nil {
		b.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		b.Fatal(err)
	}
	stmts := []string{
		"SELECT k % 16 AS g, count(*) AS n FROM t GROUP BY 1 ORDER BY 1",
		"SELECT count(*) AS n FROM t a JOIN t b ON a.k = b.k WHERE a.k < 1000",
		"SELECT k, pad FROM t ORDER BY pad DESC LIMIT 20",
	}
	for _, conc := range []int{1, 8} {
		b.Run(fmt.Sprintf("concurrent=%d", conc), func(b *testing.B) {
			var next atomic.Int64
			var wg sync.WaitGroup
			var failed atomic.Value
			b.ResetTimer()
			for w := 0; w < conc; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := next.Add(1) - 1
						if i >= int64(b.N) {
							return
						}
						if _, err := db.Query(ctx, stmts[i%int64(len(stmts))]); err != nil {
							failed.Store(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			if err, _ := failed.Load().(error); err != nil {
				b.Fatal(err)
			}
		})
	}
}
