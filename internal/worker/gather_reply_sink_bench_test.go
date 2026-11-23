// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkGatherReplySinkConsume is the sink's per-batch cost — encode,
// stamp, marshal, publish on NATS — for a 1-row and a full 2,048-row batch.
func BenchmarkGatherReplySinkConsume(b *testing.B) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = b.TempDir()
	en, err := distributed.NewEmbeddedNATS(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { nc.Close() })
	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64, Nullable: true}}
	for _, rows := range []int{1, 2048} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			bt := batch.NewRecordBatch(schema, rows)
			for i := 0; i < rows; i++ {
				bt.Columns[0].Int64Data[i] = int64(i)
			}
			sink := newGatherReplySink(nc, "wadjet.gather.bench", "w0", testGatherTask, schema)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := sink.Consume(ctx, bt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
