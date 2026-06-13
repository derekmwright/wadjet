package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// TestDistributedDecimalAndNullGroups is the distributed half of the
// issue #144 correctness gate: Decimal values and NULL group keys through
// the full dispatch → worker pipeline → WSHF shuffle encode/decode →
// gather → coordinator path. TPC-H stores money as Float64 and its group
// keys are never NULL, so SF0.01/SF1/SF100 never exercised either (the
// sweep's coordinator Decimal-merge collapse and the PR #142 NULL-group
// drop both shipped SF100-green).
func TestDistributedDecimalAndNullGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("creating NATS KV: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "grp", Type: parquet.TypeString, Nullable: true},
		{Name: "amount", Type: parquet.TypeDecimal, Nullable: true, Precision: 12, Scale: 2},
	}}
	const numRows = 4000
	rows := make([]map[string]any, numRows)
	type oracle struct {
		count int64
		sum   float64
	}
	want := map[string]*oracle{}
	for i := range rows {
		id := int64(i)
		var grp any
		key := "<null>"
		if id%7 != 3 {
			g := fmt.Sprintf("g%d", id%3)
			grp = g
			key = g
		}
		var amt any
		if id%11 != 5 {
			amt = float64(id%40) + 0.25
		}
		rows[i] = map[string]any{"id": id, "grp": grp, "amount": amt}
		o := want[key]
		if o == nil {
			o = &oracle{}
			want[key] = o
		}
		o.count++
		if amt != nil {
			o.sum += amt.(float64)
		}
	}

	if err := cat.CreateTable(ctx, "ledger", schema, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	pdata := buf.Bytes()
	const filePath = "tables/ledger/chunk_0001.parquet"
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "ledger", map[string]string{}, "tables/ledger/", []catalog.FileEntry{{
		Path: filePath, SizeBytes: int64(len(pdata)), NumRows: numRows, CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatal(err)
	}

	w := worker.New(worker.Config{
		NATSUrl:       embeddedNATS.ClientURL(),
		MaxConcurrent: 4,
		CacheBytes:    64 * 1024 * 1024,
	}, store, nc, js, logger)
	// Workers must outlive the setup/query ctx: tying taskLoop to a
	// short-lived ctx silently killed task consumption mid-test once
	// the deadline passed (issue #143 — the -race "deadlock" was the
	// worker dying at setupDistributed\'s 30s timeout).
	workerCtx, workerCancel := context.WithCancel(context.Background())
	t.Cleanup(workerCancel)
	if err := w.Start(workerCtx); err != nil {
		t.Fatalf("starting worker: %v", err)
	}
	t.Cleanup(w.Stop)

	coord := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	coord.workers.record(distributed.WorkerHeartbeat{WorkerID: "fake-worker-0", Timestamp: time.Now()})

	t.Run("null_group_and_decimal_sum", func(t *testing.T) {
		res, err := coord.ExecuteSQL(ctx, `SELECT grp, COUNT(*) AS c, SUM(amount) AS s FROM ledger GROUP BY grp`)
		if err != nil {
			t.Fatalf("ExecuteSQL: %v", err)
		}
		got := map[string]*oracle{}
		for _, row := range mustRows(t, res) {
			key := "<null>"
			if row["grp"] != nil {
				key = fmt.Sprintf("%v", row["grp"])
			}
			var c int64
			fmt.Sscanf(fmt.Sprintf("%v", row["c"]), "%d", &c)
			var s float64
			if row["s"] != nil {
				fmt.Sscanf(fmt.Sprintf("%v", row["s"]), "%f", &s)
			}
			got[key] = &oracle{count: c, sum: s}
		}
		if len(got) != len(want) {
			t.Fatalf("groups = %d (%v), want %d incl. NULL group", len(got), got, len(want))
		}
		for k, w := range want {
			g := got[k]
			if g == nil {
				t.Fatalf("group %q missing (NULL group dropped in distributed path?)", k)
			}
			if g.count != w.count {
				t.Fatalf("group %q count = %d, want %d", k, g.count, w.count)
			}
			if diff := g.sum - w.sum; diff > 0.01 || diff < -0.01 {
				t.Fatalf("group %q sum = %v, want %v", k, g.sum, w.sum)
			}
		}
	})

	t.Run("decimal_group_keys_distinct", func(t *testing.T) {
		res, err := coord.ExecuteSQL(ctx, `SELECT amount, COUNT(*) AS c FROM ledger GROUP BY amount`)
		if err != nil {
			t.Fatalf("ExecuteSQL: %v", err)
		}
		rows := mustRows(t, res)
		// 40 distinct fractional amounts + the NULL amount group. A scale
		// or key-encoding loss collapses or truncates them (".25" gone).
		if len(rows) != 41 {
			t.Fatalf("distinct amount groups = %d, want 41", len(rows))
		}
		sawFraction := false
		for _, row := range rows {
			if row["amount"] == nil {
				continue
			}
			var f float64
			fmt.Sscanf(fmt.Sprintf("%v", row["amount"]), "%f", &f)
			frac := f - float64(int64(f))
			if frac > 0.24 && frac < 0.26 {
				sawFraction = true
			} else {
				t.Fatalf("amount key %v lost its fraction (want x.25)", row["amount"])
			}
		}
		if !sawFraction {
			t.Fatal("no fractional decimal keys came back")
		}
	})
}
