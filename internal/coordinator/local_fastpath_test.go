package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// TestLocalFastPathDifferential is the correctness gate for the small-query
// fast path: every query runs through BOTH a fast-path-enabled coordinator
// (executes in-process) and a DAG-only coordinator (distributed stage DAG)
// against the same catalog and data, and the results must match. The two
// paths consume the identical optimized logical plan, so any divergence is
// a routing or execution bug.
func TestLocalFastPathDifferential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)
	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("js: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("cat init: %v", err)
	}

	data := tpch.Generate(tpch.SF001)
	schema := tpch.AllTables["lineitem"]
	rows := data["lineitem"]
	if err := cat.CreateTable(ctx, "lineitem", schema, nil); err != nil {
		t.Fatal(err)
	}
	const chunks = 3
	per := (len(rows) + chunks - 1) / chunks
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, c*per+per
		if hi > len(rows) {
			hi = len(rows)
		}
		if lo >= hi {
			break
		}
		var buf bytes.Buffer
		pw, _ := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		pw.Close()
		fp := fmt.Sprintf("tables/lineitem/chunk_%04d.parquet", c)
		pd := buf.Bytes()
		store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
		cat.AddFiles(ctx, "lineitem", map[string]string{}, "tables/lineitem/", []catalog.FileEntry{{
			Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
		}})
	}

	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{NATSUrl: embeddedNATS.ClientURL(), MaxConcurrent: 4, CacheBytes: 64 << 20}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}

	fast := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test",
		LocalFastPathBytes: DefaultLocalFastPathBytes}, cat, nc, js, logger)
	dag := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	tiny := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test",
		LocalFastPathBytes: 1}, cat, nc, js, logger)
	for _, c := range []*Coordinator{fast, dag, tiny} {
		for i := 0; i < 3; i++ {
			c.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
		}
	}

	queries := []struct {
		sql     string
		cols    []string
		ordered bool
	}{
		{"SELECT l_orderkey, l_extendedprice FROM lineitem WHERE l_orderkey = 1",
			[]string{"l_orderkey", "l_extendedprice"}, false},
		{"SELECT COUNT(*) AS c FROM lineitem WHERE l_quantity > 45",
			[]string{"c"}, false},
		{"SELECT l_returnflag, COUNT(*) AS c, SUM(l_extendedprice) AS s FROM lineitem GROUP BY l_returnflag",
			[]string{"l_returnflag", "c", "s"}, false},
		{"SELECT l_orderkey, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10",
			[]string{"l_orderkey", "l_extendedprice"}, true},
		{"SELECT DISTINCT l_returnflag, l_linestatus FROM lineitem",
			[]string{"l_returnflag", "l_linestatus"}, false},
		// Expression above a GROUP BY: the aggregate fragment computes the
		// derived columns, so both paths project. (A bare expression SELECT
		// over a scan is NOT here — the DAG path has a pre-existing bug
		// where scan→gather never computes SELECT-list expressions; the
		// ground-truth check at the end of this test covers that shape.)
		{"SELECT lower(l_shipmode) AS m, COUNT(*) AS c FROM lineitem GROUP BY lower(l_shipmode)",
			[]string{"m", "c"}, false},
		{"SELECT a.l_orderkey, a.l_linenumber, b.l_extendedprice FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey WHERE a.l_orderkey < 50",
			[]string{"a.l_orderkey", "a.l_linenumber", "b.l_extendedprice"}, false},
	}

	// canon renders each row as the requested output columns, positionally.
	// Lookup tries the qualified name first, then the unqualified suffix —
	// the two paths qualify join output columns differently, and the DAG
	// gather can carry extra upstream columns; value parity on the
	// requested columns is the correctness contract here (column-set/name
	// parity is tracked as a separate polish issue). Floats render at 12
	// significant digits: the single-process pipeline sums sequentially
	// while the DAG merges partial sums, so float aggregates legitimately
	// drift by a few ULP between the paths (same non-associativity as the
	// Q15 CTE float drift).
	canon := func(rows []map[string]any, cols []string, ordered bool) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			var sb strings.Builder
			for _, col := range cols {
				v, ok := r[col]
				if !ok {
					if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
						v = r[col[dot+1:]]
					}
				}
				switch f := v.(type) {
				case float64:
					fmt.Fprintf(&sb, "%.12g|", f)
				case float32:
					fmt.Fprintf(&sb, "%.6g|", f)
				default:
					fmt.Fprintf(&sb, "%v|", v)
				}
			}
			out[i] = sb.String()
		}
		if !ordered {
			sort.Strings(out)
		}
		return out
	}

	for i, q := range queries {
		fastRes, err := fast.ExecuteSQL(ctx, q.sql)
		if err != nil || fastRes.Error != "" {
			t.Fatalf("fast %s: %v %s", q.sql, err, fastRes.Error)
		}
		if got := fast.LocalFastPathHits(); got != int64(i+1) {
			t.Fatalf("%s: expected fast path hit %d, counter=%d (routed to DAG?)", q.sql, i+1, got)
		}
		dagRes, err := dag.ExecuteSQL(ctx, q.sql)
		if err != nil || dagRes.Error != "" {
			t.Fatalf("dag %s: %v %s", q.sql, err, dagRes.Error)
		}
		fr, dr := canon(mustRows(t, fastRes), q.cols, q.ordered), canon(mustRows(t, dagRes), q.cols, q.ordered)
		if len(fr) != len(dr) {
			t.Errorf("%s: row count fast=%d dag=%d", q.sql, len(fr), len(dr))
			continue
		}
		for j := range fr {
			if fr[j] != dr[j] {
				t.Errorf("%s: row %d differs\n fast: %s\n dag:  %s", q.sql, j, fr[j], dr[j])
				break
			}
		}
	}
	if dag.LocalFastPathHits() != 0 {
		t.Errorf("DAG-only coordinator took the fast path %d times", dag.LocalFastPathHits())
	}

	// A threshold smaller than the table forces the DAG even with the fast
	// path enabled.
	res, err := tiny.ExecuteSQL(ctx, "SELECT COUNT(*) AS c FROM lineitem")
	if err != nil || res.Error != "" {
		t.Fatalf("tiny threshold: %v %s", err, res.Error)
	}
	if tiny.LocalFastPathHits() != 0 {
		t.Errorf("over-threshold query took the fast path")
	}

	// Bare expression SELECT over a scan, checked against ground truth
	// computed from the source rows. This shape is differential-excluded:
	// the DAG's scan→gather path never computes SELECT-list expressions
	// (pre-existing bug, same family as the #163 gather-projection gap) —
	// the fast path is the one that gets it right.
	fastRes, err := fast.ExecuteSQL(ctx, "SELECT lower(l_shipmode) AS m, l_quantity * 2 AS q2 FROM lineitem WHERE l_orderkey < 10")
	if err != nil || fastRes.Error != "" {
		t.Fatalf("expression fast: %v %s", err, fastRes.Error)
	}
	var want []string
	for _, r := range rows {
		if r["l_orderkey"].(int32) < 10 {
			want = append(want, fmt.Sprintf("%v|%.12g|",
				strings.ToLower(r["l_shipmode"].(string)),
				r["l_quantity"].(float64)*2))
		}
	}
	sort.Strings(want)
	got := canon(mustRows(t, fastRes), []string{"m", "q2"}, false)
	if len(got) != len(want) {
		t.Fatalf("expression: got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expression row %d: got %s want %s", i, got[i], want[i])
		}
	}
}
