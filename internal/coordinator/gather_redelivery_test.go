// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

// #1709 kept regressions: a gather-streaming task that executes twice — a
// real JetStream AckWait redelivery, a Nak after failed result publishes, a
// duplicate publish, a worker restart with unacked work — must fail the
// statement loudly, never return duplicated or mixed rows. Built on real
// workers and a real JetStream; the only synthetic parts are test-only
// hooks that ORDER the two executions (hold a delivery unacked until the
// server redelivers it; make two executions overlap their streaming), never
// a hand-written second stream.
//
// At the base commit every conflict case below returned SUCCESS with
// duplicate rows (or with the right row count and one group duplicated and
// another missing); see the issue and the 1709 reproduction note.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ---------------------------------------------------------------------------
// Cluster and fixture
// ---------------------------------------------------------------------------

// rdCluster is the in-process cluster: embedded NATS + JetStream, MemStore,
// the NATS-KV catalog, real workers, every statement on the stage DAG.
type rdCluster struct {
	coord  *Coordinator
	js     jetstream.JetStream
	nc     *nats.Conn
	url    string
	store  *objstore.MemStore
	logger *slog.Logger
}

func (c *rdCluster) startWorker(t *testing.T, id string) *worker.Worker {
	t.Helper()
	w := worker.New(worker.Config{
		WorkerID: id, NATSUrl: c.url, MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
	}, c.store, c.nc, c.js, c.logger)
	wctx, wcancel := context.WithCancel(context.Background())
	t.Cleanup(wcancel)
	if err := w.Start(wctx); err != nil {
		t.Fatal(err)
	}
	return w
}

func rdWriteTable(t *testing.T, ctx context.Context, cat *catalog.Catalog, store *objstore.MemStore, name string, total, chunks int) {
	t.Helper()
	// name(k, g, v): k = 1..total, g = k % 7, v = k % 100 — the 1709 fixture
	// PostgreSQL was given by generate_series.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatal(err)
	}
	per := total / chunks
	for ch := 0; ch < chunks; ch++ {
		var rows []map[string]any
		for k := ch*per + 1; k <= (ch+1)*per; k++ {
			rows = append(rows, map[string]any{"k": int64(k), "g": int64(k % 7), "v": int64(k % 100)})
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
		fp := fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, ch)
		pd := buf.Bytes()
		if _, err := store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", []catalog.FileEntry{{
			Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(per), CreatedAt: time.Now(),
		}}); err != nil {
			t.Fatal(err)
		}
	}
}

func newRDCluster(t *testing.T, ctx context.Context, nWorkers int) *rdCluster {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	en, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
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
	rdWriteTable(t, ctx, cat, store, "t1", 2000, 4)
	rdWriteTable(t, ctx, cat, store, "t2", 6000, 6)
	c := &rdCluster{js: js, nc: nc, url: en.ClientURL(), store: store, logger: logger}
	for i := 0; i < nWorkers; i++ {
		w := c.startWorker(t, fmt.Sprintf("w%d", i))
		t.Cleanup(w.Stop)
	}
	c.coord = New(Config{NATSUrl: en.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	c.coord.config.LocalFastPathBytes = 0
	for i := 0; i < 3; i++ {
		c.coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
	}
	return c
}

// rdShortenAckWait sets AckWait on every intake consumer at the server,
// keeping the rest of Worker.Start's consumer config.
func rdShortenAckWait(t *testing.T, ctx context.Context, js jetstream.JetStream, d time.Duration) {
	t.Helper()
	for _, c := range []struct{ stream, name, filter string }{
		{distributed.StreamTasks, "tasks", distributed.SubjectTasksAll},
		{distributed.StreamPriTasks, "pritasks-leaf", "wadjet.pritasks.leaf.>"},
		{distributed.StreamPriTasks, "pritasks-deep", "wadjet.pritasks.deep.>"},
	} {
		if _, err := js.CreateOrUpdateConsumer(ctx, c.stream, jetstream.ConsumerConfig{
			Durable: c.name, FilterSubject: c.filter, AckPolicy: jetstream.AckExplicitPolicy,
			AckWait: d, MaxDeliver: 3,
		}); err != nil {
			t.Fatalf("shorten AckWait on %s: %v", c.name, err)
		}
	}
}

func rdRender(res *oracle.Result) []string {
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, len(res.Columns))
		for i, c := range res.Columns {
			parts[i] = fmt.Sprintf("%v", r[c])
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

// ---------------------------------------------------------------------------
// Shapes and their PostgreSQL 17.11 answers
// ---------------------------------------------------------------------------

type rdShape struct {
	name    string
	sql     string
	ordered bool // compare in order (ORDER BY); else as a multiset
	want    []string
	multi   bool // planned as a fused hash-partitioned stage (24 tasks)
}

func rdSeq(from, to int, f func(k int) string) []string {
	var out []string
	for k := from; k <= to; k++ {
		if s := f(k); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// rdShapes is every row of the 1709 reproduction's results table. The
// answers are PostgreSQL 17.11's (postgres:17-alpine, --locale=C
// --encoding=UTF8) over the same fixture.
func rdShapes() []rdShape {
	lowV := func(k int) string {
		if k%100 < 3 {
			return fmt.Sprintf("%d|%d", k, k%100)
		}
		return ""
	}
	return []rdShape{
		{name: "agg_groupby", multi: true, sql: `SELECT g, COUNT(*) AS c, SUM(v) AS s FROM t1 GROUP BY g`,
			want: []string{"0|285|14185", "1|286|14171", "2|286|14157", "3|286|14143", "4|286|14129", "5|286|14115", "6|285|14100"}},
		{name: "distinct", multi: true, sql: `SELECT DISTINCT g FROM t1`,
			want: []string{"0", "1", "2", "3", "4", "5", "6"}},
		{name: "groupby_cntdistinct", multi: true, sql: `SELECT g, COUNT(DISTINCT v) AS c FROM t1 GROUP BY g`,
			want: []string{"0|100", "1|100", "2|100", "3|100", "4|100", "5|100", "6|100"}},
		{name: "agg_global", sql: `SELECT COUNT(*) AS c, SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx FROM t1`,
			want: []string{"2000|99000|0|99"}},
		{name: "agg_minmax", sql: `SELECT MIN(v) AS mn, MAX(v) AS mx FROM t1`, want: []string{"0|99"}},
		{name: "count_distinct", sql: `SELECT COUNT(DISTINCT g) AS c FROM t1`, want: []string{"7"}},
		{name: "groupby_count", sql: `SELECT COUNT(*) AS c FROM (SELECT g, SUM(v) AS s FROM t1 GROUP BY g) x`, want: []string{"7"}},
		{name: "groupby_order", ordered: true, sql: `SELECT g, SUM(v) AS s FROM t1 GROUP BY g ORDER BY g`,
			want: []string{"0|14185", "1|14171", "2|14157", "3|14143", "4|14129", "5|14115", "6|14100"}},
		{name: "orderby", ordered: true, sql: `SELECT k, v FROM t1 WHERE v < 3 ORDER BY k`, want: rdSeq(1, 2000, lowV)},
		{name: "orderby_limit", ordered: true, sql: `SELECT k FROM t1 ORDER BY k LIMIT 5`, want: []string{"1", "2", "3", "4", "5"}},
		{name: "orderby_limit_offset", ordered: true, sql: `SELECT k FROM t1 ORDER BY k LIMIT 3 OFFSET 4`, want: []string{"5", "6", "7"}},
		{name: "scan_filter", sql: `SELECT k, v FROM t1 WHERE v < 3`, want: rdSeq(1, 2000, lowV)},
	}
}

// rdControlShapes are single-delivery controls beyond the table: output
// spanning several 2,048-row gather messages under ORDER BY … LIMIT/OFFSET,
// and legitimate duplicate SQL rows, which identity-aware acceptance must
// keep.
func rdControlShapes() []rdShape {
	return []rdShape{
		{name: "multibatch_limit_offset", ordered: true, sql: `SELECT k FROM t2 ORDER BY k LIMIT 5000 OFFSET 100`,
			want: rdSeq(101, 5100, func(k int) string { return fmt.Sprint(k) })},
		{name: "multibatch_desc_limit_offset", ordered: true, sql: `SELECT k, g FROM t2 ORDER BY k DESC LIMIT 4500 OFFSET 7`,
			want: func() []string {
				var out []string
				for k := 6000 - 7; k > 6000-7-4500; k-- {
					out = append(out, fmt.Sprintf("%d|%d", k, k%7))
				}
				return out
			}()},
		{name: "duplicate_rows", sql: `SELECT g FROM t1 WHERE v < 3`,
			want: rdSeq(1, 2000, func(k int) string {
				if k%100 < 3 {
					return fmt.Sprint(k % 7)
				}
				return ""
			})},
		{name: "duplicate_rows_ordered", ordered: true, sql: `SELECT v FROM t1 WHERE k <= 300 ORDER BY v`,
			want: func() []string {
				var out []string
				for v := 0; v < 100; v++ {
					for i := 0; i < 3; i++ {
						out = append(out, fmt.Sprint(v))
					}
				}
				return out
			}()},
	}
}

func rdAssertAnswer(t *testing.T, sh rdShape, res *oracle.Result) {
	t.Helper()
	got := rdRender(res)
	want := append([]string(nil), sh.want...)
	if !sh.ordered {
		got = append([]string(nil), got...)
		sort.Strings(got)
		sort.Strings(want)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		n := len(got)
		if n > 20 {
			n = 20
		}
		t.Fatalf("%s: %d rows, want %d (PostgreSQL 17.11); first rows %v", sh.name, len(got), len(want), got[:n])
	}
}

// ---------------------------------------------------------------------------
// The controller: orders two executions of one gather task "A"
// ---------------------------------------------------------------------------

type rdMode string

const (
	rdRedeliver   rdMode = "redeliver"   // JetStream AckWait redelivery of A
	rdEarly       rdMode = "early"       // redelivery + B's payload delayed past its result notification
	rdNak         rdMode = "nak"         // A's result publishes fail 3x → Nak → redelivery
	rdDupDispatch rdMode = "dupdispatch" // A published a second time
	rdRestart     rdMode = "restart"     // A's worker dies mid-stream; a restarted worker gets the redelivery
)

type rdCtl struct {
	t     *testing.T
	mode  rdMode
	multi bool
	start time.Time

	stop     chan struct{} // closed at test end: releases every hold
	stopOnce sync.Once

	mu         sync.Mutex
	seen       map[string]bool // distinct gather task ids delivered
	a, b       string
	aEntries   int
	a2Started  chan struct{}
	a2Once     sync.Once
	bRelease   chan struct{}
	bReleased  bool
	terminals  int
	finalizers int
	finBarrier chan struct{}
	aAtFinal   chan struct{} // restart: A1 reached Finalize (its rows are sent)
	aFinalOnce sync.Once
	aRefused   int                 // nak: A's result publish attempts seen
	execs      map[string][]string // coordinator-observed execution ids per task, in order
	republish  func(distributed.Task)
	holdB      bool // never release B: only the test's end does
}

func newRDCtl(t *testing.T, mode rdMode, multi bool) *rdCtl {
	return &rdCtl{
		t: t, mode: mode, multi: multi, start: time.Now(),
		stop: make(chan struct{}), seen: map[string]bool{},
		a2Started: make(chan struct{}), bRelease: make(chan struct{}),
		finBarrier: make(chan struct{}), aAtFinal: make(chan struct{}),
		execs: map[string][]string{},
	}
}

func (c *rdCtl) release() { c.stopOnce.Do(func() { close(c.stop) }) }

func (c *rdCtl) logf(format string, args ...any) {
	c.t.Logf("[%6.2fs] "+format, append([]any{time.Since(c.start).Seconds()}, args...)...)
}

func rdIsGatherTask(task *distributed.Task) bool {
	b, _ := json.Marshal(task)
	return strings.Contains(string(b), "wadjet.gather.")
}

func rdHasInputFiles(task *distributed.Task) bool {
	b, _ := json.Marshal(task)
	return strings.Contains(string(b), ".wshf") || strings.Contains(string(b), ".parquet")
}

func (c *rdCtl) wait(ch chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-c.stop:
		return false
	case <-time.After(d):
		return false
	}
}

func (c *rdCtl) onDelivery(task *distributed.Task, delivery uint64, workerID string, inProgress func()) {
	if !rdIsGatherTask(task) {
		return
	}
	c.mu.Lock()
	if delivery == 1 {
		c.seen[task.ID] = true
	}
	c.logf("delivery task=%s delivery=%d worker=%s", task.ID, delivery, workerID)
	if task.ID == c.a {
		c.aEntries++
		second := delivery >= 2 || (c.mode == rdDupDispatch && c.aEntries >= 2)
		c.mu.Unlock()
		if second && c.mode != rdNak && c.mode != rdRestart {
			c.a2Once.Do(func() { close(c.a2Started) })
		}
		return
	}
	switch {
	case c.a == "" && delivery == 1 && (!c.multi || rdHasInputFiles(task)):
		c.a = task.ID
		c.aEntries = 1
		c.logf("A=%s", task.ID)
		switch c.mode {
		case rdRedeliver, rdEarly:
			// Hold delivery 1 unacked, without InProgress, until the server
			// redelivers it at AckWait.
			ch := c.a2Started
			c.mu.Unlock()
			c.wait(ch, 60*time.Second)
			return
		case rdDupDispatch:
			ch := c.a2Started
			cp := *task
			c.mu.Unlock()
			go c.republish(cp)
			c.wait(ch, 60*time.Second)
			return
		}
		c.mu.Unlock()
		return
	case c.multi && c.b == "" && c.a != "" && delivery == 1 && rdHasInputFiles(task):
		// B stands for "another task of the stage still running": held,
		// with InProgress so it is never redelivered, until the coordinator
		// has counted as many terminals as distinct tasks were delivered.
		c.b = task.ID
		rel := c.bRelease
		c.logf("B=%s held", task.ID)
		c.mu.Unlock()
		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()
		deadline := time.After(60 * time.Second)
		for {
			select {
			case <-rel:
				return
			case <-c.stop:
				return
			case <-deadline:
				return
			case <-tk.C:
				inProgress()
			}
		}
	}
	c.mu.Unlock()
}

func (c *rdCtl) onFinalize(task distributed.Task) {
	c.mu.Lock()
	if task.ID != c.a {
		c.mu.Unlock()
		return
	}
	if c.mode == rdRestart {
		c.mu.Unlock()
		// A1's rows are out; it now "dies": no terminal, no ack, ever.
		first := false
		c.aFinalOnce.Do(func() { first = true; close(c.aAtFinal) })
		if first {
			<-c.stop
		}
		return
	}
	if c.multi || c.mode == rdNak {
		c.mu.Unlock()
		return
	}
	// Single-task forms: both executions stream their rows before either
	// sends its terminal.
	c.finalizers++
	n := c.finalizers
	bar := c.finBarrier
	if n >= 2 {
		close(bar)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.wait(bar, 15*time.Second)
}

func (c *rdCtl) onResultPublish(task distributed.Task, attempt int) error {
	if c.mode != rdNak {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.ID != c.a {
		return nil
	}
	// The first execution's three attempts fail; it Naks. Every later
	// attempt (the redelivered execution's) goes through.
	c.aRefused++
	if c.aRefused <= 3 {
		return fmt.Errorf("test: result request %d for task %s refused", attempt+1, task.ID)
	}
	return nil
}

func (c *rdCtl) onParsed(msg *distributed.GatherBatchMsg) {
	c.mu.Lock()
	if msg.TaskID != "" {
		ex := c.execs[msg.TaskID]
		known := false
		for _, e := range ex {
			if e == msg.ExecutionID {
				known = true
			}
		}
		if !known {
			c.execs[msg.TaskID] = append(ex, msg.ExecutionID)
		}
	}
	delayB := c.mode == rdEarly && c.bReleased && !msg.Terminal && msg.TaskID == c.b
	if msg.Terminal {
		c.terminals++
		if c.multi && !c.holdB && !c.bReleased && c.b != "" && c.terminals >= len(c.seen) {
			c.bReleased = true
			close(c.bRelease)
			c.logf("B released: terminals=%d distinct=%d", c.terminals, len(c.seen))
		}
	}
	c.mu.Unlock()
	if delayB {
		// Early completion: B's payload is held in the gather subscription
		// goroutine while its result notification proceeds.
		time.Sleep(3 * time.Second)
	}
}

func (c *rdCtl) install(t *testing.T) {
	worker.SetTaskDeliveryHookForTest(c.onDelivery)
	worker.SetGatherFinalizeHookForTest(c.onFinalize)
	worker.SetResultPublishHookForTest(c.onResultPublish)
	h := c.onParsed
	gatherParsedTestHook.Store(&h)
	t.Cleanup(func() {
		c.release()
		worker.SetTaskDeliveryHookForTest(nil)
		worker.SetGatherFinalizeHookForTest(nil)
		worker.SetResultPublishHookForTest(nil)
		gatherParsedTestHook.Store(nil)
	})
}

// rdAssertConflict: the statement failed loudly with the conflict naming
// task A and both of its execution ids, under the internal-error SQLSTATE,
// and returned no rows.
func rdAssertConflict(t *testing.T, c *rdCtl, res *oracle.Result, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("statement SUCCEEDED with %d rows %v; want an explicit conflicting-execution error", len(res.Rows), rdRender(res))
	}
	if res != nil {
		t.Fatalf("an error AND rows: %v", rdRender(res))
	}
	t.Logf("statement error (SQLSTATE %s): %v", sqlerr.StateOf(err), err)
	c.mu.Lock()
	a, execs := c.a, append([]string(nil), c.execs[c.a]...)
	c.mu.Unlock()
	msg := err.Error()
	if !strings.Contains(msg, "task "+a+" executed more than once") {
		t.Fatalf("error %q does not name task %s's conflicting execution", msg, a)
	}
	if len(execs) < 2 {
		t.Fatalf("coordinator observed %d execution(s) of %s; the construction did not produce two", len(execs), a)
	}
	for _, e := range execs[:2] {
		if !strings.Contains(msg, e) {
			t.Errorf("error %q does not name execution %s", msg, e)
		}
	}
	if code := sqlerr.StateOf(err); code != "XX000" {
		t.Errorf("SQLSTATE = %q, want XX000", code)
	}
}

func rdShapeByName(name string) rdShape {
	for _, s := range rdShapes() {
		if s.name == name {
			return s
		}
	}
	panic("unknown shape " + name)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestGatherRedelivery_SingleDeliveryMatchesPostgres: the controls. Every
// shape, on single delivery, equals PostgreSQL 17.11 exactly.
func TestGatherRedelivery_SingleDeliveryMatchesPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cl := newRDCluster(t, ctx, 3)
	var batches, maxSeq atomic.Int64
	h := func(msg *distributed.GatherBatchMsg) {
		if !msg.Terminal {
			batches.Add(1)
			for {
				cur := maxSeq.Load()
				if int64(msg.Seq) <= cur || maxSeq.CompareAndSwap(cur, int64(msg.Seq)) {
					break
				}
			}
		}
	}
	gatherParsedTestHook.Store(&h)
	t.Cleanup(func() { gatherParsedTestHook.Store(nil) })
	for _, sh := range append(rdShapes(), rdControlShapes()...) {
		t.Run(sh.name, func(t *testing.T) {
			batches.Store(0)
			maxSeq.Store(0)
			res, err := tmdRunDAG(ctx, cl.coord, sh.sql)
			if err != nil {
				t.Fatalf("%s: %v", sh.name, err)
			}
			rdAssertAnswer(t, sh, res)
			// The multi-batch controls really stream several 2,048-row
			// messages from one execution, numbered in sequence.
			if strings.HasPrefix(sh.name, "multibatch") && (batches.Load() < 3 || maxSeq.Load() < 3) {
				t.Fatalf("%s: %d gather batch messages (max seq %d); want one execution streaming ≥3", sh.name, batches.Load(), maxSeq.Load())
			}
			t.Logf("%s: %d rows = PostgreSQL 17.11; %d gather batch messages, max seq %d", sh.name, len(res.Rows), batches.Load(), maxSeq.Load())
		})
	}
}

// TestGatherRedelivery_AckWaitRedelivery: every shape of the results table,
// a real AckWait redelivery of one gather task, both executions running to
// completion. Multi-task forms on three workers and on one (same-worker
// overlap); single-task forms with both executions streaming before either
// terminal.
func TestGatherRedelivery_AckWaitRedelivery(t *testing.T) {
	type run struct {
		shape   string
		workers int
		mode    rdMode
	}
	var runs []run
	for _, sh := range rdShapes() {
		runs = append(runs, run{sh.name, 3, rdRedeliver})
	}
	runs = append(runs,
		run{"agg_groupby", 1, rdRedeliver}, // same-worker overlap, 24 tasks
		run{"agg_global", 1, rdRedeliver},  // same-worker overlap, 1 task
		run{"agg_groupby", 3, rdEarly},     // duplicate terminal in place of a running task
	)
	for _, r := range runs {
		sh := rdShapeByName(r.shape)
		t.Run(fmt.Sprintf("%s/%s/workers=%d", r.mode, r.shape, r.workers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			c := newRDCtl(t, r.mode, sh.multi)
			c.install(t)
			cl := newRDCluster(t, ctx, r.workers)
			rdShortenAckWait(t, ctx, cl.js, 3*time.Second)
			res, err := tmdRunDAG(ctx, cl.coord, sh.sql)
			c.release()
			rdAssertConflict(t, c, res, err)
		})
	}
}

// TestGatherRedelivery_ConflictInterruptsDAGWait: the fused path waits for
// the whole DAG before it reads the gather result. With task B of the stage
// held running (for up to 60 s) the conflict must still reach the client at
// once — delivered to the statement's cancellation path, not read after the
// DAG finishes.
func TestGatherRedelivery_ConflictInterruptsDAGWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	sh := rdShapeByName("agg_groupby")
	c := newRDCtl(t, rdRedeliver, true)
	c.holdB = true
	c.install(t)
	cl := newRDCluster(t, ctx, 3)
	rdShortenAckWait(t, ctx, cl.js, 3*time.Second)
	start := time.Now()
	res, err := tmdRunDAG(ctx, cl.coord, sh.sql)
	took := time.Since(start)
	c.mu.Lock()
	b := c.b
	c.mu.Unlock()
	c.release()
	if b == "" {
		t.Fatal("construction: no task B was held")
	}
	rdAssertConflict(t, c, res, err)
	if took > 20*time.Second {
		t.Fatalf("the conflict reached the client after %v, with B still held: it waited for the DAG", took)
	}
	t.Logf("conflict returned after %v with task %s still running", took.Round(time.Millisecond), b)
}

// TestGatherRedelivery_NakAfterFailedResultPublish: the worker's own Nak —
// three failed result requests after the first execution already streamed
// its rows and terminal — redelivers the task with no AckWait involved.
// On a 24-task stage the second execution streams while another task still
// runs: a loud conflict. On a single-task gather the first execution had
// already completed the statement, so the duplicate is discarded and the
// answer is PostgreSQL's.
func TestGatherRedelivery_NakAfterFailedResultPublish(t *testing.T) {
	for _, tc := range []struct {
		shape    string
		conflict bool
		// redelivered: the statement is still waiting when the Nak lands,
		// so the second execution really runs. The standalone gather never
		// reads result notifications: it returned on the first execution's
		// terminal, and the redelivery of a finished query is Termed at
		// intake, so it shows only the refused publishes.
		redelivered bool
	}{
		{"agg_groupby", true, true},
		{"agg_global", false, true},
		{"scan_filter", false, false},
	} {
		sh := rdShapeByName(tc.shape)
		t.Run(tc.shape, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			c := newRDCtl(t, rdNak, sh.multi)
			c.install(t)
			cl := newRDCluster(t, ctx, 3)
			res, err := tmdRunDAG(ctx, cl.coord, sh.sql)
			if tc.conflict {
				c.release()
				rdAssertConflict(t, c, res, err)
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", sh.name, err)
			}
			rdAssertAnswer(t, sh, res)
			// The construction really ran: A's first execution's three
			// result requests were refused (its Nak), and — where the
			// statement was still waiting — a second execution was delivered.
			ran := func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.aRefused >= 3 && (!tc.redelivered || c.aEntries >= 2)
			}
			deadline := time.Now().Add(30 * time.Second)
			for !ran() && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
			}
			c.mu.Lock()
			refused, entries := c.aRefused, c.aEntries
			c.mu.Unlock()
			if !ran() {
				t.Fatalf("construction did not run: %d refused publishes, %d deliveries of A", refused, entries)
			}
			t.Logf("answer = PostgreSQL's; A: %d result-request attempts (the first 3 refused), %d deliveries", refused, entries)
			c.release()
		})
	}
}

// TestGatherRedelivery_DuplicateDispatch: the same task published twice — no
// delivery count can tell the copies apart.
func TestGatherRedelivery_DuplicateDispatch(t *testing.T) {
	for _, name := range []string{"agg_groupby", "orderby"} {
		sh := rdShapeByName(name)
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			c := newRDCtl(t, rdDupDispatch, sh.multi)
			c.install(t)
			cl := newRDCluster(t, ctx, 3)
			c.republish = func(task distributed.Task) {
				if err := cl.coord.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
					t.Errorf("republishing %s: %v", task.ID, err)
				}
			}
			res, err := tmdRunDAG(ctx, cl.coord, sh.sql)
			c.release()
			rdAssertConflict(t, c, res, err)
		})
	}
}

// TestGatherRedelivery_WorkerRestartWithUnackedWork: A's first execution
// streams its rows and its worker dies before the terminal and the ack; a
// restarted worker (same worker id) receives the redelivery. The partial
// first execution must not be completed by the replacement's terminal.
func TestGatherRedelivery_WorkerRestartWithUnackedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	sh := rdShapeByName("agg_global")
	c := newRDCtl(t, rdRestart, false)
	c.install(t)
	cl := newRDCluster(t, ctx, 0)
	first := cl.startWorker(t, "w0")
	rdShortenAckWait(t, ctx, cl.js, 3*time.Second)
	go func() {
		if !c.wait(c.aAtFinal, 60*time.Second) {
			return
		}
		select {
		case <-c.stop:
			return
		default:
		}
		c.logf("A1 streamed its rows; worker w0 dies")
		go first.Stop() // stops consuming at once; returns after the test releases A1
		w := cl.startWorker(t, "w0")
		t.Cleanup(w.Stop)
		rdShortenAckWait(t, ctx, cl.js, 3*time.Second)
		c.logf("worker w0 restarted")
	}()
	res, err := tmdRunDAG(ctx, cl.coord, sh.sql)
	c.release()
	rdAssertConflict(t, c, res, err)
}
