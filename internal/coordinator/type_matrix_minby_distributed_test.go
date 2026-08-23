package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The distributed half of the #392 / #393 gate: the same corpus run in a
// single-process wadjet.DB and on the stage DAG, compared row for row.
//
// Two paths matter here for two different reasons. #392's crash was in
// exec.HashAggregate, which BOTH paths run — but the two declare the
// aggregate's output type in different places (the planner's AggSpec for
// the pipeline, the wire AggSpec for the worker), and a fix that reaches
// only one of them is the #353/#354/#371 shape. #393's crash was in the
// scan, which both paths also run, one of them inside a worker process
// where a panic is a lost task rather than a lost query.

const tmdRows = 3000

func tmdSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "c_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "c_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "c_str", Type: parquet.TypeString, Nullable: true},
		{Name: "c_bytes", Type: parquet.TypeBytes, Nullable: true},
		{Name: "c_ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "c_dur", Type: parquet.TypeDuration, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "c_date", Type: parquet.TypeDate, Nullable: true},
		{Name: "c_dec", Type: parquet.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
		{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 4},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "c_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
	}}
}

func tmdData(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{"id": int64(i), "g": int32(i % 3)}
		put := func(name string, stride int, v any) {
			if i%stride == stride-1 {
				r[name] = nil
				return
			}
			r[name] = v
		}
		put("c_bool", 23, i%3 == 0)
		put("c_i32", 29, int32(i*3))
		put("c_i64", 31, int64(i)*1_000_003)
		put("c_f32", 37, float32(i)/7)
		put("c_f64", 41, float64(i)/3)
		put("c_str", 43, fmt.Sprintf("s-%06d", i))
		put("c_bytes", 47, []byte(fmt.Sprintf("bytes-%06d", i)))
		put("c_ts", 53, int64(1_700_000_000_000+int64(i)*61_000))
		put("c_ipv4", 59, fmt.Sprintf("10.0.%d.%d", (i/256)%256, i%256))
		put("c_ipv6", 61, fmt.Sprintf("2001:db8::%x", i))
		put("c_cidr", 67, fmt.Sprintf("192.168.%d.0/24", i%256))
		put("c_mac", 71, fmt.Sprintf("aa:bb:cc:00:%02x:%02x", (i/256)%256, i%256))
		put("c_port", 73, int32(1024+i%40000))
		put("c_proto", 79, int32(i%256))
		put("c_dur", 83, int64(i)*1_000_000)
		put("c_uuid", 89, fmt.Sprintf("00000000-0000-4000-8000-%012x", i))
		put("c_date", 97, fmt.Sprintf("20%02d-%02d-%02d", 10+i%15, 1+i%12, 1+i%28))
		put("c_dec", 101, float64(i)+0.0001*float64(i%9973))
		put("c_vec", 113, []float32{float32(i), float32(i) + 0.5, -float32(i), 0.25})
		put("c_arr", 103, []any{fmt.Sprintf("a%05d", i)})
		put("c_row", 107, map[string]any{"a": fmt.Sprintf("r-%05d", i), "b": int64(i) * 11})
		switch i % 4 {
		case 0:
			put("c_map", 109, map[string]any{})
		case 1:
			put("c_map", 109, map[string]any{fmt.Sprintf("k%d", i%5): int64(i)})
		case 2:
			put("c_map", 109, map[string]any{"a": int64(i), "b": int64(i) * 2})
		default:
			put("c_map", 109, map[string]any{"nil": nil})
		}
		rows[i] = r
	}
	return rows
}

// tmdQuery is one corpus entry: the SQL, and the pin when this entry is
// KNOWN to diverge for a reason that is not #392 or #393.
type tmdQuery struct {
	sql string
	// pin names the tracking issue for a divergence that is not fixed here.
	// The comparison still RUNS: a pinned entry that stops diverging fails,
	// so deleting the pin is the whole of "the fix landed" (ADR-0013 §Pins).
	// It is not a skip.
	pin string
	// why says what diverges, in one line, so a reader of a failure does not
	// have to open the issue to know whether it is the same thing.
	why string
}

// tmdColPin returns the pin covering every shape over one column.
//
// #397 — ARRAY/ROW/MAP/VECTOR had no arm in the shuffle/gather encoder
// (worker/shuffle_format.go:415), so any distributed query carrying one
// failed the task outright — is fixed: the container codec
// (internal/engine/batch/container_codec.go) carries all four types across
// both the WSHF shuffle and the aggregate gather now. Every c_arr/c_row/
// c_map/c_vec entry in this corpus, and the SELECT * below, agrees on both
// paths.
//
// #396 — IPv4/IPv6/MAC/UUID arriving at the coordinator typed as their
// STORAGE form (int64, 16 bytes) instead of their display form — is fixed:
// the NativeWriter now stamps the declared schema into the footer, and the
// reader restores each leaf's real type identity from it. All twelve
// c_ipv4/c_ipv6/c_mac/c_uuid entries agree on both paths now.
func tmdColPin(col string) (string, string) {
	return "", ""
}

// tmdCorpus is the query corpus: MIN_BY/MAX_BY over every type in both the
// scalar and grouped shapes, the projection of every type, and the SELECT *
// that #393 killed.
func tmdCorpus() []tmdQuery {
	var out []tmdQuery
	for _, c := range tmdSchema().Columns[2:] {
		pin, why := tmdColPin(c.Name)
		for _, sql := range []string{
			fmt.Sprintf("SELECT MIN_BY(%s, id) AS lo, MAX_BY(%s, id) AS hi FROM tmatrix", c.Name, c.Name),
			fmt.Sprintf("SELECT g, MIN_BY(%s, id) AS lo, MAX_BY(%s, id) AS hi FROM tmatrix GROUP BY g ORDER BY g",
				c.Name, c.Name),
			fmt.Sprintf("SELECT id, %s AS v FROM tmatrix WHERE id %% 331 = 7 ORDER BY id", c.Name),
		} {
			out = append(out, tmdQuery{sql: sql, pin: pin, why: why})
		}
	}
	// SELECT * carried the containers, so it was pinned to the same encoder
	// gap; #397 is fixed (see tmdColPin), so this now agrees unpinned too.
	return append(out, tmdQuery{
		sql: "SELECT * FROM tmatrix WHERE id % 743 = 5 ORDER BY id",
	})
}

func TestTypeMatrixMinByTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the two-path type matrix starts an embedded NATS and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
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

	schema := tmdSchema()
	rows := tmdData(tmdRows)
	if err := cat.CreateTable(ctx, "tmatrix", schema, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	// Three chunks so the scan fans out across the workers.
	const chunks = 3
	per := (len(rows) + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, (c+1)*per
		if hi > len(rows) {
			hi = len(rows)
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/tmatrix/chunk_%04d.parquet", c)
		data := buf.Bytes()
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(data)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
		})
	}
	if err := cat.AddFiles(ctx, "tmatrix", map[string]string{}, "tables/tmatrix/", entries); err != nil {
		t.Fatal(err)
	}

	const wantWorkers = 3
	for i := 0; i < wantWorkers; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
			SpillDir:      t.TempDir(),
		}, store, nc, js, logger)
		workerCtx, workerCancel := context.WithCancel(context.Background())
		t.Cleanup(workerCancel)
		if err := w.Start(workerCtx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
	}
	coord := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && coord.workers.Count() < wantWorkers {
		time.Sleep(50 * time.Millisecond)
	}
	if got := coord.workers.Count(); got < wantWorkers {
		t.Fatalf("workers failed to register (count=%d, want=%d)", got, wantWorkers)
	}

	// The single-process arm over the identical rows.
	sdb, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "single"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sdb.Close() })
	if err := sdb.CreateTable(ctx, "tmatrix", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := sdb.NewIngester("tmatrix", schema, nil, ingest.Config{MaxBufferRows: tmdRows + 1, RowGroupSize: 700})
	if err := ing.Ingest(ctx, tmdData(tmdRows)); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	for _, q := range tmdCorpus() {
		q := q
		t.Run(tmdName(q.sql), func(t *testing.T) {
			diff := tmdCompare(ctx, t, sdb, coord, q.sql)
			if q.pin == "" {
				if diff != "" {
					t.Errorf("%s\n%s", q.sql, diff)
				}
				return
			}
			if diff == "" {
				t.Fatalf("pinned entry now AGREES on both paths — %s is fixed, delete the pin:\n%s\n(pin: %s — %s)",
					q.pin, q.sql, q.pin, q.why)
			}
			t.Logf("known divergence %s (%s):\n%s", q.pin, q.why, diff)
		})
	}
}

// tmdCompare runs one query on both arms and returns the divergence, "" when
// they agree. One arm erroring where the other answers IS a divergence: that
// is the shape of both open pins.
func tmdCompare(ctx context.Context, t *testing.T, sdb *wadjet.DB, coord *Coordinator, sql string) string {
	t.Helper()
	sres, serr := sdb.Query(ctx, sql)
	dres, derr := coord.ExecuteSQL(ctx, sql)
	if derr == nil && dres.Error != "" {
		derr = fmt.Errorf("%s", dres.Error)
	}
	switch {
	case serr != nil && derr != nil:
		// Both refuse. Not a two-path divergence — but not an acceptable
		// answer for this corpus either, so say so loudly rather than
		// counting it as agreement.
		t.Fatalf("%s\nBOTH arms refused an ordinary query:\nsingle-process err=%v\ndistributed err=%v", sql, serr, derr)
	case serr != nil:
		return fmt.Sprintf("single-process errored where the DAG answered: %v", serr)
	case derr != nil:
		return fmt.Sprintf("the DAG errored where a single process answered: %v", derr)
	}
	dRows, err := dres.Rows()
	if err != nil {
		return fmt.Sprintf("reading distributed rows: %v", err)
	}
	cols := append([]string(nil), sres.Columns...)
	sc := oracle.CanonicalizeOrdered(&oracle.Result{Columns: cols, Rows: sres.Rows})
	dc := oracle.CanonicalizeOrdered(&oracle.Result{Columns: cols, Rows: dRows})
	if d := sc.Diff(dc, oracle.Query{Name: "two-path"}); d != "" {
		return fmt.Sprintf("(%d single-process rows vs %d distributed)\n%s", sc.Rows(), dc.Rows(), d)
	}
	return ""
}

// tmdName makes a stable subtest name out of a corpus query.
func tmdName(sql string) string {
	name := make([]rune, 0, 48)
	for _, r := range sql {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			name = append(name, r)
		default:
			name = append(name, '_')
		}
		if len(name) == 48 {
			break
		}
	}
	return string(name)
}
