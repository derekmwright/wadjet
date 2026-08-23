package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The distributed half of the type-coverage gates.
//
// TestTypeMatrixTwoPath answers the whole type-matrix corpus twice — once on
// the single-process engine and once on the stage DAG, over the same rows —
// and requires the two to agree. That is the two-path invariance contract
// (benchmarks/tpch/two_path_invariance_test.go) applied to the 19 types the
// TPC-H corpus does not have, and it reaches what the single-process gates
// cannot: the WSHF shuffle encoding, the partial→final aggregate merge, the
// gather, and the coordinator-side result assembly, per type.
//
// Pins follow ADR-0013: the comparison runs for a pinned entry, a divergence
// is logged, and a pin whose issue stops diverging anywhere FAILS.

// tmdPins carries #407 (project_c_vec): the SINGLE-PROCESS arm answers NULL
// for every row of a VECTOR column; the stage DAG answers the values. #392
// (MIN_BY/MAX_BY's declared output type), #394 (DECIMAL ordering) and #396
// (network and UUID columns in raw storage form, 37+ entries) are all fixed,
// so nothing else here diverges by VALUE. tmdRawFormReason and
// tmdMinByTypeReason go with them — nothing references either string now.
var tmdPins = map[string]typematrix.Pin{
	// #407 — the SINGLE-PROCESS arm answers NULL for every row of a VECTOR
	// column; the stage DAG answers the values. Found the moment #397's encoder
	// half landed: while the DAG refused the query the comparison never ran, so
	// nothing had ever checked the single-process side of it. The DAG is the
	// right arm here.
	"project_c_vec": {Issue: "#407", Reason: "Reading a VECTOR column returns NULL for every row on the " +
		"single-process engine (both the native columnar decode and the scanner's ROW fallback); the stage " +
		"DAG returns the values. ROW on the same fixture row reads back fine, so this is VECTOR-specific."},
	// #392's twelve pins (the scalar-form asymmetry over c_cidr/c_ipv6/c_mac/
	// c_uuid, and the grouped form over the same four types plus minby/maxby)
	// are NOT added: #392 was fixed at the root on main before this rebase
	// (minMaxDeclaredType now derives MIN_BY/MAX_BY's output type from the
	// value's own type instead of falling through to FLOAT64) — pending a
	// gate run to confirm all twelve now agree on both arms.
}

// tmdWholeTableMapScanReason is the mechanism behind the #410 entries left in
// tmdUnsupported. It is NOT the exchange: #397 is fixed (the WSHF format
// carries ARRAY/ROW/MAP/VECTOR since the container codec landed) and these
// queries never reach the encoder. #397 reported this as its "second face"
// and misattributed it; #410 carries the real mechanism.
const tmdWholeTableMapScanReason = "A stage whose fragment carries no column " +
	"projection (spec.Columns empty, internal/worker/executor_fragment.go:2582) scans the " +
	"WHOLE typemx_nested table, which pulls in c_map. Any ARRAY or MAP column routes the " +
	"read through the scanner's ROW fallback (scan.HasUnsupportedColumnarTypes → " +
	"readFileBatchesViaRows, internal/engine/scan/columnar.go:47), and there batch.FromRows " +
	"hands Vector.SetValue the parquet reader's map[string]any for the MAP column while the " +
	"MAP arm accepts only the list-of-entries forms — the #361 guard fires " +
	"(\"cannot store map[string]interface {} into MAP vector\"). That is #393's mechanism " +
	"reached from the DAG: the single-process arm projects the MAP column away and never " +
	"reads it. Fixing it means either a MAP arm for the row fallback (#393, whose crash pins " +
	"in tmdCrashPins/wadjet gate it) or a projection on that stage (planner), not the exchange."

var tmdUnsupported = map[string]typematrix.Pin{
	// #397's shuffle-encoder gap (ARRAY/ROW/VECTOR through a stage boundary,
	// project_c_arr/project_c_row/project_c_vec/window_c_arr/window_c_row/
	// window_c_vec) is fixed — the container codec lands the payload on the
	// wire. These remaining entries were not part of that commit's own
	// six-entry proof list; pending a gate re-run to confirm the same fix
	// closes them too (they hit the identical encoder, just from the MAP leaf
	// and the aggregate-gather writer rather than the plain shuffle writer).
	"project_c_map": {Issue: "#397", Reason: "MAP column through a shuffle: WSHF has no encoder arm."},
	"window_c_map":  {Issue: "#397", Reason: "MAP column through a shuffle: WSHF has no encoder arm."},

	// #397, same encoder gap hit from the OTHER shuffle writer: MIN_BY/MAX_BY's
	// grouped and scalar forms carry the winning value through the final
	// aggregate's GATHER, which is the same WSHF family and has the same
	// missing arms for ARRAY/ROW/MAP. Newly comparable now that #392 no
	// longer crashes the process before either arm can be compared.
	"minby_c_arr":        {Issue: "#397", Reason: "ARRAY column through the aggregate gather: WSHF has no encoder arm."},
	"maxby_c_arr":        {Issue: "#397", Reason: "ARRAY column through the aggregate gather: WSHF has no encoder arm."},
	"minby_scalar_c_arr": {Issue: "#397", Reason: "ARRAY column through the aggregate gather: WSHF has no encoder arm."},
	"minby_c_row":        {Issue: "#397", Reason: "ROW column through the aggregate gather: WSHF has no encoder arm."},
	"maxby_c_row":        {Issue: "#397", Reason: "ROW column through the aggregate gather: WSHF has no encoder arm."},
	"minby_scalar_c_row": {Issue: "#397", Reason: "ROW column through the aggregate gather: WSHF has no encoder arm."},
	"minby_c_map":        {Issue: "#397", Reason: "MAP column through the aggregate gather: WSHF has no encoder arm."},
	"maxby_c_map":        {Issue: "#397", Reason: "MAP column through the aggregate gather: WSHF has no encoder arm."},
	"minby_scalar_c_map": {Issue: "#397", Reason: "MAP column through the aggregate gather: WSHF has no encoder arm."},

	// wide_row_nested is SELECT * over the nested-schema table (ARRAY, ROW,
	// MAP and VECTOR columns all present), so it hits the same scan-side WSHF
	// encoder gap as project_c_arr/project_c_row/project_c_map/project_c_vec
	// at once — the widest possible blast radius for #397, the same way it was
	// #393's widest blast radius before that one was fixed.
	"wide_row_nested": {Issue: "#397", Reason: "SELECT * over a table with ARRAY/ROW/MAP/VECTOR columns: WSHF has no encoder arm for any of them."},

	// #410 — split out of #397, which reported this as "the exchange hands the
	// receiver a MAP vector". The stack says otherwise; see the reason.
	"flat_nested_join":   {Issue: "#410", Reason: tmdWholeTableMapScanReason},
	"maxby_c_vec":        {Issue: "#410", Reason: tmdWholeTableMapScanReason},
	"minby_c_vec":        {Issue: "#410", Reason: tmdWholeTableMapScanReason},
	"minby_scalar_c_vec": {Issue: "#410", Reason: tmdWholeTableMapScanReason},
}

// tmdPinPrefixes pins every corpus entry that begins with the key, for a
// defect that is a property of a TYPE rather than of one query shape.
var tmdPinPrefixes = map[string]typematrix.Pin{}

func TestTypeMatrixTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the type-matrix two-path gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	corpus := typematrix.Corpus()
	t.Logf("type-matrix two-path gate: %d queries × 2 arms (A single-process, B stage DAG)", len(corpus))

	ratchet := newTmdRatchet()
	var compared, diverged, refused int
	for _, q := range corpus {
		q := q
		t.Run(q.Name, func(t *testing.T) {
			if p, ok := tmdCrashPins[q.Name]; ok {
				t.Skipf("process killer, tracked in %s — gated by "+
					"wadjet.TestTypeMatrixNoProcessKillers:\n  %s", p.Issue, p.Reason)
			}
			aRes, aErr := tmdRunSingle(ctx, single, q.SQL)
			bRes, bErr := tmdRunDAG(ctx, coord, q.SQL)

			pin, pinKey, pinned := tmdPinFor(q.Name)
			fail := func(format string, args ...any) {
				if pinned {
					ratchet.observe(pinKey, true)
					t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  "+format,
						append([]any{pin.Issue, pin.Reason}, args...)...)
					return
				}
				t.Errorf(format, args...)
			}

			if aErr != nil && bErr != nil {
				// Both refuse: not a divergence. Still ratcheted so the list
				// of what the DAG cannot do stays honest.
				refused++
				if p, ok := tmdUnsupported[q.Name]; ok {
					ratchet.observe(q.Name, true)
					t.Skipf("both arms refuse this shape (%s): %v", p.Issue, aErr)
				}
				t.Skipf("both arms refuse this shape: single=%v dag=%v", aErr, bErr)
			}
			if aErr != nil {
				fail("the single-process arm FAILED on a query the stage DAG answered (%d rows): %v\n  SQL: %s",
					len(bRes.Rows), aErr, q.SQL)
				return
			}
			if bErr != nil {
				if p, ok := tmdUnsupported[q.Name]; ok {
					ratchet.observe(q.Name, true)
					t.Skipf("the stage DAG refuses this shape (%s): %v", p.Issue, bErr)
				}
				fail("the stage DAG FAILED on a query the single-process engine answered (%d rows): %v\n  SQL: %s",
					len(aRes.Rows), bErr, q.SQL)
				return
			}
			if p, ok := tmdUnsupported[q.Name]; ok {
				t.Errorf("%q is listed as unsupported (%s) but BOTH arms answered it, so the "+
					"limitation is gone. Delete the entry from tmdUnsupported.", q.Name, p.Issue)
			}
			compared++
			if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: q.Mode}); diff != "" {
				diverged++
				fail("TWO-PATH DIVERGENCE: the stage DAG and the single-process engine answer "+
					"this query differently\n  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					q.SQL, diff, tmdRender(aRes, 3), tmdRender(bRes, 3))
				return
			}
			if pinned {
				ratchet.observe(pinKey, false)
			}
		})
	}
	ratchet.finish(t, tmdPinPrefixes, "two-path prefix")
	ratchet.finish(t, tmdPins, "two-path")
	ratchet.finish(t, tmdUnsupported, "two-path unsupported")
	t.Logf("type-matrix two-path gate: %d compared, %d diverged, %d refused by both arms",
		compared, diverged, refused)
	if compared == 0 {
		t.Error("no corpus entry was compared on both arms — the gate proves nothing")
	}
}

// tmdCrashPins mirrors wadjet.tmCrashPins: entries that take a process down
// cannot be compared here either. The wadjet-side gate owns the ratchet, so
// this list carries no independent claim — it exists so this suite does not
// die on the first one.
//
// #392 and #393 are both fixed at the root, and independently #400 and this
// commit wrapped all three goroutines that had no recover (the parallel-emit
// aggregate, the scan workers, and the fragment source pump) so nothing in
// the corpus kills a process any more either. The map is empty rather than
// removed so the mechanism stays in place for the next crash-pinned defect.
// wadjet.tmCrashPins is empty for the same reasons and owns that ratchet.
var tmdCrashPins = map[string]typematrix.Pin{}

func tmdPinFor(name string) (typematrix.Pin, string, bool) {
	if p, ok := tmdPins[name]; ok {
		return p, name, true
	}
	for prefix, p := range tmdPinPrefixes {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			return p, prefix, true
		}
	}
	return typematrix.Pin{}, "", false
}

// tmdRatchet is the same two-way ratchet the wadjet-side gates use.
type tmdRatchet struct{ covered, diverged map[string]bool }

func newTmdRatchet() *tmdRatchet {
	return &tmdRatchet{covered: map[string]bool{}, diverged: map[string]bool{}}
}

func (r *tmdRatchet) observe(key string, diverged bool) {
	r.covered[key] = true
	if diverged {
		r.diverged[key] = true
	}
}

// finish's ratchet is per ISSUE, not per pin (ADR-0013 amendment 2026-08-23):
// an issue with many pins — #396 has 37 — can have most of them silently
// start agreeing while the ratchet stays green on the strength of one still
// diverging. finish() also logs a "k of m pins still diverge" summary per
// issue on every run, so a real fix narrowing 37/37 to 1/37 is visible in the
// test log long before the last pin agrees and the ratchet actually fires.
func (r *tmdRatchet) finish(t *testing.T, pins map[string]typematrix.Pin, kind string) {
	t.Helper()
	keys := make([]string, 0, len(pins))
	for k := range pins {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	issueDiverged := map[string]bool{}
	issueReason := map[string]string{}
	issueTotal := map[string]int{}
	issueDivergedCount := map[string]int{}
	var issues []string
	for _, k := range keys {
		p := pins[k]
		if !r.covered[k] {
			t.Errorf("%s pin %q (%s) matches no corpus entry — it exempts nothing and hides "+
				"nothing. Delete it or fix the name.", kind, k, p.Issue)
			continue
		}
		if _, seen := issueTotal[p.Issue]; !seen {
			issues = append(issues, p.Issue)
		}
		issueTotal[p.Issue]++
		if r.diverged[k] {
			issueDivergedCount[p.Issue]++
			issueDiverged[p.Issue] = true
		}
		if p.GatedBy != "" {
			issueDiverged[p.Issue] = true
		} else if issueReason[p.Issue] == "" {
			issueReason[p.Issue] = p.Reason
		}
	}
	sort.Strings(issues)
	for _, issue := range issues {
		t.Logf("%s issue %s: %d of %d pins still diverge this run", kind, issue,
			issueDivergedCount[issue], issueTotal[issue])
		if reason := issueReason[issue]; reason != "" && !issueDiverged[issue] {
			t.Errorf("every %s pin for %s now AGREES, so it is FIXED:\n  %s\nDelete those pins.",
				kind, issue, reason)
		}
	}
}

func tmdRunSingle(ctx context.Context, db *wadjet.DB, sql string) (res *oracle.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := db.Query(ctx, sql)
	if qerr != nil {
		return nil, qerr
	}
	return &oracle.Result{Columns: out.Columns, Rows: out.Rows}, nil
}

func tmdRunDAG(ctx context.Context, coord *Coordinator, sql string) (res *oracle.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := coord.ExecuteSQL(ctx, sql)
	if qerr != nil {
		return nil, qerr
	}
	if out.Error != "" {
		return nil, fmt.Errorf("%s", out.Error)
	}
	rows, rerr := out.Rows()
	if rerr != nil {
		return nil, fmt.Errorf("materializing distributed rows: %w", rerr)
	}
	return &oracle.Result{Columns: out.Columns, Rows: rows}, nil
}

// tmdStandalone is arm A: the embedded single-process engine over the same
// deterministic fixture.
func tmdStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tmdTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

type tmdTable struct {
	name   string
	schema parquet.Schema
	rows   []map[string]any
}

func tmdTables() []tmdTable {
	return []tmdTable{
		{typematrix.Table, typematrix.Schema(), typematrix.Data(typematrix.Rows)},
		{typematrix.Nested, typematrix.NestedSchema(), typematrix.NestedData(typematrix.Rows)},
		{typematrix.Dim, typematrix.DimSchema(), typematrix.DimData()},
	}
}

// tmdCluster is arm B: embedded NATS + MemStore + NATS-KV catalog + three
// workers, and a coordinator with LocalFastPathBytes=0 so every query takes
// the stage DAG rather than the in-process fast path.
func tmdCluster(t *testing.T, ctx context.Context) *Coordinator {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	t.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("catalog kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	// Several files per table so the DAG really fans scans out across tasks —
	// a single-file table hides a per-task defect by accident.
	const chunks = 4
	for _, tbl := range tmdTables() {
		if err := cat.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		n := len(tbl.rows)
		per := (n + chunks - 1) / chunks
		var entries []catalog.FileEntry
		for c := 0; c < chunks; c++ {
			lo, hi := c*per, min(c*per+per, n)
			if lo >= hi {
				break
			}
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, tbl.schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("parquet writer %s: %v", tbl.name, err)
			}
			if err := pw.WriteRows(tbl.rows[lo:hi]); err != nil {
				t.Fatalf("write %s: %v", tbl.name, err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("close %s: %v", tbl.name, err)
			}
			path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tbl.name, c)
			payload := buf.Bytes()
			if _, err := store.Put(ctx, "test", path, bytes.NewReader(payload),
				int64(len(payload)), "application/octet-stream"); err != nil {
				t.Fatalf("put %s: %v", path, err)
			}
			entries = append(entries, catalog.FileEntry{
				Path: path, SizeBytes: int64(len(payload)),
				NumRows: int64(hi - lo), CreatedAt: time.Now(),
			})
		}
		if err := cat.AddFiles(ctx, tbl.name, map[string]string{}, "tables/"+tbl.name+"/", entries); err != nil {
			t.Fatalf("add files %s: %v", tbl.name, err)
		}
	}

	coord := New(Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test",
		// 0 disables the small-query fast path outright, which is what forces
		// every query onto the stage DAG regardless of how small the scan is.
		LocalFastPathBytes: 0,
	}, cat, nc, js, logger)

	const workers = 3
	ids := make([]string, workers)
	for i := range ids {
		ids[i] = fmt.Sprintf("typematrix-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID: ids[i], NATSUrl: embedded.ClientURL(),
			MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
		}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatalf("worker start: %v", err)
		}
		t.Cleanup(w.Stop)
	}

	// A worker enters the registry by heartbeat and the worker's own ticker is
	// on a 10s cadence; publish on their behalf so planning sees the cluster
	// immediately.
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, id := range ids {
			hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 4, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatalf("marshal heartbeat: %v", err)
			}
			if err := nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
				t.Fatalf("publish heartbeat: %v", err)
			}
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("nats flush: %v", err)
		}
		if coord.Workers().Count() >= workers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers never registered: %d, want %d", coord.Workers().Count(), workers)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return coord
}

func tmdRender(res *oracle.Result, n int) string {
	if res == nil {
		return "<no result>"
	}
	if len(res.Rows) == 0 {
		return "<0 rows>"
	}
	var sb bytes.Buffer
	fmt.Fprintf(&sb, "%d rows %v", len(res.Rows), res.Columns)
	for i, row := range res.Rows {
		if i >= n {
			fmt.Fprintf(&sb, "\n      ... %d more", len(res.Rows)-n)
			break
		}
		sb.WriteString("\n      ")
		for j, c := range res.Columns {
			if j > 0 {
				sb.WriteString(" | ")
			}
			if b, ok := row[c].([]byte); ok {
				fmt.Fprintf(&sb, "%s=%q", c, b)
				continue
			}
			fmt.Fprintf(&sb, "%s=%v", c, row[c])
		}
	}
	return sb.String()
}
