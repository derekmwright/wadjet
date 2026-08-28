package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"sort"
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

// #622: a FULL/LEFT/RIGHT OUTER JOIN with a non-equi ON, feeding a scalar
// aggregate, must answer the SAME whether or not a predicate true for EVERY
// row is added to the WHERE. That is the TLP-Aggregate self-consistency the
// standing SQLancer soak reduced to a byte-for-byte case: `BOOL_OR(t2.c2)`
// over `t5, t1 FULL OUTER JOIN t2 ON t1.c7` answered NULL, and the same query
// under an always-true `WHERE t5.c2 IS NULL` answered FALSE.
//
// The root cause was neither the join nor the aggregate over null-padded rows:
// the single-process planner stripped the table qualifier off the aggregate's
// input column ("t2.c2" -> "c2"), and a bare name binds to the FIRST column of
// that name in the join output. t5, t1 and t2 all carry a bare "c2", so the
// aggregate read t5.c2 (all NULL). The always-true WHERE only changed the
// answer because it reordered the cross join, flipping which wrong "c2" the
// aggregate read (t1.c2, FALSE). The stage DAG's AggSpec already carried the
// qualifier, so this gate holds BOTH arms to the invariant AND to each other.
//
// The tables deliberately SHARE bare column names (every one has a c0/c1/c2),
// which is what turns a stripped qualifier into a wrong-column read; a corpus
// of uniquely-named columns cannot see this defect.

func fonTables() []tmdTable {
	t1s := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c2", Type: parquet.TypeBool, Nullable: true},
		{Name: "c7", Type: parquet.TypeBool, Nullable: true},
	}}
	t2s := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c2", Type: parquet.TypeBool, Nullable: true},
		{Name: "c3", Type: parquet.TypeInt64, Nullable: true},
	}}
	t5s := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeString, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c2", Type: parquet.TypeBool, Nullable: true},
	}}
	return []tmdTable{
		// t1: one row, c7=TRUE (the ON predicate), c2=FALSE, c0 left NULL.
		{"t1", t1s, []map[string]any{
			{"c1": int64(1337655936), "c2": false, "c7": true},
		}},
		// t2: the aggregated side. Two rows, c2 = TRUE then FALSE; c1 present
		// in one row only; c0 present in one row only.
		{"t2", t2s, []map[string]any{
			{"c0": int64(-1070270311), "c1": int64(47652028), "c2": true},
			{"c2": false},
		}},
		// t5: the comma-joined table. c2 is NEVER set — every row is NULL, so
		// the always-true filter `t5.c2 IS NULL` covers 100% of rows, exactly
		// the soak's fixture.
		{"t5", t5s, []map[string]any{
			{"c0": ""}, {"c0": ""}, {"c0": "Cnd"},
		}},
		// tE: an empty table, to cover the empty-input arm of the class.
		{"tE", t2s, nil},
	}
}

func TestFullOuterNonEquiAggTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := fonStandalone(t, ctx)
	coord := fonCluster(t, ctx)

	// The verbatim soak repro, kept as its own case so a regression names it.
	verbatim := "SELECT BOOL_OR(t2.c2) AS b FROM t5, t1 FULL OUTER JOIN t2 ON t1.c7"

	// The class: OUTER JOIN x non-equi ON x aggregate, in the comma-join
	// shape that lets the always-true WHERE reorder the cross join.
	joins := []string{"FULL OUTER", "LEFT OUTER", "RIGHT OUTER"}
	ons := []string{
		"t1.c7",                     // bare boolean, the soak shape
		"t1.c1 < t2.c1",             // non-equi <
		"t1.c1 > t2.c1",             // non-equi >
		"t1.c1 <> t2.c1",            // non-equi <>
		"t1.c1 BETWEEN 0 AND t2.c1", // BETWEEN
		"t1.c7 OR t2.c2",            // OR
	}
	aggs := []string{
		"COUNT(*)", "COUNT(t2.c2)", "COUNT(t2.c1)",
		"SUM(t2.c1)", "MIN(t2.c1)", "MAX(t2.c1)", "AVG(t2.c1)",
		"BOOL_OR(t2.c2)", "BOOL_AND(t2.c2)", "MIN(t2.c2)", "MAX(t2.c2)",
	}

	// Two always-true filters, each covering 100% of rows. The first is the
	// soak's own shape (t5.c2 was never inserted, so it is NULL for every
	// row); the second is the OR tautology, which additionally exercises the
	// OrFilter all-rows union fix (an `IS NOT NULL` OR-branch over a no-null
	// column used to contribute zero rows).
	filters := []string{
		"t5.c2 IS NULL",
		"(t5.c0 IS NULL OR t5.c0 IS NOT NULL)",
	}
	var cases []struct{ name, base, filt string }
	add := func(name, base string) {
		for _, p := range filters {
			cases = append(cases, struct{ name, base, filt string }{
				name + "|" + p, base, base + " WHERE " + p,
			})
		}
	}
	add("verbatim", verbatim)
	for _, jt := range joins {
		for _, on := range ons {
			for _, agg := range aggs {
				base := fmt.Sprintf("SELECT %s AS a FROM t5, t1 %s JOIN t2 ON %s", agg, jt, on)
				name := fmt.Sprintf("%s|%s|%s", jt, on, agg)
				add(name, base)
			}
		}
	}
	// An all-unmatched non-equi ON (nothing in t2 satisfies it) over an OUTER
	// join, so every probe/build row is null-padded — the arm where the
	// aggregate must see NULLs, not dropped rows.
	add("full_all_unmatched", "SELECT COUNT(t2.c2) AS a, BOOL_OR(t2.c2) AS b FROM t5, t1 FULL OUTER JOIN t2 ON t1.c1 < -1")
	// Aggregate a column from the EMPTY table's outer join.
	add("empty_side", "SELECT COUNT(tE.c2) AS a, BOOL_OR(tE.c2) AS b FROM t5, t1 FULL OUTER JOIN tE ON t1.c7")

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sBase, sErr := tmdRunSingle(ctx, single, c.base)
			sFilt, sfErr := tmdRunSingle(ctx, single, c.filt)
			dBase, dErr := tmdRunDAG(ctx, coord, c.base)
			dFilt, dfErr := tmdRunDAG(ctx, coord, c.filt)

			// TLP self-consistency: on EACH path, an always-true filter must
			// not change a scalar aggregate (nor turn an answer into an error).
			assertScalarConsistent(t, "single", c.base, c.filt, sBase, sErr, sFilt, sfErr)
			assertScalarConsistent(t, "dag", c.base, c.filt, dBase, dErr, dFilt, dfErr)

			// Two-path agreement, when both engines accept the shape.
			if sErr == nil && dErr == nil {
				if got, want := scalarKey(dBase), scalarKey(sBase); got != want {
					t.Fatalf("two-path divergence on %q:\n  single: %s\n  dag:    %s", c.base, want, got)
				}
			}
		})
	}
}

// assertScalarConsistent holds one path to the TLP invariant: the base query
// and the always-true-filtered query must agree, and a filter true for every
// row must not turn a result into an error or vice versa.
func assertScalarConsistent(t *testing.T, arm, base, filt string, b *oracle.Result, bErr error, f *oracle.Result, fErr error) {
	t.Helper()
	if (bErr == nil) != (fErr == nil) {
		t.Fatalf("[%s] an always-true filter changed acceptance of %q:\n  base err=%v\n  filt err=%v", arm, base, bErr, fErr)
	}
	if bErr != nil {
		return // both refused the shape — legal, just not this gate's target
	}
	if got, want := scalarKey(f), scalarKey(b); got != want {
		t.Fatalf("[%s] TLP violation: an always-true filter changed the aggregate on %q:\n  base:     %s\n  filtered: %s", arm, base, want, got)
	}
}

// scalarKey renders a single-row aggregate result as a stable, column-sorted
// key for comparison. Aggregates with no GROUP BY produce exactly one row.
func scalarKey(r *oracle.Result) string {
	if r == nil {
		return "<nil>"
	}
	if len(r.Rows) != 1 {
		return fmt.Sprintf("<%d rows>", len(r.Rows))
	}
	keys := make([]string, 0, len(r.Rows[0]))
	for k := range r.Rows[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb bytes.Buffer
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%s=%v", k, r.Rows[0][k])
	}
	return sb.String()
}

func fonStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range fonTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1,
		})
		if len(tbl.rows) > 0 {
			if err := ing.Ingest(ctx, tbl.rows); err != nil {
				t.Fatalf("ingest %s: %v", tbl.name, err)
			}
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

func fonCluster(t *testing.T, ctx context.Context) *Coordinator {
	t.Helper()
	infra := tmdInfra(t, ctx)

	for _, tbl := range fonTables() {
		if err := infra.cat.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		if len(tbl.rows) == 0 {
			continue // empty table: cataloged, no files
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, tbl.schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer %s: %v", tbl.name, err)
		}
		if err := pw.WriteRows(tbl.rows); err != nil {
			t.Fatalf("write %s: %v", tbl.name, err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close %s: %v", tbl.name, err)
		}
		path := fmt.Sprintf("tables/%s/chunk_0000.parquet", tbl.name)
		payload := buf.Bytes()
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		entries := []catalog.FileEntry{{
			Path: path, SizeBytes: int64(len(payload)),
			NumRows: int64(len(tbl.rows)), CreatedAt: time.Now(),
		}}
		if err := infra.cat.AddFiles(ctx, tbl.name, map[string]string{},
			"tables/"+tbl.name+"/", entries); err != nil {
			t.Fatalf("add files %s: %v", tbl.name, err)
		}
	}

	coord := New(Config{
		NATSUrl: infra.clientURL, ResultBucket: "test",
		LocalFastPathBytes: 0, // every query on the stage DAG
	}, infra.cat, infra.nc, infra.js, infra.logger)

	const workers = 3
	ids := make([]string, workers)
	for i := range ids {
		ids[i] = fmt.Sprintf("fon-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID: ids[i], NATSUrl: infra.clientURL,
			MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
		}, infra.store, infra.nc, infra.js, infra.logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatalf("worker start: %v", err)
		}
		t.Cleanup(w.Stop)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, id := range ids {
			hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 4, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatalf("marshal heartbeat: %v", err)
			}
			if err := infra.nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
				t.Fatalf("publish heartbeat: %v", err)
			}
		}
		if err := infra.nc.Flush(); err != nil {
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
