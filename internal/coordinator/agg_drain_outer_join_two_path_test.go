package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// TPC-H Q20's shape, on the four arms, with the aggregate's drain forced (#920).
//
// The shape is a two-INT64-key `GROUP BY … SUM(float64)` consumed by an OUTER
// join whose result feeds a predicate on the SUM. It is the shape #920 was
// filed against, and #920's own "gates that did not see it" section is right:
// neither TestSpillArcShapesAgreeOnBothDistributionArms nor
// TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget has it. Two properties of
// it are load-bearing and appear in no other cell in this package:
//
//   - some probe keys have NO group at all, so the join owes them a NULL and
//     the predicate above owes them UNKNOWN. That is the half a wrong answer
//     comes out of: a stale or reused build value under an unmatched row reads
//     as a real number, and `avail > 0.5 * <that number>` is then TRUE for rows
//     whose supplier does not belong in the answer. #920's three "lost"
//     suppliers are exactly two such rows (forest parts with no 1994 lineitem
//     row, availqty 8093 and 5952 — they pass against almost any non-NULL) plus
//     one that fails its predicate honestly.
//   - the predicate sits ABOVE the join and reads the aggregate's column, so a
//     divergence shows up as a changed ROW SET rather than a changed number.
//     A gate that compares sums would not see it.
//
// The drain is forced for the DAG arms only and the single-process reference is
// taken DISARMED, per ADR-0027: a knob armed on both sides lets a shared defect
// cancel out (#790). The absolute cell (unmatched_is_null) does not rely on the
// cross-arm comparison at all — it asserts the VALUE PostgreSQL owes, so it
// stands even if every arm agreed on a wrong one.
func TestAnOuterJoinOverADrainedGroupSumAnswersTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	li, ps := q20ShapeFixture()
	single := q20ShapeStandalone(t, ctx, li, ps)

	infra := tmdInfra(t, ctx)
	q20ShapeWrite(t, ctx, infra, li, ps)
	coord := tmdCoordinator(t, ctx, infra)

	infraC := tmdInfra(t, ctx)
	q20ShapeWrite(t, ctx, infraC, li, ps)
	coordC := tmdCoordinatorWithWorkers(t, ctx, infraC,
		func(w *worker.Config) { w.MemoryBudget = 512 * 1024; w.CacheBytes = 1 << 20 })

	infraB := tmdInfra(t, ctx)
	q20ShapeWrite(t, ctx, infraB, li, ps)
	coordB := tmdCoordinatorWithWorkers(t, ctx, infraB,
		func(w *worker.Config) { w.MemoryBudget = 512 * 1024; w.CacheBytes = 1 << 20 },
		func(c *Config) { c.BroadcastBytesOverride = 1 })

	const aggSQL = `SELECT pk, sk, SUM(qty) AS s FROM q20li GROUP BY pk, sk`
	const joinedSQL = `SELECT p.ppk AS ppk, p.psk AS psk, a.s AS s FROM q20ps p ` +
		`LEFT JOIN (SELECT pk, sk, SUM(qty) AS s FROM q20li GROUP BY pk, sk) a ` +
		`ON p.ppk = a.pk AND p.psk = a.sk`
	const q20SQL = `SELECT DISTINCT p.psk AS supp FROM q20ps p ` +
		`LEFT JOIN (SELECT pk, sk, SUM(qty) AS s FROM q20li GROUP BY pk, sk) a ` +
		`ON p.ppk = a.pk AND p.psk = a.sk WHERE p.avail > 0.5 * a.s`
	// The absolute cell: every probe row whose key has no group must carry a
	// NULL, and the predicate above it must therefore admit none of them. The
	// fixture gives those rows avail = 1e9, which passes against any real sum
	// the fixture can produce, so a non-NULL there is a changed row set here.
	const unmatchedSQL = `SELECT COUNT(*) AS n FROM q20ps p ` +
		`LEFT JOIN (SELECT pk, sk, SUM(qty) AS s FROM q20li GROUP BY pk, sk) a ` +
		`ON p.ppk = a.pk AND p.psk = a.sk WHERE p.avail > 500000000 AND a.s IS NOT NULL`

	for _, tc := range []struct {
		name, sql string
		// want, when non-empty, is the answer PostgreSQL owes regardless of
		// what any arm says — the absolute half of this gate.
		want []string
		// wantNullCol, when set, names a column whose NULL count every arm
		// must match wantNulls. It is the half that does NOT depend on the
		// reference: a defect present on BOTH sides cancels out of a
		// cross-arm comparison (#790's lesson applied to a shared operator
		// rather than to a shared knob), and the outer join's null-fill is
		// exactly such a shared operator.
		wantNullCol string
		wantNulls   int
	}{
		{name: "agg", sql: aggSQL},
		{name: "joined", sql: joinedSQL, wantNullCol: "s", wantNulls: q20UnmatchedRows},
		{name: "q20", sql: q20SQL},
		{name: "unmatched_is_null", sql: unmatchedSQL, want: []string{"n=int64:0|"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := tmdRunSingle(ctx, single, tc.sql)
			if err != nil {
				t.Fatalf("single-process arm: %v\n  SQL: %s", err, tc.sql)
			}
			if len(ref.Rows) == 0 {
				t.Fatalf("the single-process arm returned no rows — this cell would compare nothing\n  SQL: %s", tc.sql)
			}
			w := spillArcRows(ref.Columns, ref.Rows)
			if tc.want != nil && !equalRows(w, tc.want) {
				t.Fatalf("single-process arm: %v, want %v\n  SQL: %s", w, tc.want, tc.sql)
			}
			if tc.wantNullCol != "" {
				if n := countNullCol(ref.Rows, tc.wantNullCol); n != tc.wantNulls {
					t.Fatalf("single-process arm: %d NULL %s, want %d — an unmatched probe row "+
						"owes NULL, and a value there is admitted by every predicate above it\n  SQL: %s",
						n, tc.wantNullCol, tc.wantNulls, tc.sql)
				}
			}

			forcedBefore := exec.ForcedAggDrains.Load()
			restore := exec.ForceAggDrainEvery(1)
			defer exec.ForceAggDrainEvery(restore)
			for _, arm := range []struct {
				name  string
				coord *Coordinator
			}{{"dag", coord}, {"dag+budgeted-workers", coordC}, {"dag+shuffled", coordB}} {
				got, err := tmdRunDAG(ctx, arm.coord, tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				g := spillArcRows(got.Columns, got.Rows)
				if tc.want != nil && !equalRows(g, tc.want) {
					t.Errorf("%s arm: %v, want %v\n  SQL: %s", arm.name, g, tc.want, tc.sql)
					continue
				}
				if tc.wantNullCol != "" {
					if n := countNullCol(got.Rows, tc.wantNullCol); n != tc.wantNulls {
						t.Errorf("%s arm: %d NULL %s, want %d\n  SQL: %s",
							arm.name, n, tc.wantNullCol, tc.wantNulls, tc.sql)
						continue
					}
				}
				if len(g) != len(w) {
					t.Errorf("%s arm: %d rows, want %d\n  SQL: %s", arm.name, len(g), len(w), tc.sql)
					continue
				}
				for i := range w {
					if g[i] != w[i] {
						t.Errorf("%s arm: row %d differs\n  %s: %s\n  single: %s\n  SQL: %s",
							arm.name, i, arm.name, g[i], w[i], tc.sql)
						break
					}
				}
			}
			// Engagement: a forcing knob that fired nothing turns this cell
			// into a comparison of in-memory runs.
			if exec.ForcedAggDrains.Load() == forcedBefore {
				t.Error("the forcing knob fired no drain on any DAG arm — the workers did not " +
					"inherit it, so this cell compared in-memory runs and proves nothing")
			}
		})
	}
}

// countNullCol counts rows whose named column is NULL.
func countNullCol(rows []map[string]any, col string) int {
	n := 0
	for _, r := range rows {
		if r[col] == nil {
			n++
		}
	}
	return n
}

func equalRows(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// q20UnmatchedRows is how many partsupp rows the fixture gives keys that
// appear in NO lineitem row. Their join half is NULL by definition, so the
// count is an absolute expectation no arm may disagree with.
const q20UnmatchedRows = 5000

func q20LiSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "pk", Type: parquet.TypeInt64},
		{Name: "sk", Type: parquet.TypeInt64},
		{Name: "qty", Type: parquet.TypeFloat64},
	}}
}

func q20PsSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "ppk", Type: parquet.TypeInt64},
		{Name: "psk", Type: parquet.TypeInt64},
		{Name: "avail", Type: parquet.TypeFloat64},
	}}
}

// q20ShapeFixture builds Q20's two sides deterministically.
//
// Quantities are INTEGER-VALUED, as l_quantity is: a float64 sum of them is
// exact in any order, so a divergence here is never ADR-0013's class 9 and
// never has to be argued about.
//
// The partsupp side carries two kinds of row:
//   - one per (pk, sk) pair that HAS lineitem rows, with avail set within ±1 of
//     the predicate's boundary so a one-unit error in the sum flips it;
//   - q20UnmatchedRows rows whose (pk, sk) appears in NO lineitem row, whose
//     supplier appears in no other row, and whose avail is 1e9. Those are the
//     rows the join owes a NULL; every one of them enters the answer if it gets
//     a value instead.
func q20ShapeFixture() (li, ps []map[string]any) {
	const (
		nRows  = 120000
		nParts = 20000
		nSupps = 40
	)
	rng := rand.New(rand.NewSource(920))
	sums := map[[2]int64]float64{}
	li = make([]map[string]any, 0, nRows)
	for i := 0; i < nRows; i++ {
		pk := int64(rng.Intn(nParts))
		sk := int64(rng.Intn(nSupps))
		q := float64(1 + rng.Intn(50))
		li = append(li, map[string]any{"pk": pk, "sk": sk, "qty": q})
		sums[[2]int64{pk, sk}] += q
	}
	keys := make([][2]int64, 0, len(sums))
	for k := range sums {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	ps = make([]map[string]any, 0, len(keys)+q20UnmatchedRows)
	for _, k := range keys {
		avail := 0.5*sums[k] + float64(rng.Intn(3)) - 1
		ps = append(ps, map[string]any{"ppk": k[0], "psk": k[1], "avail": avail})
	}
	for i := 0; i < q20UnmatchedRows; i++ {
		ps = append(ps, map[string]any{
			"ppk": int64(nParts + i), "psk": int64(nSupps + i%20), "avail": 1e9,
		})
	}
	return li, ps
}

func q20ShapeStandalone(t *testing.T, ctx context.Context, li, ps []map[string]any) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{{"q20li", q20LiSchema(), li}, {"q20ps", q20PsSchema(), ps}} {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 8192,
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

// q20ShapeWrite writes the fixture into one infra's store and catalog, four
// files per table so the DAG really fans the scan out.
func q20ShapeWrite(t *testing.T, ctx context.Context, infra tmdInfraT, li, ps []map[string]any) {
	t.Helper()
	q20WriteTable(t, ctx, infra, "q20li", q20LiSchema(), li)
	q20WriteTable(t, ctx, infra, "q20ps", q20PsSchema(), ps)
}

func q20WriteTable(t *testing.T, ctx context.Context, infra tmdInfraT, name string,
	schema parquet.Schema, rows []map[string]any) {
	t.Helper()
	const chunks = 4
	store, cat := infra.store, infra.cat
	if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	n := len(rows)
	per := (n + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, min(c*per+per, n)
		if lo >= hi {
			break
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer %s: %v", name, err)
		}
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close %s: %v", name, err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, c)
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
	if err := cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", entries); err != nil {
		t.Fatalf("add files %s: %v", name, err)
	}
}
