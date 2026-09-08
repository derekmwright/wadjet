package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A TIE IS BROKEN BY A VALUE, AND THE SPLIT DECIDES NOTHING — #965.
//
// `open` is the price at the earliest instant and `close` the price at the
// latest, so a bar over rows that SHARE an instant has to break the tie some
// way. Breaking it by arrival order would be a wrong answer that no
// single-process test can see: in process every tied row lands in one state in
// file order, and the answer looks stable. Across the DAG it is not — each
// task holds a different subset, the partial states merge in whatever order
// they arrive, and the same query returns a different `open` run to run.
//
// So the rule is a VALUE: among rows at the earliest instant `open` is the
// SMALLEST price, and among rows at the latest instant `close` is the LARGEST
// (ADR-0035). That makes the merge associative and commutative, which is what
// lets a partial state cross a stage at all.
//
// This fixture is built to make the split real, which is the part a fixture
// gets wrong by default: the four rows sharing 12:00:05 and the four sharing
// 12:00:25 are written ONE PER FILE, so no task can hold two rows of the same
// tie and the merge is the only thing that can decide it. The author's
// original fixture put both tied rows in one chunk and never exercised this
// (round-2 review, P4 — the reviewer built this probe and it is kept as the
// gate).
//
// Values, not just agreement between arms: three arms that all break a tie by
// arrival order agree with each other and are all wrong.

const otsTable = "otsbar"

func otsSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "px_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "px_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "px_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "px_d3810", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "px_d184", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "vol_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "vol_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "vol_d92", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
}

func otsMillis(t *testing.T, s string) int64 {
	t.Helper()
	v, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		t.Fatal(err)
	}
	return v.UTC().UnixMilli()
}

func otsRow(id int32, ts int64, px float64, vol int64) map[string]any {
	return map[string]any{
		"id": id, "ts": ts,
		"px_i32": int32(px), "px_i64": int64(px), "px_f64": px,
		"px_d3810": fmt.Sprintf("%.10f", px), "px_d184": fmt.Sprintf("%.4f", px),
		"vol_i32": int32(vol), "vol_i64": vol, "vol_d92": fmt.Sprintf("%.2f", float64(vol)),
	}
}

// otsChunks is the fixture ALREADY SPLIT into the files it has to live in.
// Prices at the first instant are 10,40,20,30 and at the last 33,11,44,22, so
// `open` is 10 and `close` is 44 only if the tiebreak is the value; an
// arrival-order tiebreak returns whichever file was read first.
func otsChunks(t *testing.T) [][]map[string]any {
	t.Helper()
	t0 := otsMillis(t, "2020-09-13 12:00:05")
	t1 := otsMillis(t, "2020-09-13 12:00:25")
	p0 := []float64{10, 40, 20, 30}
	p1 := []float64{33, 11, 44, 22}
	out := make([][]map[string]any, 4)
	for c := 0; c < 4; c++ {
		out[c] = []map[string]any{
			otsRow(int32(10+c), t0, p0[c], int64(c+1)),
			otsRow(int32(20+c), t1, p1[c], int64(c+1)),
		}
	}
	return out
}

func otsWrite(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog,
	chunks [][]map[string]any) {
	t.Helper()
	sch := otsSchema()
	if err := cat.CreateTable(ctx, otsTable, sch, nil); err != nil {
		t.Fatalf("create %s: %v", otsTable, err)
	}
	var entries []catalog.FileEntry
	for c, rows := range chunks {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, sch, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", otsTable, c)
		b := buf.Bytes()
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(b), int64(len(b)),
			"application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(b)), NumRows: int64(len(rows)), CreatedAt: time.Now(),
		})
	}
	if err := cat.AddFiles(ctx, otsTable, map[string]string{}, "tables/"+otsTable+"/", entries); err != nil {
		t.Fatal(err)
	}
}

func otsStandalone(t *testing.T, ctx context.Context, chunks [][]map[string]any) *wadjet.DB {
	t.Helper()
	sch := otsSchema()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, otsTable, sch, nil); err != nil {
		t.Fatal(err)
	}
	var all []map[string]any
	for _, c := range chunks {
		all = append(all, c...)
	}
	ing := db.NewIngester(otsTable, sch, nil, ingest.Config{MaxBufferRows: len(all) + 1})
	if err := ing.Ingest(ctx, all); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestABarBreaksATieByValueWhenTheTieIsSplitAcrossTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	chunks := otsChunks(t)
	single := otsStandalone(t, ctx, chunks)
	infra := tmdInfra(t, ctx)
	otsWrite(t, ctx, infra.store, infra.cat, chunks)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	otsWrite(t, ctx, infraB.store, infraB.cat, chunks)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	// Σ(price×volume) = 545 and Σvolume = 20 over every spelling of the same
	// numbers, so vwap is 27.25 exactly — at each price type's own scale.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"tie_split_across_files_i64",
			`SELECT (b).open o,(b).high h,(b).low l,(b).close c,(b).volume v,(b).vwap w
			 FROM (SELECT ohlcv(ts,px_i64,vol_i64) b FROM ` + otsTable + `) t`,
			[]string{"o=int64:10|h=int64:44|l=int64:10|c=int64:44|v=20|w=27.2500"}},
		{"tie_split_across_files_f64",
			`SELECT (b).open o,(b).close c FROM (SELECT ohlcv(ts,px_f64,vol_i64) b FROM ` + otsTable + `) t`,
			[]string{"o=float:10|c=float:44"}},
		// Per BUCKET, which is the shape the function exists for: the first
		// bucket holds only the 12:00:05 tie, so its open and close are the
		// SMALLEST and LARGEST prices at that one instant — 10 and 40.
		{"tie_split_bucketed",
			`SELECT k,(b).open o,(b).close c FROM (SELECT time_bucket(INTERVAL '10' SECOND, ts) k,
			   ohlcv(ts,px_i64,vol_i64) b FROM ` + otsTable + ` GROUP BY 1) t ORDER BY k`,
			[]string{
				"k=int64:1599998400000|o=int64:10|c=int64:40",
				"k=int64:1599998420000|o=int64:11|c=int64:44",
			}},
		{"d3810_price_i64_vol",
			`SELECT (b).open o,(b).volume v,(b).vwap w
			 FROM (SELECT ohlcv(ts,px_d3810,vol_i64) b FROM ` + otsTable + `) t`,
			[]string{"o=10.0000000000|v=20|w=27.25000000000000"}},
		{"d184_price_d92_vol",
			`SELECT (b).open o,(b).volume v,(b).vwap w
			 FROM (SELECT ohlcv(ts,px_d184,vol_d92) b FROM ` + otsTable + `) t`,
			[]string{"o=10.0000|v=20.00|w=27.25000000"}},
		{"i32_price_i32_vol",
			`SELECT (b).open o,(b).high h,(b).volume v,(b).vwap w
			 FROM (SELECT ohlcv(ts,px_i32,vol_i32) b FROM ` + otsTable + `) t`,
			[]string{"o=int32:10|h=int32:44|v=int64:20|w=27.2500"}},
		{"star_over_bar",
			`SELECT * FROM (SELECT ohlcv(ts,px_d184,vol_d92) b FROM ` + otsTable + `) t`,
			[]string{"b=map[string]interface {}:map[close:44.0000 high:44.0000 " +
				"low:10.0000 open:10.0000 volume:20.00 vwap:27.25000000]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				run  func() ([]string, error)
			}{
				{"single", func() ([]string, error) { return na2Run(tmdRunSingle(ctx, single, tc.sql)) }},
				{"dag", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, tc.sql)) }},
				{"dag+shuffled", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, tc.sql)) }},
			} {
				got, err := arm.run()
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
