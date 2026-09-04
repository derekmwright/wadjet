package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #707: two base-table files declaring ONE catalog DECIMAL column at DIFFERENT
// scales.
//
// ADR-0018 is the charter: a parquet file's own numbers are INPUT, and the
// CATALOG's (p,s) is what the column means. A file whose footer declares
// another scale still holds the right NUMBER — the chunk carries the unscaled
// integer and the declaration carries the scale — so the read reconciles it to
// the catalog's scale rather than reinterpreting the carrier under a scale that
// is not the one it was written at.
//
// Before the fix the native scan built the output vector from the CATALOG
// schema and copied the file's carriers into it verbatim, so every row of the
// disagreeing file came back multiplied (or divided) by a power of ten:
// 12.7500 declared at scale 4 read back as 1275.00 under a catalog scale of 2.
// That is a per-ROW silent wrong answer, not only an aggregate one, and it was
// the same wrong answer in both file orders — the scan never consulted the
// file's declaration at all.
//
// PostgreSQL 17.11 is the authority for every want below, measured live on the
// shared oracle over `CREATE TABLE (a numeric(15,2))` holding two rows of
// 12.75:
//
//	SUM 25.50   MIN 12.75   MAX 12.75   AVG 12.7500000000000000   COUNT 2
//	GROUP BY a -> one group, 12.75, count 2
//
// A numeric column has ONE scale in PostgreSQL, which is exactly why the
// catalog's is the answer here: there is no PostgreSQL shape in which one
// column's two rows carry two scales.
const dfsTable = "decfilescale"

// dfsCatalog is what the TABLE is declared as.
func dfsCatalog() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}}
}

// dfsFileAt is the same relation declared at another scale — what a foreign
// writer, a pre-#647 write path or an unrepaired #608 file puts on disk.
func dfsFileAt(scale int) parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 15, Scale: scale, Nullable: true},
	}}
}

// dfsRows: file A declares (15,2) and stores 12.75 as 1275; file B declares
// (15,4) and stores the SAME NUMBER as 127500. Both files therefore mean 12.75,
// and a reader that honours each file's own declaration answers 12.75 twice.
//
// The values are chosen so the two rules are distinguishable in every
// direction (protocol method 2): reinterpreting B's carrier under scale 2
// gives 1275.00 (×100), reinterpreting A's under scale 4 gives 0.1275 (÷100),
// and the right answer is neither.
type dfsFile struct {
	schema   parquet.Schema
	id       int64
	unscaled int64
}

func dfsFiles() []dfsFile {
	return []dfsFile{
		{dfsFileAt(2), 1, 1275},   // 12.75 at the catalog's scale
		{dfsFileAt(4), 2, 127500}, // 12.7500 — the same number, declared finer
	}
}

// dfsWrite puts the two files under one catalog table. bFirst swaps the order
// they are ADDED in, which is the order the scan reads them in: the pre-fix
// accumulator adopted the scale of whichever contributing file it saw first,
// so a gate that only tries one order can pass for the wrong reason.
func dfsWrite(t *testing.T, ctx context.Context, cat *catalog.Catalog, store objstore.Store,
	table string, bFirst bool,
) {
	t.Helper()
	if err := cat.CreateTable(ctx, table, dfsCatalog(), nil); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	files := dfsFiles()
	if bFirst {
		files[0], files[1] = files[1], files[0]
	}
	var entries []catalog.FileEntry
	for i, f := range files {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, f.schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		row := map[string]any{"id": f.id, "a": parquet.Decimal128{Lo: uint64(f.unscaled)}}
		if err := pw.WriteRows([]map[string]any{row}); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
		payload := buf.Bytes()
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(payload)),
			NumRows: 1, CreatedAt: time.Now(),
		})
	}
	if err := cat.AddFiles(ctx, table, map[string]string{}, "tables/"+table+"/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}

// dfsStandalone is the embedded arm over the same two files. budget > 0 gives
// the spilled arm.
func dfsStandalone(t *testing.T, ctx context.Context, bFirst bool, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	dfsWrite(t, ctx, db.Catalog(), db.Store(), dfsTable, bFirst)
	return db
}

// dfsCase is one shape and the PostgreSQL 17.11 answer for it, rendered the way
// tmdRender renders a row.
type dfsCase struct {
	name string
	sql  string
	want []string
}

func dfsCases() []dfsCase {
	q := func(s string) string { return fmt.Sprintf(s, dfsTable) }
	return []dfsCase{
		// The per-ROW shape: this is the one the pre-fix tree got wrong
		// without any aggregate in the query at all.
		{"projection", q("SELECT id, a FROM %s ORDER BY id"),
			[]string{"1|12.75", "2|12.75"}},
		{"sum", q("SELECT SUM(a) FROM %s"), []string{"25.50"}},
		{"min", q("SELECT MIN(a) FROM %s"), []string{"12.75"}},
		{"max", q("SELECT MAX(a) FROM %s"), []string{"12.75"}},
		{"count", q("SELECT COUNT(a) FROM %s"), []string{"2"}},
		// AVG is the shape where a scale defect is easiest to hide, because
		// ADR-0012 item 9 already licenses a DIGIT-COUNT divergence here —
		// AVG(DECIMAL(p,s)) is DECIMAL(38, s+4), so wadjet renders 12.750000
		// where PostgreSQL renders 12.7500000000000000. The VALUE is the same
		// 12.75 and that is what this pins, so a future change cannot move the
		// number behind the licensed difference in how many digits follow it
		// (round 0 review, P3).
		{"avg", q("SELECT AVG(a) FROM %s"), []string{"12.750000"}},
		// GROUP BY over the column: two scales made two groups where
		// PostgreSQL has one.
		{"group by", q("SELECT a, COUNT(*) FROM %s GROUP BY a ORDER BY a"),
			[]string{"12.75|2"}},
		// ORDER BY over the column: the reinterpreted row sorted above the
		// right one.
		{"order by", q("SELECT a FROM %s ORDER BY a, id"),
			[]string{"12.75", "12.75"}},
		// The column as a JOIN KEY: a reinterpreted carrier joins nothing.
		{"join key", fmt.Sprintf(
			"SELECT COUNT(*) FROM %[1]s t1 JOIN %[1]s t2 ON t1.a = t2.a", dfsTable),
			[]string{"4"}},
		// A predicate over the column, which also exercises the row-group
		// prune against footer statistics written at the FILE's scale.
		{"filter", q("SELECT COUNT(*) FROM %s WHERE a = 12.75"), []string{"2"}},
		{"filter range", q("SELECT COUNT(*) FROM %s WHERE a > 12.00 AND a < 13.00"),
			[]string{"2"}},
	}
}

func TestMixedDeclaredScaleFilesReconcileToTheCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	for _, bFirst := range []bool{false, true} {
		order := "A-then-B"
		if bFirst {
			order = "B-then-A"
		}
		t.Run(order, func(t *testing.T) {
			single := dfsStandalone(t, ctx, bFirst, 0)
			spilled := dfsStandalone(t, ctx, bFirst, 512*1024)

			infra := tmdInfra(t, ctx)
			dfsWrite(t, ctx, infra.cat, infra.store, dfsTable, bFirst)
			dag := tmdCoordinator(t, ctx, infra)
			shuffled := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
			fast := New(Config{
				NATSUrl: infra.clientURL, ResultBucket: "test",
				LocalFastPathBytes: DefaultLocalFastPathBytes,
			}, infra.cat, infra.nc, infra.js, infra.logger)

			arms := []struct {
				name string
				run  func(string) ([]string, error)
			}{
				{"single", func(sql string) ([]string, error) { return dfsRun(tmdRunSingle(ctx, single, sql)) }},
				{"spilled", func(sql string) ([]string, error) { return dfsRun(tmdRunSingle(ctx, spilled, sql)) }},
				{"fast path", func(sql string) ([]string, error) { return dfsRun(tmdRunDAG(ctx, fast, sql)) }},
				{"dag", func(sql string) ([]string, error) { return dfsRun(tmdRunDAG(ctx, dag, sql)) }},
				{"dag-shuffled", func(sql string) ([]string, error) { return dfsRun(tmdRunDAG(ctx, shuffled, sql)) }},
			}
			for _, tc := range dfsCases() {
				for _, arm := range arms {
					t.Run(tc.name+"/"+arm.name, func(t *testing.T) {
						got, err := arm.run(tc.sql)
						if err != nil {
							t.Fatalf("%s refused %q: %v\n  PostgreSQL 17.11: %v",
								arm.name, tc.sql, err, tc.want)
						}
						if len(got) != len(tc.want) {
							t.Fatalf("%s: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17.11)\n  SQL: %s",
								arm.name, len(got), len(tc.want), got, tc.want, tc.sql)
						}
						for i := range got {
							if got[i] != tc.want[i] {
								t.Errorf("%s row %d = %q, want %q (live PostgreSQL 17.11)\n  SQL: %s\n"+
									"  a file declaring column \"a\" at a scale the catalog does not "+
									"declare was read under the catalog's scale without rescaling "+
									"its carrier (#707, ADR-0018)",
									arm.name, i, got[i], tc.want[i], tc.sql)
							}
						}
					})
				}
			}
		})
	}
}

// dfsRun renders one result as "col|col" per row, values as the engine
// rendered them — a DECIMAL's trailing zeros are part of the answer here, so
// the TEXT is compared and not a parsed number.
func dfsRun(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			switch v := r[c].(type) {
			case nil:
				parts = append(parts, "NULL")
			case string:
				parts = append(parts, v)
			default:
				parts = append(parts, fmt.Sprint(v))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, nil
}

// #707 ONE LEVEL DOWN — a DECIMAL leaf inside ROW / ARRAY / MAP.
//
// Round 0's review found the reconciliation reached the flat column and not the
// container beside it, so one row of one table rendered `12.75` for its flat
// column and `1275.00` for the same number inside a ROW — and COMPACT baked the
// wrong carrier in and deleted the input. The declaration is half of every
// DECIMAL value at every depth, so the rule cannot be a top-level one.
//
// The fixture is deliberately the SAME two numbers as the flat gate above:
// both files mean 12.75, one declaring the nested leaves at scale 2 and one at
// scale 4. PostgreSQL has one scale per column, so 12.75 is the only answer,
// and the two rows must be indistinguishable except for their id.
const dfsNestTable = "decfilescalenest"

func dfsNestSchema(scale int) parquet.Schema {
	leaf := parquet.Column{Name: "v", Type: parquet.TypeDecimal,
		Precision: 15, Scale: scale, Nullable: true}
	elem := parquet.Column{Name: "element", Type: parquet.TypeDecimal,
		Precision: 15, Scale: scale, Nullable: true}
	mval := parquet.Column{Name: "value", Type: parquet.TypeDecimal,
		Precision: 15, Scale: scale, Nullable: true}
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "flat", Type: parquet.TypeDecimal, Precision: 15, Scale: scale, Nullable: true},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{leaf}},
		{Name: "ar", Type: parquet.TypeArray, Nullable: true, ElementType: &elem},
		{Name: "mp", Type: parquet.TypeMap, Nullable: true, ElementType: &parquet.Column{
			Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				mval,
			}}},
	}}
}

// dfsNestWrite puts two files under one catalog table: file 0 declares its
// nested leaves at the catalog's scale 2, file 1 at scale 4. Both mean 12.75.
func dfsNestWrite(t *testing.T, ctx context.Context, cat *catalog.Catalog, store objstore.Store) {
	t.Helper()
	if err := cat.CreateTable(ctx, dfsNestTable, dfsNestSchema(2), nil); err != nil {
		t.Fatalf("create %s: %v", dfsNestTable, err)
	}
	type spec struct {
		schema   parquet.Schema
		id       int64
		unscaled int64
	}
	var entries []catalog.FileEntry
	for i, s := range []spec{{dfsNestSchema(2), 1, 1275}, {dfsNestSchema(4), 2, 127500}} {
		box := parquet.Decimal128{Lo: uint64(s.unscaled)}
		row := map[string]any{
			"id": s.id, "flat": box,
			"rw": map[string]any{"v": box},
			"ar": []any{box},
			"mp": map[string]any{"k": box},
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, s.schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer: %v", err)
		}
		if err := pw.WriteRows([]map[string]any{row}); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", dfsNestTable, i)
		payload := buf.Bytes()
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(payload)), NumRows: 1, CreatedAt: time.Now(),
		})
	}
	if err := cat.AddFiles(ctx, dfsNestTable, map[string]string{},
		"tables/"+dfsNestTable+"/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
}

func TestNestedDecimalLeavesReconcileToTheCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { single.Close() })
	dfsNestWrite(t, ctx, single.Catalog(), single.Store())

	infra := tmdInfra(t, ctx)
	dfsNestWrite(t, ctx, infra.cat, infra.store)
	dag := tmdCoordinator(t, ctx, infra)
	shuffled := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	fast := New(Config{
		NATSUrl: infra.clientURL, ResultBucket: "test",
		LocalFastPathBytes: DefaultLocalFastPathBytes,
	}, infra.cat, infra.nc, infra.js, infra.logger)

	// Every cell of every row is the same number, so the two rows differ only
	// in their id — which is exactly what a per-depth reconciliation buys.
	const sql = "SELECT flat, rw, ar, mp FROM " + dfsNestTable + " ORDER BY id"
	// The MAP comes back in its STORAGE shape — []any of {key,value} entry
	// maps, which is what batch.Vector.GetValue's TypeMap arm boxes and what
	// every row that passed through RowAt/ToRows carries (#647 re-review). The
	// shape is not this gate's subject; the NUMBER inside it is.
	want := []string{
		"12.75|map[v:12.75]|[12.75]|[map[key:k value:12.75]]",
		"12.75|map[v:12.75]|[12.75]|[map[key:k value:12.75]]",
	}
	for _, arm := range []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(s string) ([]string, error) { return dfsRun(tmdRunSingle(ctx, single, s)) }},
		{"fast path", func(s string) ([]string, error) { return dfsRun(tmdRunDAG(ctx, fast, s)) }},
		{"dag", func(s string) ([]string, error) { return dfsRun(tmdRunDAG(ctx, dag, s)) }},
		{"dag-shuffled", func(s string) ([]string, error) { return dfsRun(tmdRunDAG(ctx, shuffled, s)) }},
	} {
		t.Run(arm.name, func(t *testing.T) {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s refused %q: %v", arm.name, sql, err)
			}
			if len(got) != len(want) {
				t.Fatalf("%s: %d rows, want %d: %v", arm.name, len(got), len(want), got)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("%s row %d = %q, want %q (live PostgreSQL 17.11 has one scale "+
						"per column, so both files mean 12.75 at every depth) — a DECIMAL "+
						"leaf inside a container was read under the catalog's scale without "+
						"moving its carrier (#707 one level down)", arm.name, i, got[i], want[i])
				}
			}
		})
	}
}
