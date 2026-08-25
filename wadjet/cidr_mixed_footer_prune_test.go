package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The mixed-footer boundary for #523, gated the way #423 gates #396's.
//
// #523 restored row-group pruning for CIDR by fixing it at the WRITER: a
// file's footer carries parquet.CidrStatsOrderKey only when every CIDR value
// in it parsed as an address, and only then may a reader re-key that file's
// text bounds into PostgreSQL's inet order. A file written before #523
// carries no such key and its bounds stay in the address TEXT's order, which
// is a DIFFERENT order — pruning on them deletes rows the filter keeps.
//
// Every suite in this repo writes its fixtures with the CURRENT writer, so
// that file can never occur in one however the corpus grows. This gate is
// that file, constructed: one table, two files, one of each vintage, read by
// one query. The table is the shape a real deployment has for as long as it
// takes to rewrite its data — which for a table nobody compacts is forever.
//
// Two things have to hold at once, and only one of them is about answers:
//
//   - The ANSWER is the same with pruning on and off, and equals what the
//     fixture holds. A prune that read the old file's text-order bounds as
//     if they were inet-ordered would drop rows here.
//   - The prune is ENGAGED on the new file. That is not implied by the
//     first: kernel.StatsDomainValue and parquet.RowGroupStats are built to
//     WITHHOLD whenever they cannot confirm the order, and a withheld prune
//     agrees with prune-off by construction. Reverting the footer-cache
//     declared-schema fix (footer_cache.go's decodeFooter, which #523 also
//     found) degrades exactly that way — silently, with every answer still
//     right. So the engagement half asserts a specific row group is SKIPPED,
//     through the cached reader the planner actually builds.
//
// What the fixture can and cannot be, stated so the gate is not over-read:
// StripCidrStatsOrder removes the PROMISE, not the ordering — the bytes are
// still written by the current writer, so the stripped file's bounds happen
// to be the inet-order extremes even though nothing says so. That is the
// same limitation StripDeclaredSchema's gate has, and it is the right one:
// what a reader can KNOW about such a file is exactly "this file makes no
// promise", and the assertion is that it declines rather than that it
// answers. The engagement half asserts the decline positively — the legacy
// file's CIDR stats come back WITHHELD, not merely refused later at
// comparison time — so a reader that started trusting an unpromised footer
// fails here even though no answer changes.
const cidrMixedTable = "cidr_mixed"

var cidrMixedSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "c_cidr", Type: parquet.TypeCIDR},
}}

// cidrMixedRowGroups is one file's worth of rows: three row groups whose
// addresses climb in inet order, so 192.168.0.0/16 splits them cleanly —
// the first two groups are wholly below it and the third wholly at or above.
func cidrMixedRowGroups(base int) [][]map[string]any {
	groups := make([][]map[string]any, 0, 3)
	for gi, prefix := range []string{"10.0", "172.16", "192.168"} {
		rows := make([]map[string]any, 20)
		for i := range rows {
			rows[i] = map[string]any{
				"id":     int64(base + gi*20 + i),
				"c_cidr": fmt.Sprintf("%s.%d.%d/32", prefix, gi, i),
			}
		}
		groups = append(groups, rows)
	}
	return groups
}

// cidrMixedFile writes one file of three row groups. legacy=true strips
// CidrStatsOrderKey afterwards, producing byte-for-byte what a pre-#523
// writer produced for the same rows.
func cidrMixedFile(t *testing.T, base int, legacy bool) []byte {
	t.Helper()
	cfg := parquet.DefaultWriterConfig()
	cfg.RowGroupSize = 20
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, cidrMixedSchema, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, rows := range cidrMixedRowGroups(base) {
		if err := w.WriteRows(rows); err != nil {
			t.Fatalf("WriteRows: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data := buf.Bytes()
	if !legacy {
		return data
	}
	out, err := parquet.StripCidrStatsOrder(data)
	if err != nil {
		t.Fatalf("building a pre-#523 fixture: %v", err)
	}
	return out
}

// cidrMixedOpen builds a DB whose one table holds both vintages.
func cidrMixedOpen(t *testing.T) (*DB, map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, cidrMixedTable, cidrMixedSchema, nil); err != nil {
		t.Fatalf("create %s: %v", cidrMixedTable, err)
	}

	files := map[string][]byte{}
	for i, legacy := range []bool{false, true} {
		data := cidrMixedFile(t, i*1000, legacy)
		name := "fresh"
		if legacy {
			name = "legacy"
		}
		path := fmt.Sprintf("tables/%s/chunk_%s.parquet", cidrMixedTable, name)
		if _, err := db.Store().Put(ctx, "test", path, bytes.NewReader(data),
			int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		if err := db.Catalog().AddFiles(ctx, cidrMixedTable, map[string]string{},
			fmt.Sprintf("tables/%s/", cidrMixedTable), []catalog.FileEntry{{
				Path: path, SizeBytes: int64(len(data)), NumRows: 60, CreatedAt: time.Now(),
			}}); err != nil {
			t.Fatalf("add %s: %v", path, err)
		}
		files[name] = data
	}
	return db, files
}

// TestCidrPruningIsUnchangedOnAMixedFooterTable is the ANSWER half: every
// CIDR predicate answers identically with pruning on and off, and matches
// what the fixture holds, over a table half of whose files predate #523.
func TestCidrPruningIsUnchangedOnAMixedFooterTable(t *testing.T) {
	ctx := context.Background()
	db, _ := cidrMixedOpen(t)

	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})

	count := func(t *testing.T, sql string, prune bool) int64 {
		t.Helper()
		scan.StatsPrune.Set(prune)
		scan.DictPrune.Set(prune)
		res, err := tmRun(ctx, db, sql)
		if err != nil {
			t.Fatalf("%s (prune=%v): %v", sql, prune, err)
		}
		if len(res.Rows) != 1 || len(res.Columns) != 1 {
			t.Fatalf("%s: want one row of one column, got %d rows and %d columns",
				sql, len(res.Rows), len(res.Columns))
		}
		n, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
		if !ok {
			t.Fatalf("%s: COUNT(*) came back as %#v", sql, res.Rows[0][res.Columns[0]])
		}
		return n
	}

	// The fixture holds, per file: 20 rows of 10.0.*, 20 of 172.16.*, 20 of
	// 192.168.*. Two files, so every count below is twice one file's.
	for _, tc := range []struct {
		name string
		pred string
		want int64
	}{
		{"AtOrAboveTheTopGroup", "c_cidr >= '192.168.0.0/16'", 40},
		{"BelowTheTopGroup", "c_cidr < '192.168.0.0/16'", 80},
		// '9.0.0.0/8' is BELOW every row as an address and ABOVE the 10.x
		// and 172.16.x rows as TEXT — the disagreement #523 is about, so a
		// prune reading a bound in the wrong order answers this one wrong.
		{"BelowEverythingAsAnAddress", "c_cidr < '9.0.0.0/8'", 0},
		{"AtOrAboveEverythingAsAnAddress", "c_cidr >= '9.0.0.0/8'", 120},
		// A bare address IS a /32 host route (PostgreSQL: '10.0.0.1'::inet =
		// '10.0.0.1/32'::inet), so both spellings find the same two rows —
		// one per file — and the equality prune must skip the two row
		// groups that cannot hold them on the fresh file while declining on
		// the legacy one.
		{"EqualsABareAddress", "c_cidr = '172.16.1.7'", 2},
		{"EqualsTheSameAddressAsAHostRoute", "c_cidr = '172.16.1.7/32'", 2},
		{"EqualsAnAddressNoRowHolds", "c_cidr = '172.16.1.200/32'", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s", cidrMixedTable, tc.pred)
			on := count(t, sql, true)
			off := count(t, sql, false)
			if on != off {
				t.Errorf("PRUNING CHANGED THE ANSWER on a mixed-footer table\n  SQL: %s\n  prune on  = %d\n  prune off = %d",
					sql, on, off)
			}
			if on != tc.want {
				t.Errorf("WRONG AGAINST THE FIXTURE\n  SQL: %s\n  engine = %d\n  fixture says %d", sql, on, tc.want)
			}
		})
	}
}

// TestCidrPredicateActuallySkipsARowGroup is the ENGAGEMENT half, and it is
// the assertion the differential above cannot make: a withheld prune agrees
// with prune-off by construction, so the answer test goes green precisely
// when the thing it guards stops running.
//
// It reads both fixtures through parquet.OpenFileReaderFromBytesCached — the
// CACHED constructor, because that is the one the planner's buildRGUnits and
// the scan's fileSlot use, and it is the one whose decodeFooter dropped the
// declared schema until #523 found it. A reader that cannot see the column
// as CIDR withholds the bound, and every prune on it silently declines.
func TestCidrPredicateActuallySkipsARowGroup(t *testing.T) {
	_, files := cidrMixedOpen(t)

	lit, ok := kernel.StatsDomainValue(parquet.TypeCIDR, 0, "192.168.0.0/16")
	if !ok {
		t.Fatal("kernel.StatsDomainValue withheld a valid CIDR literal — pruning is off for the type")
	}
	pred := scan.StatsPredicate{Column: "c_cidr", Op: exec.OpGe, Value: lit}

	fresh, err := parquet.OpenFileReaderFromBytesCached(files["fresh"], "wadjet-test/cidr-mixed/fresh#1@1")
	if err != nil {
		t.Fatalf("open the fresh file through the footer cache: %v", err)
	}
	if n := fresh.NumRowGroups(); n != 3 {
		t.Fatalf("fresh file has %d row groups, want 3 — the fixture is not the shape this test needs", n)
	}
	// Row groups 0 (10.0.*) and 1 (172.16.*) are wholly BELOW the literal in
	// inet order, so `>= literal` can skip both. Row group 2 (192.168.*) is
	// at or above it and must NOT be skipped.
	for rg, wantPruned := range map[int]bool{0: true, 1: true, 2: false} {
		got := scan.CanPruneRowGroup(pred, fresh.RowGroupStats(rg))
		if got != wantPruned {
			if wantPruned {
				t.Errorf("row group %d is NOT skipped for `c_cidr >= '192.168.0.0/16'` — CIDR pruning "+
					"is disengaged. Nothing about this shows up in an answer: a withheld prune is "+
					"always correct and always slower. Stats: %+v",
					rg, fresh.RowGroupStats(rg).Columns["c_cidr"])
			} else {
				t.Errorf("row group %d IS skipped for `c_cidr >= '192.168.0.0/16'` but holds matching "+
					"rows — this is a wrong answer, not a lost optimization", rg)
			}
		}
	}

	// The legacy file is the other side of the same statement: it must
	// DECLINE, never answer. A pre-#523 footer's bounds are in the address
	// TEXT's order, and 192.168.* sorts below 9.* as text — so a reader that
	// compared them anyway would prune the row group holding every matching
	// row.
	legacy, err := parquet.OpenFileReaderFromBytesCached(files["legacy"], "wadjet-test/cidr-mixed/legacy#1@1")
	if err != nil {
		t.Fatalf("open the legacy file through the footer cache: %v", err)
	}
	for rg := 0; rg < legacy.NumRowGroups(); rg++ {
		if scan.CanPruneRowGroup(pred, legacy.RowGroupStats(rg)) {
			t.Errorf("row group %d of the PRE-#523 file was pruned — its bounds are in the address "+
				"text's order, which is not the order this predicate is in", rg)
		}
	}
	// ...and it is genuinely the old shape, not merely a file the reader
	// happened to decline: without this the whole gate could pass because
	// the fixture was never the migration case.
	if bytes.Equal(files["fresh"], files["legacy"]) {
		t.Fatal("the two fixtures are byte-identical — StripCidrStatsOrder did not run")
	}
	for rg := 0; rg < legacy.NumRowGroups(); rg++ {
		cs := legacy.RowGroupStats(rg).Columns["c_cidr"]
		if cs.HasStats {
			t.Errorf("row group %d of the PRE-#523 file still reports c_cidr statistics (%+v); "+
				"they must be withheld, not merely refused at comparison time", rg, cs)
		}
	}
}
