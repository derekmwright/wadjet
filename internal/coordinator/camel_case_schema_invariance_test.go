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

// CAMELCASE SCHEMA INVARIANCE — the coverage gate for the identifier fold.
//
// An unquoted identifier folds to lower case at the lexer (#731, 45baeb6e), so
// every column REFERENCE reaches the planner and the engine folded, while a
// catalog schema keeps the spelling the parquet file gave it. CamelCase
// column names are ordinary there: ClickBench's `hits` has `WatchID`,
// `EventDate`, `ResolutionWidth`, `CounterID`.
//
// That makes the reference and the schema TWO DIFFERENT STRINGS for the same
// column — and every corpus this engine gates on spells its columns lower
// case, where they are the SAME string. TPC-H is all lower case; ClickBench
// is CamelCase but single-table with no joins and its arm skips without a
// data part. So a site that compares a reference against a schema
// byte-exactly is invisible to every value gate in the tree, which is how
// #881 kept the metadata MIN/MAX rewrite dormant for thirteen releases and
// how a silent `UPDATE` no-op, a MERGE that inserted instead of updating, a
// COPY that stored NULLs and a policy door that stopped binding all shipped.
//
// This gate closes that hole by CONSTRUCTION rather than by anticipating the
// next site: one fixture in two spellings that differ ONLY in case, the same
// battery over both, results required to match on the single-process arm and
// both DAG arms.
//
// The schema is deliberately MIXED — `counterid` and `tier` are already
// folded while their neighbours are not. A schema where EVERY column is
// CamelCase is accidentally safe at several sites, because a total lookup
// miss falls back to "keep everything" (full-width projection, unpruned
// scan); it is the PARTIAL miss that drops one column and keeps the rest,
// and only a mixed schema produces one.
//
// A divergence here names the shape and the arm; that is the localization.

func ccsName(camel bool, s string) string {
	if camel {
		return s
	}
	return strings.ToLower(s)
}

func ccsSchemas(camel bool) map[string]parquet.Schema {
	n := func(s string) string { return ccsName(camel, s) }
	return map[string]parquet.Schema{
		"hits": {Columns: []parquet.Column{
			{Name: n("WatchID"), Type: parquet.TypeInt64},
			{Name: "counterid", Type: parquet.TypeInt64}, // already folded
			{Name: n("UserAgent"), Type: parquet.TypeString},
			{Name: n("RegionID"), Type: parquet.TypeInt64},
		}},
		"regions": {Columns: []parquet.Column{
			{Name: n("RegionID"), Type: parquet.TypeInt64},
			{Name: n("RegionName"), Type: parquet.TypeString},
			{Name: "tier", Type: parquet.TypeInt64},
		}},
	}
}

func ccsRows(camel bool) map[string][]map[string]any {
	n := func(s string) string { return ccsName(camel, s) }
	hits := make([]map[string]any, 0, 12)
	for i := 1; i <= 12; i++ {
		hits = append(hits, map[string]any{
			n("WatchID"):   int64(i),
			"counterid":    int64(i % 5),
			n("UserAgent"): fmt.Sprintf("agent-%d", i%3),
			n("RegionID"):  int64(i % 4), // 3 has no region row -> anti/outer rows
		})
	}
	return map[string][]map[string]any{
		"hits": hits,
		"regions": {
			{n("RegionID"): int64(0), n("RegionName"): "north", "tier": int64(1)},
			{n("RegionID"): int64(1), n("RegionName"): "south", "tier": int64(2)},
			{n("RegionID"): int64(2), n("RegionName"): "east", "tier": int64(1)},
		},
	}
}

func ccsWrite(t *testing.T, ctx context.Context, infra tmdInfraT, camel bool) {
	t.Helper()
	schemas, rows := ccsSchemas(camel), ccsRows(camel)
	const chunks = 3
	for name, schema := range schemas {
		if err := infra.cat.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		rs := rows[name]
		per := (len(rs) + chunks - 1) / chunks
		var entries []catalog.FileEntry
		for c := 0; c*per < len(rs); c++ {
			lo, hi := c*per, min(c*per+per, len(rs))
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("writer: %v", err)
			}
			if err := pw.WriteRows(rs[lo:hi]); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, c)
			b := buf.Bytes()
			if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(b),
				int64(len(b)), "application/octet-stream"); err != nil {
				t.Fatalf("put: %v", err)
			}
			entries = append(entries, catalog.FileEntry{
				Path: path, SizeBytes: int64(len(b)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
			})
		}
		if err := infra.cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", entries); err != nil {
			t.Fatalf("add files: %v", err)
		}
	}
}

func ccsStandalone(t *testing.T, ctx context.Context, camel bool) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schemas, rows := ccsSchemas(camel), ccsRows(camel)
	for name, schema := range schemas {
		if err := db.Catalog().CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("create: %v", err)
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: 1000, RowGroupSize: 3})
		if err := ing.Ingest(ctx, rows[name]); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	return db
}

var ccsShapes = []string{
	// semi / anti joins (filterColumnIndex, PruneBuildColumns)
	`SELECT WatchID FROM hits h WHERE EXISTS (SELECT 1 FROM regions r WHERE r.RegionID = h.RegionID) ORDER BY WatchID`,
	`SELECT WatchID FROM hits h WHERE NOT EXISTS (SELECT 1 FROM regions r WHERE r.RegionID = h.RegionID) ORDER BY WatchID`,
	`SELECT WatchID FROM hits h WHERE EXISTS (SELECT 1 FROM regions r WHERE r.RegionID = h.RegionID AND r.tier = 1) ORDER BY WatchID`,
	`SELECT WatchID FROM hits h WHERE EXISTS (SELECT 1 FROM regions r WHERE r.RegionID = h.RegionID AND r.RegionName = 'south') ORDER BY WatchID`,
	`SELECT WatchID FROM hits WHERE RegionID IN (SELECT RegionID FROM regions WHERE tier = 1) ORDER BY WatchID`,
	`SELECT WatchID FROM hits WHERE RegionID NOT IN (SELECT RegionID FROM regions WHERE tier = 1) ORDER BY WatchID`,
	// outer joins with an ON RESIDUAL (join_residual.go)
	`SELECT h.WatchID, r.RegionName FROM hits h LEFT JOIN regions r ON h.RegionID = r.RegionID AND r.tier = 1 ORDER BY h.WatchID`,
	`SELECT h.WatchID, r.RegionName FROM hits h LEFT JOIN regions r ON h.RegionID = r.RegionID AND r.RegionName = 'east' ORDER BY h.WatchID`,
	`SELECT h.WatchID, r.RegionName FROM hits h LEFT JOIN regions r ON h.RegionID = r.RegionID AND h.counterid > 1 ORDER BY h.WatchID`,
	`SELECT h.WatchID, r.RegionName FROM hits h FULL OUTER JOIN regions r ON h.RegionID = r.RegionID AND r.tier = 1 ORDER BY h.WatchID, r.RegionName`,
	// DISTINCT + ORDER BY + LIMIT through the coordinator merge
	`SELECT DISTINCT WatchID FROM hits ORDER BY WatchID LIMIT 5`,
	`SELECT DISTINCT RegionID FROM hits ORDER BY RegionID DESC`,
	`SELECT DISTINCT UserAgent, RegionID FROM hits ORDER BY UserAgent, RegionID`,
	// grouped aggregate merged at the coordinator
	`SELECT RegionID, COUNT(*) AS c, SUM(counterid) AS s FROM hits GROUP BY RegionID ORDER BY RegionID`,
	`SELECT UserAgent, MAX(WatchID) AS m FROM hits GROUP BY UserAgent ORDER BY UserAgent`,
	`SELECT COUNT(*) AS c, SUM(WatchID) AS s, AVG(counterid) AS a FROM hits`,
	// join on a CamelCase key with a projection narrower than the table
	`SELECT r.RegionName, COUNT(*) AS c FROM hits h JOIN regions r ON h.RegionID = r.RegionID GROUP BY r.RegionName ORDER BY r.RegionName`,
	`SELECT h.UserAgent, r.tier FROM hits h JOIN regions r ON h.RegionID = r.RegionID ORDER BY h.UserAgent, r.tier`,
	`SELECT h.WatchID FROM hits h JOIN regions r ON h.RegionID = r.RegionID WHERE r.tier = 2 ORDER BY h.WatchID`,
	// window + sort
	`SELECT WatchID, ROW_NUMBER() OVER (PARTITION BY RegionID ORDER BY WatchID DESC) AS rn FROM hits ORDER BY WatchID`,
	`SELECT WatchID, RANK() OVER (ORDER BY counterid) AS rk FROM hits ORDER BY WatchID`,
}

func ccsLower(sql string) string {
	for _, c := range []string{"WatchID", "UserAgent", "RegionName", "RegionID"} {
		sql = strings.ReplaceAll(sql, c, strings.ToLower(c))
	}
	return sql
}

// TestCamelCaseSchemaAnswersWhatALowerCaseSchemaAnswers compares a MIXED-case schema against the identical
// all-lower one on the single-process arm and both DAG arms.
func TestCamelCaseSchemaAnswersWhatALowerCaseSchemaAnswers(t *testing.T) {
	ctx := context.Background()

	camelInfra := tmdInfra(t, ctx)
	ccsWrite(t, ctx, camelInfra, true)
	lowerInfra := tmdInfra(t, ctx)
	ccsWrite(t, ctx, lowerInfra, false)

	camelDAG := tmdCoordinator(t, ctx, camelInfra)
	lowerDAG := tmdCoordinator(t, ctx, lowerInfra)
	camelShuf := tmdCoordinator(t, ctx, camelInfra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	lowerShuf := tmdCoordinator(t, ctx, lowerInfra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	camelSingle := ccsStandalone(t, ctx, true)
	lowerSingle := ccsStandalone(t, ctx, false)

	type arm struct {
		name         string
		camel, lower func(string) ([]string, [][]any, error)
	}
	arms := []arm{
		{"single",
			func(s string) ([]string, [][]any, error) { return e3PosSingle(ctx, camelSingle, s) },
			func(s string) ([]string, [][]any, error) { return e3PosSingle(ctx, lowerSingle, s) }},
		{"dag",
			func(s string) ([]string, [][]any, error) { return e3PosDAG(ctx, camelDAG, s) },
			func(s string) ([]string, [][]any, error) { return e3PosDAG(ctx, lowerDAG, s) }},
		{"dagshuf",
			func(s string) ([]string, [][]any, error) { return e3PosDAG(ctx, camelShuf, s) },
			func(s string) ([]string, [][]any, error) { return e3PosDAG(ctx, lowerShuf, s) }},
	}

	for _, a := range arms {
		for _, sql := range ccsShapes {
			low := ccsLower(sql)
			_, crows, cerr := a.camel(sql)
			_, lrows, lerr := a.lower(low)
			if (cerr == nil) != (lerr == nil) {
				t.Errorf("[%s] ERR DIVERGENCE\n  sql: %s\n  camel: %v\n  lower: %v", a.name, sql, cerr, lerr)
				continue
			}
			if cerr != nil {
				continue
			}
			if got, want := ccsRender(crows), ccsRender(lrows); got != want {
				t.Errorf("[%s] VALUE DIVERGENCE\n  sql: %s\n  camel: %s  lower: %s", a.name, sql, got, want)
			}
		}
	}
}

func ccsRender(rows [][]any) string {
	var b strings.Builder
	for _, r := range rows {
		for _, c := range r {
			fmt.Fprintf(&b, "%v|", c)
		}
		b.WriteString("\n")
	}
	return b.String()
}
