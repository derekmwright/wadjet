// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcPCLargeCatalogDescribesWithinThreeTimesPostgreSQL is P1's gate: over
// 1,000 tables of 20 columns, psql's `\d` listing and `\d table` (the exact
// statements psql 17 sends, from the tool corpus) each finish within THREE
// TIMES what PostgreSQL 17.11 took for the same command on the same catalog,
// measured by the arc PC review (median of five psql runs: `\d` 55.6 ms,
// `\d perf_0500` 50.6 ms). A catalog scan reads cached per-table descriptors
// keyed by the table definition's KV revision and resolves regclass through
// an index; the round-1 tip took 336.8 ms and 2,072 ms.
func TestArcPCLargeCatalogDescribesWithinThreeTimesPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: creates 1,000 tables")
	}
	db := pcCorpusDB(t)
	ctx := context.Background()
	var cols []parquet.Column
	for j := 0; j < 20; j++ {
		cols = append(cols, parquet.Column{Name: fmt.Sprintf("c%02d", j), Type: parquet.TypeInt64, Nullable: true})
	}
	for i := 0; i < 1000; i++ {
		if err := db.CreateTable(ctx, fmt.Sprintf("perf_%04d", i), parquet.Schema{Columns: cols}, nil); err != nil {
			t.Fatal(err)
		}
	}
	srv := startTestServer(t, db)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	res, err := db.Query(ctx, "SELECT oid FROM pg_class WHERE relname = 'perf_0500'")
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("resolving perf_0500: %v %v", res, err)
	}
	oid := fmt.Sprint(res.Rows[0]["oid"])

	byName := map[string]string{}
	for _, e := range pcLoadCorpus(t) {
		byName[e.Name] = e.SQL
	}
	command := func(names ...string) []string {
		var out []string
		for _, n := range names {
			sql, ok := byName[n]
			if !ok {
				t.Fatalf("corpus statement %q is missing", n)
			}
			sql = strings.ReplaceAll(sql, "'^(o)$'", "'^(perf_0500)$'")
			out = append(out, pcResolve(sql, oid))
		}
		return out
	}
	var detail []string
	for i := 1; i <= 8; i++ {
		detail = append(detail, fmt.Sprintf(`psql \d o #%d`, i))
	}
	for _, c := range []struct {
		name  string
		stmts []string
		pg    time.Duration
	}{
		{`\d`, command(`psql \d #1`), 55600 * time.Microsecond},
		{`\d perf_0500`, command(detail...), 50600 * time.Microsecond},
	} {
		var samples []time.Duration
		for i := 0; i < 5; i++ {
			start := time.Now()
			for _, sql := range c.stmts {
				if o := pcRawText(ctx, conn, sql); o.Err != "" {
					t.Fatalf("%s: %s\n  SQL: %s", c.name, o.Err, sql)
				}
			}
			samples = append(samples, time.Since(start))
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		median := samples[len(samples)/2]
		t.Logf("%s over 1,000 tables: median %v (PostgreSQL 17.11: %v)", c.name, median, c.pg)
		if median > 3*c.pg {
			t.Errorf("%s over 1,000 tables took %v (median of 5), over 3x PostgreSQL 17.11's %v",
				c.name, median, c.pg)
		}
	}
}
