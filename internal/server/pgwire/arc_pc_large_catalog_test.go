// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// pcCountingKV is the catalog's MemKV with every read counted: a value read,
// a revision probe and a key listing are each one read of the catalog store.
// It forwards MemKV's optional capabilities (RevisionReader,
// GenerationReader), so the catalog takes the same cache paths it takes over
// a bare MemKV.
type pcCountingKV struct {
	*catalog.MemKV
	reads atomic.Int64
}

func (k *pcCountingKV) Get(key string) ([]byte, uint64, error) {
	k.reads.Add(1)
	return k.MemKV.Get(key)
}

func (k *pcCountingKV) Revision(key string) (uint64, error) {
	k.reads.Add(1)
	return k.MemKV.Revision(key)
}

func (k *pcCountingKV) List(prefix string) ([]string, error) {
	k.reads.Add(1)
	return k.MemKV.List(prefix)
}

// TestArcPCLargeCatalogDescribeReadsDoNotGrowWithTableCount is P1's gate
// (arc PC): psql's `\d` listing and `\d table` (the exact statements psql 17
// sends, from the tool corpus) must not do per-table work for the tables
// they do not describe. The round-1 tip did: every catalog scan decoded every
// user table's definition, and every result decoded every definition again
// for a column-type fallback, so over 1,000 tables of 20 columns `\d` took
// 336.8 ms and `\d table` 2,072 ms (PostgreSQL 17.11 on the review host,
// median of five psql runs: 55.6 ms and 50.6 ms). Round 3 found the same
// shape on a store with no revision probe (a read per table per scan).
//
// The assertion is the work, not the wall clock: the same commands are run
// over a 100-table and a 1,000-table catalog, warmed, and the number of
// catalog-store reads each issues must be the same at both sizes. A
// per-table read in a describe path adds at least 900 reads per scan at the
// larger size. An earlier form of this test held the median wall time under
// three times the PostgreSQL figure above; it failed on a loaded runner with
// no change to the describe path (#1711), because a median taken under
// parallel load says how busy the machine was. The medians are still logged,
// beside the PostgreSQL figures, as context.
func TestArcPCLargeCatalogDescribeReadsDoNotGrowWithTableCount(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: creates 1,100 tables")
	}
	ctx := context.Background()
	byName := map[string]string{}
	for _, e := range pcLoadCorpus(t) {
		byName[e.Name] = e.SQL
	}
	var detail []string
	for i := 1; i <= 8; i++ {
		detail = append(detail, fmt.Sprintf(`psql \d o #%d`, i))
	}

	type catalogUnderTest struct {
		tables int
		kv     *pcCountingKV
		conn   *pgx.Conn
		target string // the table `\d table` describes
		oid    string
	}
	open := func(tables int) *catalogUnderTest {
		kv := &pcCountingKV{MemKV: catalog.NewMemKV()}
		db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test", MetaKV: kv})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		var cols []parquet.Column
		for j := 0; j < 20; j++ {
			cols = append(cols, parquet.Column{Name: fmt.Sprintf("c%02d", j), Type: parquet.TypeInt64, Nullable: true})
		}
		for i := 0; i < tables; i++ {
			if err := db.CreateTable(ctx, fmt.Sprintf("perf_%04d", i), parquet.Schema{Columns: cols}, nil); err != nil {
				t.Fatal(err)
			}
		}
		target := fmt.Sprintf("perf_%04d", tables/2)
		res, err := db.Query(ctx, "SELECT oid FROM pg_class WHERE relname = '"+target+"'")
		if err != nil || len(res.Rows) != 1 {
			t.Fatalf("resolving %s: %v %v", target, res, err)
		}
		srv := startTestServer(t, db)
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		return &catalogUnderTest{tables: tables, kv: kv, conn: conn, target: target, oid: fmt.Sprint(res.Rows[0]["oid"])}
	}
	small, large := open(100), open(1000)

	command := func(c *catalogUnderTest, names []string) []string {
		var out []string
		for _, n := range names {
			sql, ok := byName[n]
			if !ok {
				t.Fatalf("corpus statement %q is missing", n)
			}
			sql = strings.ReplaceAll(sql, "'^(o)$'", "'^("+c.target+")$'")
			out = append(out, pcResolve(sql, c.oid))
		}
		return out
	}
	// run issues one command and returns its wall time and catalog reads.
	run := func(c *catalogUnderTest, name string, names []string) (time.Duration, int64) {
		stmts := command(c, names)
		before := c.kv.reads.Load()
		start := time.Now()
		for _, sql := range stmts {
			if o := pcRawText(ctx, c.conn, sql); o.Err != "" {
				t.Fatalf("%s over %d tables: %s\n  SQL: %s", name, c.tables, o.Err, sql)
			}
		}
		return time.Since(start), c.kv.reads.Load() - before
	}
	median := func(d []time.Duration) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		return d[len(d)/2]
	}

	for _, c := range []struct {
		name  string
		names []string
		pg    time.Duration
	}{
		{`\d`, []string{`psql \d #1`}, 55600 * time.Microsecond},
		{`\d table`, detail, 50600 * time.Microsecond},
	} {
		// The first run fills the caches a catalog scan keeps (a cold scan
		// reads every definition once, by design); the gate is every warm
		// run after it.
		run(small, c.name, c.names)
		run(large, c.name, c.names)
		var smallTimes, largeTimes []time.Duration
		var smallReads, largeReads []int64
		for i := 0; i < 5; i++ {
			d, r := run(small, c.name, c.names)
			smallTimes, smallReads = append(smallTimes, d), append(smallReads, r)
			d, r = run(large, c.name, c.names)
			largeTimes, largeReads = append(largeTimes, d), append(largeReads, r)
		}
		t.Logf("%s: catalog reads per run %v over 100 tables, %v over 1,000; median %v and %v (PostgreSQL 17.11 over 1,000 tables: %v)",
			c.name, smallReads, largeReads, median(smallTimes), median(largeTimes), c.pg)
		for i := range largeReads {
			if largeReads[i] != smallReads[i] {
				t.Errorf("%s run %d: %d catalog reads over 1,000 tables against %d over 100; a describe must not read per table",
					c.name, i+1, largeReads[i], smallReads[i])
			}
		}
	}
}
