// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc BY (#1501): a text becomes BYTES through ONE reading, PostgreSQL's
// byteain (kernel.ByteaIn), at every door — the explicit CAST, the comparison,
// decode() — and a BYTES value that is spliced back into SQL text (a scalar
// subquery's answer, a correlated re-run's outer value) is spelled so that
// reading gives the same bytes back.
//
// The fixture is built to DISCRIMINATE: by_t holds bytes whose own text is a
// valid bytea spelling of OTHER bytes (row 2 is the four bytes `\x41`, which
// byteain reads as `A` — row 3), and bytes holding two backslashes (row 4,
// which byteain reads as one); `A` is held twice, so the correlated count of
// row 2 is 1 under the right reading and 2 under the second one; row 8 holds a
// NUL and invalid UTF-8, which no quoted literal spells. A door that hands the raw bytes to byteain a
// second time answers a different row; a door that hands a literal's
// SPELLING on answers no row.
func byTables() []tmdTable {
	i64 := parquet.Column{Name: "id", Type: parquet.TypeInt64}
	t := tmdTable{name: "by_t", schema: parquet.Schema{Columns: []parquet.Column{
		i64, {Name: "b", Type: parquet.TypeBytes, Nullable: true}}}}
	for i, b := range [][]byte{[]byte("hi"), []byte(`\x41`), []byte("A"), []byte(`a\\b`), nil, []byte("hi"), []byte("A"), {0x00, 0xff}} {
		var v any
		if b != nil {
			v = b
		}
		t.rows = append(t.rows, map[string]any{"id": int64(i + 1), "b": v})
	}
	s := tmdTable{name: "by_s", schema: parquet.Schema{Columns: []parquet.Column{
		i64, {Name: "s", Type: parquet.TypeString, Nullable: true}}}}
	for i, v := range []any{`\x6869`, "hi", `a\b`, `\x686`, `\x 68 69`, nil} {
		s.rows = append(s.rows, map[string]any{"id": int64(i + 1), "s": v})
	}
	return []tmdTable{t, s}
}

// byPGFixture is byTables as PostgreSQL DDL. BYTES is a DOMAIN over bytea, so
// the cells' `CAST(x AS BYTES)` is `x::bytea` there.
const byPGFixture = `CREATE DOMAIN bytes AS bytea;
CREATE TABLE by_t (id bigint, b bytea);
INSERT INTO by_t VALUES (1, '\x6869'), (2, '\x5c783431'), (3, '\x41'), (4, '\x615c5c62'), (5, NULL), (6, '\x6869'), (7, '\x41'), (8, '\x00ff');
CREATE TABLE by_s (id bigint, s text);
INSERT INTO by_s VALUES (1, '\x6869'), (2, 'hi'), (3, 'a\b'), (4, '\x686'), (5, '\x 68 69'), (6, NULL);`

// byCells is the cell table; testdata/arc_by_bytea_input_pg17.tsv holds
// PostgreSQL 17.11's answer for every name.
var byCells = []struct{ name, sql string }{
	{"cast/lit/hex", `SELECT encode(CAST('\x6869' AS BYTES), 'hex') AS v`},
	{"cast/lit/plain", `SELECT encode(CAST('hi' AS BYTES), 'hex') AS v`},
	{"cast/lit/escape", `SELECT encode(CAST('a\\b\000c\134' AS BYTES), 'hex') AS v`},
	{"cast/lit/empty-hex", `SELECT encode(CAST('\x' AS BYTES), 'hex') AS v, length(CAST('\x' AS BYTES)) AS n`},
	{"cast/lit/utf8", `SELECT encode(CAST('é' AS BYTES), 'hex') AS v`},
	{"cast/lit/ws", `SELECT encode(CAST('\x 68 69 ' AS BYTES), 'hex') AS v`},
	{"cast/lit/upper-x", `SELECT encode(CAST('\X6869' AS BYTES), 'hex') AS v`},
	{"cast/lit/odd", `SELECT encode(CAST('\x686' AS BYTES), 'hex') AS v`},
	{"cast/lit/bad-digit", `SELECT encode(CAST('\x68zz' AS BYTES), 'hex') AS v`},
	{"cast/lit/ws-in-pair", `SELECT encode(CAST('\x6 869' AS BYTES), 'hex') AS v`},
	{"cast/lit/lone-backslash", `SELECT encode(CAST('a\b' AS BYTES), 'hex') AS v`},
	{"cast/lit/octal-past-byte", `SELECT encode(CAST('a\400' AS BYTES), 'hex') AS v`},
	{"cast/col/ok", `SELECT id, encode(CAST(s AS BYTES), 'hex') AS v FROM by_s WHERE id IN (1, 2, 5, 6)`},
	{"cast/col/lone-backslash", `SELECT id, encode(CAST(s AS BYTES), 'hex') AS v FROM by_s WHERE id = 3`},
	{"cast/col/odd", `SELECT id, encode(CAST(s AS BYTES), 'hex') AS v FROM by_s WHERE id = 4`},
	{"cast/col/join-key", `SELECT s.id, b.id FROM by_t b JOIN by_s s ON b.b = CAST(s.s AS BYTES) WHERE s.id IN (1, 2, 5)`},
	{"cast/col/bigint", `SELECT CAST(id AS BYTES) AS v FROM by_t WHERE id = 1`},
	{"cast/lit/numeric", `SELECT CAST(1.5 AS BYTES) AS v`},
	{"cast/lit/boolean", `SELECT CAST(true AS BYTES) AS v`},
	{"cast/col/identity", `SELECT id, encode(CAST(b AS BYTES), 'hex') AS v FROM by_t`},
	{"cast/col/text-roundtrip", `SELECT id, encode(CAST(CAST(b AS TEXT) AS BYTES), 'hex') AS v FROM by_t`},
	{"cast/where", `SELECT id FROM by_t WHERE b = CAST('\x6869' AS BYTES)`},
	{"cmp/hex", `SELECT id FROM by_t WHERE b = '\x6869'`},
	{"cmp/ws", `SELECT id FROM by_t WHERE b = '\x68 69'`},
	{"cmp/ws-in-pair", `SELECT id FROM by_t WHERE b = '\x6 869'`},
	{"cmp/odd", `SELECT id FROM by_t WHERE b = '\x686'`},
	{"cmp/bad-digit", `SELECT id FROM by_t WHERE b <> '\x68zz'`},
	{"cmp/upper-x", `SELECT id FROM by_t WHERE b = '\X6869'`},
	{"cmp/escape", `SELECT id FROM by_t WHERE b = 'a\\\\b'`},
	{"cmp/lone-backslash", `SELECT id FROM by_t WHERE b = 'a\b'`},
	{"cmp/in", `SELECT id FROM by_t WHERE b IN ('\x41', '\x5c783431')`},
	{"read/rows", `SELECT id, b, length(b) AS n, CAST(b AS TEXT) AS t FROM by_t`},
	{"read/join", `SELECT a.id, c.id FROM by_t a JOIN by_t c ON a.b = c.b WHERE a.id < c.id`},
	{"read/group", `SELECT encode(b, 'hex') AS h, count(*) AS n FROM by_t GROUP BY b`},
	{"scalar/where-backslash", `SELECT id FROM by_t WHERE b = (SELECT b FROM by_t WHERE id = 2)`},
	{"scalar/where-two-backslashes", `SELECT id FROM by_t WHERE b = (SELECT b FROM by_t WHERE id = 4)`},
	{"scalar/select", `SELECT encode((SELECT b FROM by_t WHERE id = 2), 'hex') AS v`},
	{"correlated/count", `SELECT o.id, (SELECT count(*) FROM by_t i WHERE i.b = o.b) AS n FROM by_t o WHERE o.b IS NOT NULL`},
	{"correlated/exists", `SELECT o.id FROM by_t o WHERE EXISTS (SELECT 1 FROM by_t i WHERE i.b = o.b AND i.id <> o.id)`},
	// Without the NUL row, which c67ebf5b refused outright: the outer value
	// holding `\x41` and the one holding two backslashes are the re-read.
	{"correlated/count-no-nul", `SELECT o.id, (SELECT count(*) FROM by_t i WHERE i.b = o.b) AS n FROM by_t o WHERE o.id < 8 AND o.b IS NOT NULL`},
	{"correlated/select-outer", `SELECT o.id, encode((SELECT o.b FROM by_s x WHERE x.id = 1), 'hex') AS h FROM by_t o`},
	{"decode/hex-ws", `SELECT encode(decode('68 69', 'hex'), 'hex') AS v`},
	{"decode/hex-odd", `SELECT encode(decode('686', 'hex'), 'hex') AS v`},
	{"decode/escape-bad", `SELECT encode(decode('a\b', 'escape'), 'hex') AS v`},
	{"decode/escape-ok", `SELECT encode(decode('a\\b\000', 'escape'), 'hex') AS v`},
}

func byPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_by_bytea_input_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	return out
}

// byRender is PostgreSQL's psql -At rendering: a BYTES cell in bytea_output
// hex, NULL as NULL, rows sorted.
func byRender(res *oracle.Result) string {
	var rows []string
	for _, r := range brCells(res) {
		cells := make([]string, len(r))
		for i, v := range r {
			switch x := v.(type) {
			case nil:
				cells[i] = "NULL"
			case []byte:
				cells[i] = `\x` + hex.EncodeToString(x)
			default:
				cells[i] = fmt.Sprint(x)
			}
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
}

func byStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range byTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

func byArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, byTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := byStandalone(t, ctx, 0), byStandalone(t, ctx, 512*1024)
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, db, sql) }
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, c, sql) }
	}
	return []brArm{
		{"single", runSingle(single)},
		{"spilled512k", runSingle(spilled)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

// TestArcBYByteaInputEveryArm holds every cell to PostgreSQL 17.11's answer on
// all five arms: the value, or the refusal's SQLSTATE and message.
func TestArcBYByteaInputEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the bytea input table")
	}
	answers := byPGAnswers(t)
	for _, c := range byCells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := byArms(t, ctx)
	answered := 0
	for _, tc := range byCells {
		want := answers[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				res *oracle.Result
				err error
			}
			results := make([]result, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i].res, results[i].err = arm.run(tc.sql)
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				res, err := results[i].res, results[i].err
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (PostgreSQL 17.11)", tc.sql, arm.name, byRender(res), state, msg)
					} else if st := sqlerr.StateOf(err); st != state || !strings.Contains(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q (PostgreSQL 17.11)", tc.sql, arm.name, st, err, state, msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, err, want)
					continue
				}
				if got := byRender(res); got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, want)
					continue
				}
				answered++
			}
		})
	}
	// Non-vacuous: the answered cells × arms the table expects.
	if want := 5 * 27; answered < want && !t.Failed() {
		t.Errorf("only %d (cell, arm) answers compared, want at least %d", answered, want)
	}
}
