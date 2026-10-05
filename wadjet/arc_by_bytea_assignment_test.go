// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1501: a text assigned to a BYTES column is read by byteain — the reading a
// comparison of the same column gives it (#582) — at every write door, and a
// text byteain refuses stores NOTHING.
//
// At c67ebf5b every assignment door handed the text on, so `VALUES
// ('\x6869')` stored the six characters `\x6869` and the row did not match
// `b = '\x6869'`, the literal it was written with; `'a\b'` and `'\x686'` were
// stored where PostgreSQL refuses, and a multi-row INSERT holding one of them
// stored every row. `CAST(text AS BYTES)` was a pass-through declared STRING,
// so CTAS over it minted a STRING column and INSERT … SELECT of it was 42804.
//
// The statements and every want are PostgreSQL 17.11's over the same script
// (by_author/emb.sql, emb_pg.out; the column is bytea there). The store is
// closed and reopened from its data directory between the writes and the
// reads, so what is checked is what was STORED.
func TestArcBYBytesAssignmentStoresByteainBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	big := `\x` + strings.Repeat("ab", 512*1024) // a 1 MiB hex text
	for _, s := range []struct{ sql, state string }{
		{`CREATE TABLE bt (id BIGINT, b BYTES)`, ""},
		{`CREATE TABLE bsrc (id BIGINT, s TEXT)`, ""},
		{`INSERT INTO bsrc VALUES (1, '\x6869'), (2, 'a\\b')`, ""},
		{`INSERT INTO bt VALUES (1, '\x6869'), (2, 'hi'), (3, E'\\x6869'), (4, 'a\\b'), (5, 'a\000b'), (6, '\x'), (7, ''), (8, 'é'), (9, '\x 68 69'), (10, NULL), (11, 'a\134b')`, ""},
		{`INSERT INTO bt VALUES (12, '` + big + `')`, ""},
		{`INSERT INTO bt SELECT 13, '\x6869'`, ""},
		{`INSERT INTO bt SELECT 14 + id, CAST(s AS BYTES) FROM bsrc`, ""},
		{`UPDATE bt SET b = '\x7a7a' WHERE id = 2`, ""},
		{`MERGE INTO bt USING (SELECT 17 AS id) s ON bt.id = s.id WHEN NOT MATCHED THEN INSERT (id, b) VALUES (s.id, '\x6869')`, ""},
		{`MERGE INTO bt USING (SELECT 17 AS id) s ON bt.id = s.id WHEN MATCHED THEN UPDATE SET b = '\x7a'`, ""},
		{`CREATE TABLE bc AS SELECT 1 AS id, CAST('\x6869' AS BYTES) AS b`, ""},
		// The refusals. Each stores nothing: the multi-row INSERT's good row
		// is not written either, and the UPDATEs leave row 1 as it was.
		{`INSERT INTO bt VALUES (20, '\x6869'), (21, 'a\b')`, "22P02"},
		{`INSERT INTO bt VALUES (22, '\x686')`, "22023"},
		{`INSERT INTO bt VALUES (23, '\X6869')`, "22P02"},
		{`INSERT INTO bt VALUES (24, '\x68zz')`, "22023"},
		{`UPDATE bt SET b = '\x68zz' WHERE id = 1`, "22023"},
		{`UPDATE bt SET b = 'a\b' WHERE id = 1`, "22P02"},
	} {
		_, err := db.Query(ctx, s.sql)
		label := s.sql
		if len(label) > 120 {
			label = label[:120] + "…"
		}
		switch {
		case s.state == "" && err != nil:
			t.Errorf("%s\n  refused: %v (PostgreSQL 17.11 executes it)", label, err)
		case s.state != "" && err == nil:
			t.Errorf("%s\n  executed; PostgreSQL 17.11 refuses with %s", label, s.state)
		case s.state != "" && sqlerr.StateOf(err) != s.state:
			t.Errorf("%s\n  refused %s %v; PostgreSQL 17.11 refuses with %s", label, sqlerr.StateOf(err), err, s.state)
		}
	}
	db.Close()

	db, err = Open(ctx, Config{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	for _, c := range []struct{ sql, want string }{
		{`SELECT id, encode(b, 'hex'), length(b) FROM bt WHERE id <> 12 ORDER BY id`,
			"1|6869|2 ; 2|7a7a|2 ; 3|6869|2 ; 4|615c62|3 ; 5|610062|3 ; 6||0 ; 7||0 ; 8|c3a9|2 ; 9|6869|2 ; 10|| ; " +
				"11|615c62|3 ; 13|6869|2 ; 15|6869|2 ; 16|615c62|3 ; 17|7a|1"},
		{`SELECT length(b), substr(encode(b, 'hex'), 1, 8) FROM bt WHERE id = 12`, "524288|abababab"},
		// The literal finds the rows written with it — the issue's own cell.
		{`SELECT count(*) FROM bt WHERE b = '\x6869'`, "5"},
		{`SELECT encode(b, 'hex'), count(*) FROM bt WHERE id <> 12 GROUP BY b ORDER BY 1`,
			"|2 ; 610062|1 ; 615c62|3 ; 6869|5 ; 7a|1 ; 7a7a|1 ; c3a9|1 ; |1"},
		{`SELECT count(*) FROM bt x JOIN bt y ON x.b = y.b WHERE x.id < y.id`, "14"},
		{`SELECT CAST(b AS TEXT) FROM bt WHERE id = 1`, `\x6869`},
		{`SELECT id, encode(b, 'hex') FROM bc`, "1|6869"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s\n  refused: %v", c.sql, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			var f []string
			for _, v := range res.Cells(i) {
				if v == nil {
					f = append(f, "")
					continue
				}
				f = append(f, fmt.Sprint(v))
			}
			rows = append(rows, strings.Join(f, "|"))
		}
		if got := strings.Join(rows, " ; "); got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
}

// byBaseStoreRows are the bytes c67ebf5b's assignment door stored for
// by_author/write_base_store.sh's INSERT — the SPELLING of every text but the
// plain one, and the escape forms' characters undecoded.
var byBaseStoreRows = []struct {
	id  int64
	hex string // "" with null=true is a NULL
}{
	{1, "5c7836383639"}, {2, "6869"}, {3, "615c5c62"}, {4, "615c30303062"},
	{5, "5c78363836"}, {6, ""}, {7, "615c62"}, {8, "5c78"},
}

// TestArcBYBaseWrittenStoreKeepsItsBytes: rows the base binary stored keep
// reading as the bytes they ARE. The fix reads TEXT on its way in; it never
// reinterprets a stored value, so the six characters `\x6869` written by
// c67ebf5b stay six bytes, are not matched by `b = '\x6869'`, and are matched
// by the hex spelling of those six bytes.
//
// BY_BASE_STORE names a data directory the base BINARY wrote
// (by_author/write_base_store.sh with a c67ebf5b build of cmd/wadjet); it is
// opened from a copy. Without it the same bytes are written through the
// ingester, which stores a []byte verbatim — the store the base binary
// produces, byte for byte, for these eight rows.
func TestArcBYBaseWrittenStoreKeepsItsBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if src := os.Getenv("BY_BASE_STORE"); src != "" {
		if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
			t.Fatalf("copying the base store: %v", err)
		}
	} else {
		db, err := Open(ctx, Config{DataDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		sc := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeBytes, Nullable: true},
		}}
		if err := db.CreateTable(ctx, "bb", sc, nil); err != nil {
			t.Fatal(err)
		}
		var rows []map[string]any
		for _, r := range byBaseStoreRows {
			var v any
			if r.id != 6 {
				raw, _ := hex.DecodeString(r.hex)
				v = raw
			}
			rows = append(rows, map[string]any{"id": r.id, "b": v})
		}
		ing := db.NewIngester("bb", sc, nil, ingest.Config{MaxBufferRows: 16})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	db, err := Open(ctx, Config{DataDir: dir})
	if err != nil {
		t.Fatalf("opening the base-written store: %v", err)
	}
	defer db.Close()
	var want []string
	for _, r := range byBaseStoreRows {
		want = append(want, fmt.Sprintf("%d|%s", r.id, r.hex))
	}
	for _, c := range []struct{ sql, want string }{
		{`SELECT id, encode(b, 'hex') FROM bb ORDER BY id`, strings.Join(want, " ; ")},
		{`SELECT id FROM bb WHERE b = '\x6869' ORDER BY id`, "2"},
		{`SELECT id FROM bb WHERE b = '\x5c7836383639' ORDER BY id`, "1"},
		{`SELECT id FROM bb WHERE b = 'a\\\\b' ORDER BY id`, "3"},
		{`SELECT id, CAST(b AS TEXT), length(b) FROM bb WHERE id IN (1, 3) ORDER BY id`, `1|\x5c7836383639|6 ; 3|\x615c5c62|4`},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s\n  refused: %v", c.sql, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			var f []string
			for _, v := range res.Cells(i) {
				if v == nil {
					f = append(f, "")
					continue
				}
				f = append(f, fmt.Sprint(v))
			}
			rows = append(rows, strings.Join(f, "|"))
		}
		if got := strings.Join(rows, " ; "); got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s", c.sql, got, c.want)
		}
	}
}
