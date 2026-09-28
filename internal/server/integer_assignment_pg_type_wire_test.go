// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/intround"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/jackc/pgx/v5"
)

// The pgwire door of wadjet.TestIntegerAssignmentRoundsByPostgresTypeOnEveryDoor:
// the same intround table — every operator and double-precision function over
// every operand category, the CASE family and the plan constructs, on VALUES,
// INSERT … SELECT, UPDATE and MERGE — sent as a client sends it, and the rows
// read back over the wire. PostgreSQL 17.11 rounds a numeric source half away
// from zero and a float8 half to even; this engine computes several numeric
// expressions in float64, and the declaration's category is what picks the
// rule (#1353).
func TestIntegerAssignmentRoundsByPostgresTypeOverPgwire(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up a pgwire door and runs the whole table")
	}
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	pg := pgwire.NewServer(db, pgwire.Config{}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", pg.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	// The same pre-assignment refusals the embedded table pins (a SELECT of
	// GREATEST(numeric, float8) whose numeric arm wins, a SELECT of
	// NULLIF(numeric column, float8 expression)), for the same reason.
	pinned := map[string]string{
		"fold/GREATEST(numcol,f8col-1) [select]":     "cannot store string into FLOAT64 vector",
		"fold/GREATEST(div_numcol,f8col-1) [select]": "cannot store string into FLOAT64 vector",
		"fold/NULLIF(numcol,f8col+9) [select]":       "cannot store string into FLOAT64 vector",
		"fold/NULLIF(numcol,f8lit) [select]":         "cannot store string into FLOAT64 vector",
		"fold/NULLIF(div_numcol,f8col+9) [select]":   "cannot store string into FLOAT64 vector",
		"fold/NULLIF(div_numcol,f8lit) [select]":     "cannot store string into FLOAT64 vector",
	}
	cells := append(append(intround.Cells(), intround.DialectCells()...), intround.MergeSourceCells()...)
	failed := 0
	for _, c := range cells {
		key := c.Name + " [" + c.Door + "]"
		got, err := wireIntRoundCell(ctx, conn, c)
		pin, ok := pinned[key]
		if !ok {
			pin, ok = intround.MergeSourceRefusal(c.Name)
		}
		if ok {
			if err == nil || !strings.Contains(err.Error(), pin) {
				failed++
				t.Errorf("%s: the pinned refusal moved (stored %s, err %v); if it now stores %s, delete the pin",
					key, got, err, c.Want)
			}
			continue
		}
		if err != nil {
			failed++
			t.Errorf("%s: %v", key, err)
			continue
		}
		if got != c.Want {
			failed++
			t.Errorf("%s: stored %s, PostgreSQL stores %s", key, got, c.Want)
		}
	}
	if failed > 0 {
		t.Logf("%d of %d cells differ from PostgreSQL 17.11 over pgwire", failed, len(cells))
	}
}

// wireIntRoundCell rebuilds the fixture — a table rewritten thousands of times
// over grows a file per statement, and the cells are independent — runs the
// cell and reads the target back as the wire delivers it.
func wireIntRoundCell(ctx context.Context, conn *pgx.Conn, c intround.Cell) (string, error) {
	for _, tbl := range []string{"src", "t_v", "t_s", "t_u", "t_m", "t_a"} {
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			return "", fmt.Errorf("drop %s: %w", tbl, err)
		}
	}
	for _, s := range intround.Fixture {
		if _, err := conn.Exec(ctx, s); err != nil {
			return "", fmt.Errorf("fixture %s: %w", s, err)
		}
	}
	for _, s := range c.Stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return "", fmt.Errorf("%s: %w", s, err)
		}
	}
	rows, err := conn.Query(ctx, c.Read)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return "", err
		}
		part := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				part[i] = "NULL"
				continue
			}
			part[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(part, ":"))
	}
	return strings.Join(out, " "), rows.Err()
}
