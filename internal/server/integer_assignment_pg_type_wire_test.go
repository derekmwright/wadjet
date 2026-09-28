// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/intround"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// The pgwire door of wadjet.TestMergeSetExpressionTableOnEveryForm: the MERGE
// action's expression table (intround.MergeSetCells) sent as a client sends
// it, the SQLSTATE read off the wire, plus the one form only this door can
// bind — a parameter, typed by the client (float8, numeric, int4, text) or
// left to the server — against PostgreSQL 17.11's answers.
func TestMergeSetExpressionTableOverPgwire(t *testing.T) {
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

	var cells []wireMergeSetCase
	for _, c := range intround.MergeSetCells() {
		cells = append(cells, wireMergeSetCase{Cell: c})
	}
	// The parameter rows, measured on PostgreSQL 17.11 with PREPARE p(<type>)
	// (typed) and PREPARE p (untyped): a typed parameter assigns by its type's
	// rule — float8 half to even, numeric half away from zero — and text is
	// 42804; an untyped one is typed from its target column, so the fraction
	// is 22P02 and an integer's text answers. (An untyped $1 feeding both the
	// INTEGER and the BIGINT column is 42P08 there, so each has its own.)
	const merge = "MERGE INTO t USING s ON t.id = s.id "
	for _, p := range []struct {
		name      string
		oids      []uint32
		vals      []string
		want, err string
	}{
		{"float8", []uint32{701}, []string{"2.5"}, "1:2:2 2:2:2 3:2:2 4:2:2", ""},
		{"numeric", []uint32{1700}, []string{"2.5"}, "1:3:3 2:3:3 3:3:3 4:3:3", ""},
		{"int4", []uint32{23}, []string{"5"}, "1:5:5 2:5:5 3:5:5 4:5:5", ""},
		{"text", []uint32{25}, []string{"2.5"}, "", "42804"},
		{"untyped-fraction", []uint32{0, 0}, []string{"2.5", "2.5"}, "", "22P02"},
		{"untyped-integer", []uint32{0, 0}, []string{"2", "3"}, "1:2:3 2:2:3 3:2:3 4:2:3", ""},
	} {
		second := "$1"
		if len(p.vals) == 2 {
			second = "$2"
		}
		for _, a := range []struct{ door, reset, action string }{
			{"merge-update", "UPDATE t SET n4 = 0, n8 = 0", "WHEN MATCHED THEN UPDATE SET n4 = $1, n8 = " + second},
			{"merge-insert", "DELETE FROM t", "WHEN NOT MATCHED THEN INSERT (id, n4, n8) VALUES (s.id, $1, " + second + ")"},
		} {
			cells = append(cells, wireMergeSetCase{
				Cell: intround.Cell{Name: "merge-set/parameter/" + p.name + "/catalog/" + strings.TrimPrefix(a.door, "merge-"),
					Door: a.door, Stmts: []string{a.reset, merge + a.action},
					Read: "SELECT id, n4, n8 FROM t ORDER BY id", Want: p.want, WantErr: p.err},
				oids: p.oids, vals: p.vals,
			})
		}
	}
	failed := 0
	for _, c := range cells {
		got, state, err := wireMergeSetCell(ctx, conn, c)
		if msg := wireMergeSetVerdict(c.Cell, got, state, err); msg != "" {
			failed++
			t.Errorf("%s: %s", c.Name, msg)
		}
	}
	if failed > 0 {
		t.Logf("%d of %d MERGE SET cells differ from PostgreSQL 17.11 over pgwire", failed, len(cells))
	}
}

// wireMergeSetCase is a MERGE SET cell, and for a parameter row the values
// and type OIDs its last statement binds (0: the server decides).
type wireMergeSetCase struct {
	intround.Cell
	oids []uint32
	vals []string
}

// wireParamRefusals are the parameter rows that refuse with another SQLSTATE
// than PostgreSQL's: Bind renders a text-typed parameter as a quoted literal
// (SQL's unknown), so the INTEGER column's input function reads `2.5` and
// raises 22P02, where PostgreSQL keeps the parameter's declared text type and
// raises 42804 for assigning text to an integer. Both write nothing. A pin
// that moves fails: re-measure it.
var wireParamRefusals = map[string]string{
	"merge-set/parameter/text/catalog/update": "22P02",
	"merge-set/parameter/text/catalog/insert": "22P02",
}

func wireMergeSetVerdict(c intround.Cell, got, state string, err error) string {
	pin, ok := intround.MergeSetRefusal(c)
	if !ok {
		pin, ok = wireParamRefusals[c.Name]
	}
	if ok {
		if err == nil || state != pin {
			return fmt.Sprintf("the pinned %s refusal moved (stored %s, err %v); if it now stores %s, delete the pin",
				pin, got, err, c.Want)
		}
		return ""
	}
	if c.WantErr != "" {
		if err == nil {
			return fmt.Sprintf("stored %s where PostgreSQL raises %s", got, c.WantErr)
		}
		if state != c.WantErr {
			return fmt.Sprintf("raised %s (%v) where PostgreSQL raises %s", state, err, c.WantErr)
		}
		return ""
	}
	if err != nil {
		return fmt.Sprintf("raised %s (%v) where PostgreSQL stores %s", state, err, c.Want)
	}
	if got != c.Want {
		return fmt.Sprintf("stored %s, PostgreSQL stores %s", got, c.Want)
	}
	return ""
}

// wireMergeSetCell rebuilds MergeSetFixture, runs the cell and reads t back
// as the wire delivers it. A refused statement must have written nothing.
func wireMergeSetCell(ctx context.Context, conn *pgx.Conn, c wireMergeSetCase) (string, string, error) {
	for _, tbl := range []string{"s0", "s", "t"} {
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			return "", "", fmt.Errorf("drop %s: %w", tbl, err)
		}
	}
	for _, s := range intround.MergeSetFixture {
		if _, err := conn.Exec(ctx, s); err != nil {
			return "", "", fmt.Errorf("fixture %s: %w", s, err)
		}
	}
	for i, s := range c.Stmts {
		var err error
		if i == len(c.Stmts)-1 && c.oids != nil {
			vals := make([][]byte, len(c.vals))
			for j, v := range c.vals {
				vals[j] = []byte(v)
			}
			err = conn.PgConn().ExecParams(ctx, s, vals, c.oids, nil, nil).Read().Err
		} else {
			_, err = conn.Exec(ctx, s)
		}
		if err != nil {
			state := "(uncoded)"
			var pe *pgconn.PgError
			if errors.As(err, &pe) {
				state = pe.Code
			}
			if i > 0 {
				untouched := "1:0:0 2:0:0 3:0:0 4:0:0"
				if c.Door == "merge-insert" {
					untouched = ""
				}
				if got, rerr := wireReadRows(ctx, conn, c.Read); rerr == nil && got != untouched {
					// never equal to a SQLSTATE, so no verdict accepts it
					return got, "wrote-then-" + state, fmt.Errorf("refused AFTER writing %s: %w", got, err)
				}
			}
			return "", state, err
		}
	}
	got, err := wireReadRows(ctx, conn, c.Read)
	return got, "", err
}

// wireReadRows reads a query's rows as "c1:c2:… c1:c2:…".
func wireReadRows(ctx context.Context, conn *pgx.Conn, q string) (string, error) {
	rows, err := conn.Query(ctx, q)
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
