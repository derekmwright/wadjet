// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The round-1 review's P2. #1141 decides which of PostgreSQL's two casts to an
// integer type an operand takes from that operand's DECLARATION, and three
// shapes had no declaration this layer could read from the EXPRESSION alone:
// an ARRAY element, a MAP value and a scalar subquery. They kept the rounding
// cast, so `CAST(arr[1] AS INTEGER)` over an ARRAY OF TEXT holding '2.5'
// answered 3 where the server raises, and
// `INSERT … SELECT CAST(arr[1] AS PORT)` put that 3 at REST in a column whose
// own text door refuses '2.5' — #1141's own symptom sentence, surviving
// through a container.
//
// The declarations exist, just not on the expression: a container's element
// type is on the child vector and a subquery's on the node the planner
// resolved. castOperandDeclaresText reads them from the BATCH.
//
// The second table is what a narrower repair would have broken: a container of
// DECIMAL must still ROUND, because `(ARRAY[2.5::numeric,1])[1]::integer` is 3
// on 17.11 and `(ARRAY['2.5','12'])[1]::integer` is 22P02 — the same split the
// column case has.
func TestACastOverAContainerElementReadsTheElementsDeclaration(t *testing.T) {
	ctx := context.Background()
	db := ccoOpen(t)

	for _, c := range []struct {
		name, expr, dest string
		pg               string
	}{
		{"array_of_text_element", `atxt[1]`, "INTEGER", `22P02 — (ARRAY['2.5','12'])[1]::integer`},
		{"array_of_text_element_bigint", `atxt[1]`, "BIGINT", `22P02 invalid input syntax for type bigint`},
		{"array_of_text_element_port", `atxt[1]`, "PORT", `wadjet's own type; its text door refuses '2.5'`},
		{"array_of_text_exponent", `atxt[3]`, "INTEGER", `22P02 — '1e3' is not an int4 spelling`},
		{"map_of_text_value", `ELEMENT_AT(mtxt, 'a')`, "INTEGER", `22P02`},
		{"scalar_subquery_over_text", `(SELECT s FROM cco WHERE k = 1)`, "INTEGER", `22P02`},
		{"row_field_of_text", `rw.f`, "INTEGER", `22P02`},
	} {
		t.Run("refuses/"+c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT CAST(%s AS %s) AS v FROM cco WHERE k = 1`, c.expr, c.dest)
			res, err := db.Query(ctx, sql)
			if err == nil {
				t.Fatalf("answered %v; PostgreSQL 17.11 says %s — a container element carrying "+
					"text takes the TEXT cast, as the same characters in a column do\n  SQL: %s",
					res.Rows, c.pg, sql)
			}
			if got := sqlerr.StateOf(err); got != "22P02" {
				t.Errorf("SQLSTATE %q, want 22P02\n  err: %v\n  PostgreSQL 17.11: %s\n  SQL: %s",
					got, err, c.pg, sql)
			}
		})
	}

	// THE OTHER HALF, and the one a wider repair would break: a container
	// whose element is a NUMBER still takes the numeric cast.
	for _, c := range []struct {
		name, expr string
		want       any
		pg         string
	}{
		{"array_of_decimal_rounds", `adec[1]`, int64(3), `(ARRAY[2.5::numeric])[1]::integer is 3`},
		{"array_of_decimal_rounds_negative", `adec[2]`, int64(-3), `-2.5 -> -3`},
		{"map_of_decimal_rounds", `ELEMENT_AT(mdec, 'a')`, int64(3), `numeric 2.5 -> 3`},
		{"array_of_text_whole_number_converts", `atxt[2]`, int64(12), `12`},
		{"array_of_int_passes_through", `aint[1]`, int64(7), `7`},
	} {
		t.Run("answers/"+c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT CAST(%s AS INTEGER) AS v FROM cco WHERE k = 1`, c.expr)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%v — PostgreSQL 17.11 answers %s\n  SQL: %s", err, c.pg, sql)
			}
			if got := res.Rows[0]["v"]; got != c.want {
				t.Errorf("= %#v, want %#v (PostgreSQL 17.11: %s)\n  SQL: %s", got, c.want, c.pg, sql)
			}
		})
	}

	// AND AT THE WRITER DOORS, which is where the value reached REST: the
	// INSERT must refuse rather than store a rounded 3 in a PORT column whose
	// own text door refuses '2.5'.
	t.Run("writer/insert_select_over_a_container_element", func(t *testing.T) {
		if _, err := db.Query(ctx, `CREATE TABLE ccosink (k BIGINT, p PORT)`); err != nil {
			t.Fatalf("create sink: %v", err)
		}
		const ins = `INSERT INTO ccosink (k, p) SELECT k, CAST(atxt[1] AS PORT) FROM cco WHERE k = 1`
		if _, err := db.Execute(ctx, ins); err == nil {
			t.Errorf("INSERT … SELECT answered; the PORT text door refuses '2.5' and this is "+
				"#1141's own symptom sentence through a container\n  SQL: %s", ins)
		}
		rows := mustRows(t, db, `SELECT p AS v FROM ccosink`)
		if len(rows) != 0 {
			t.Errorf("%d rows reached rest, want 0: %v", len(rows), rows)
		}
	})
	t.Run("writer/ctas_over_a_container_element", func(t *testing.T) {
		const ctas = `CREATE TABLE ccoctas AS SELECT CAST(atxt[1] AS INTEGER) AS v FROM cco WHERE k = 1`
		if _, err := db.Query(ctx, ctas); err == nil {
			t.Errorf("CTAS answered; PostgreSQL 17.11 raises 22P02 for the same cast\n  SQL: %s", ctas)
		}
	})
}

func ccoOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "atxt", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "aint", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
		{Name: "adec", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeDecimal,
				Precision: 10, Scale: 2, Nullable: true}},
		{Name: "mtxt", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeString, Nullable: true},
			}}},
		{Name: "mdec", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
			}}},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "f", Type: parquet.TypeString, Nullable: true},
		}},
	}}
	if err := db.CreateTable(ctx, "cco", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("cco", sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{
		"k": int64(1), "s": "2.5",
		"atxt": []any{"2.5", "12", "1e3"},
		"aint": []any{int64(7), int64(8)},
		"adec": []any{"2.50", "-2.50"},
		"mtxt": map[string]any{"a": "2.5"},
		"mdec": map[string]any{"a": "2.50"},
		"rw":   map[string]any{"f": "2.5"},
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
