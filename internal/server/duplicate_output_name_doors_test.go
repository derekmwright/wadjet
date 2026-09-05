package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TWO OUTPUT COLUMNS OF ONE NAME, on every door (#732 round-2, review B1/B2).
//
// PostgreSQL names an unaliased expression `?column?`, so `SELECT g+1, g+2, g+3`
// is three columns of ONE name and `SELECT COUNT(*), COUNT(g)` is two. That was
// a rare shape before the naming rule (`SELECT abs(a), abs(b)`); it is now the
// ordinary one, and it is exactly the shape a door that carries rows as a
// NAME-KEYED MAP cannot represent:
//
//   - the HTTP door derived its `columns` list from the row map's KEYS, so
//     three columns became one and two of the three VALUES were dropped;
//   - the gRPC door sent the full `columns` list beside a one-key map, so a
//     client zipping them read the last value under the first name;
//   - pgwire is positional and was right about the VALUES, but
//     `deriveColumnMetas` resolved each column's DECLARATION by NAME, so
//     `SELECT g + 1, s || 'x'` declared a bigint as TEXT and
//     `CAST(s AS VARCHAR(4)), CAST(s AS VARCHAR(9))` sent modifier 13 twice.
//
// Every cell drives DIFFERENTLY TYPED columns wherever the declaration is what
// is asserted: the wire corpus could not see B2 because every duplicate-name
// cell in it pairs two columns of the same type.
func TestDuplicateOutputNamesOnTheHTTPAndWireDoors(t *testing.T) {
	base, pg := hdSetup(t)

	t.Run("http/three-unnamed-columns", func(t *testing.T) {
		cols, rows, values := hdQuery(t, base, "SELECT id + 1, id + 2, id + 3 FROM hd")
		if len(cols) != 3 {
			t.Fatalf("columns = %v, want three (PostgreSQL sends three `?column?`)", cols)
		}
		for i, c := range cols {
			if c != "?column?" {
				t.Errorf("column %d = %q, want %q", i, c, "?column?")
			}
		}
		// The VALUES are the point: a map keyed by name carries one of the
		// three, so the positional form has to be there and has to be right.
		if len(values) != 1 || len(values[0]) != 3 {
			t.Fatalf("values = %v, want one row of three", values)
		}
		for i, want := range []float64{2, 3, 4} {
			if got, ok := values[0][i].(float64); !ok || got != want {
				t.Errorf("value %d = %v, want %v", i, values[0][i], want)
			}
		}
		if len(rows) != 1 {
			t.Fatalf("rows = %v, want one", rows)
		}
	})

	t.Run("http/two-aggregates-of-one-name", func(t *testing.T) {
		cols, _, values := hdQuery(t, base, "SELECT COUNT(*), COUNT(s) FROM hd")
		if len(cols) != 2 || cols[0] != "count" || cols[1] != "count" {
			t.Fatalf("columns = %v, want two `count`", cols)
		}
		if len(values) != 1 || len(values[0]) != 2 {
			t.Fatalf("values = %v, want one row of two", values)
		}
	})

	// A DIFFERENTLY TYPED pair, which is what the wire corpus could not see.
	t.Run("wire/duplicate-names-of-different-types", func(t *testing.T) {
		fields := wireFields(t, pg, "SELECT id + 1, s || 'x' FROM hd")
		if len(fields) != 2 {
			t.Fatalf("RowDescription has %d fields, want 2", len(fields))
		}
		if fields[0].oid == fields[1].oid {
			t.Fatalf("both columns declared OID %d; PostgreSQL declares int8 then text, "+
				"and a name-keyed declaration gives column 0 the LAST column's type",
				fields[0].oid)
		}
		if fields[0].oid != 20 {
			t.Errorf("column 0 (id + 1) OID = %d, want 20 (int8)", fields[0].oid)
		}
		if fields[1].oid != 25 {
			t.Errorf("column 1 (s || 'x') OID = %d, want 25 (text)", fields[1].oid)
		}
	})

	t.Run("wire/duplicate-names-of-different-modifiers", func(t *testing.T) {
		fields := wireFields(t, pg, "SELECT CAST(s AS VARCHAR(4)), CAST(s AS VARCHAR(9)) FROM hd")
		if len(fields) != 2 {
			t.Fatalf("RowDescription has %d fields, want 2", len(fields))
		}
		// PostgreSQL sends length+4 as the typmod: 8 and 13.
		if fields[0].mod == fields[1].mod {
			t.Fatalf("both columns declared modifier %d; PostgreSQL sends 8 then 13",
				fields[0].mod)
		}
		if fields[0].mod != 8 || fields[1].mod != 13 {
			t.Errorf("modifiers = %d, %d; want 8, 13", fields[0].mod, fields[1].mod)
		}
	})
}

// hdQuery runs one SELECT through the HTTP door and returns its column list,
// its name-keyed rows and its POSITIONAL values.
func hdQuery(t *testing.T, base, sql string) ([]string, []map[string]any, [][]any) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+"/v1/queries", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %q: %v", sql, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
		Values  [][]any          `json:"values"`
		Error   string           `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("POST %q returned %d with unparseable body %q", sql, resp.StatusCode, raw)
	}
	if out.Error != "" {
		t.Fatalf("POST %q: %s", sql, out.Error)
	}
	return out.Columns, out.Rows, out.Values
}

// wireField is one RowDescription field's declaration.
type wireField struct {
	name string
	oid  uint32
	mod  int32
}

func wireFields(t *testing.T, conn *pgconn.PgConn, sql string) []wireField {
	t.Helper()
	res := conn.ExecParams(context.Background(), sql, nil, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatalf("%s: %v", sql, res.Err)
	}
	out := make([]wireField, 0, len(res.FieldDescriptions))
	for _, f := range res.FieldDescriptions {
		out = append(out, wireField{name: f.Name, oid: f.DataTypeOID, mod: f.TypeModifier})
	}
	if len(out) == 0 {
		t.Fatalf("%s: no RowDescription fields", sql)
	}
	_ = fmt.Sprint
	return out
}
