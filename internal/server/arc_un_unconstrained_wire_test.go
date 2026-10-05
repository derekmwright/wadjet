// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/structpb"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A COLUMN CREATED FROM AN UNCONSTRAINED NUMERIC ON THE WIRE (#1541,
// ADR-0024 §10): `CREATE TABLE un_w (… v NUMERIC …)` and a CREATE TABLE AS
// column from `CAST(id AS NUMERIC)` are declared as PostgreSQL 17.11 declares
// them — OID 1700, typmod −1 — and their values go out without the stored
// scale's trailing zeros in the text format, and with that value's own
// dscale in the binary format (PostgreSQL's `1.25` is dscale 2, `1` dscale
// 0), on the single-process door and the coordinator (DAG) door alike. The
// controls: a NUMERIC(10,2) column keeps its typmod and its two digits; a
// column a catalog record declares DECIMAL(38,0) with no marker (what the
// 8e681724 door wrote for `NUMERIC`) keeps typmod (38,0) and stores 1 for
// 1.25; an expression over the column prints at its one declared scale
// (catalog numeric-decimal r18). At 8e681724 `v` was DECIMAL(38,0) on the
// wire (typmod 2490372) and 1.25 read back as 1.
func TestArcUNUnconstrainedColumnOnTheWire(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: a coordinator door with three workers")
	}
	ctx := context.Background()
	singleAddr, dagAddr, coord, db := owaDoorsOver(t, ctx, func(t *testing.T, ctx context.Context, db *wadjet.DB) {
		t.Helper()
		for _, q := range []string{
			"CREATE TABLE un_w (id BIGINT, v NUMERIC, n NUMERIC(10,2))",
			"INSERT INTO un_w VALUES (1, 1.25, 1.25), (2, 0.755, 2.50), (3, 1, 1), (4, NULL, NULL), (5, 1234567890, 10)",
			"CREATE TABLE un_c AS SELECT id, CAST(id AS NUMERIC) AS c FROM un_w",
		} {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		legacy := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64, Nullable: true},
			{Name: "l", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		}}
		if err := db.CreateTable(ctx, "un_legacy", legacy, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Query(ctx, "INSERT INTO un_legacy VALUES (1, 1.25), (2, 7)"); err != nil {
			t.Fatal(err)
		}
	})
	type cell struct {
		name, sql string
		oid       uint32
		typmod    int32
		text      string // rows `;`-joined, NULL spelled out
		dscale    string // the binary format's dscale per row, `;`-joined ("" = not numeric)
	}
	cells := []cell{
		{"bare", "SELECT v FROM un_w ORDER BY id", 1700, -1, "1.25;0.755;1;NULL;1234567890", "2;3;0;NULL;0"},
		{"ctas_cast", "SELECT c FROM un_c ORDER BY id", 1700, -1, "1;2;3;4;5", "0;0;0;0;0"},
		{"derived", "SELECT x.v FROM (SELECT id, v FROM un_w) x ORDER BY x.id", 1700, -1, "1.25;0.755;1;NULL;1234567890", "2;3;0;NULL;0"},
		{"cast_text", "SELECT CAST(v AS TEXT) FROM un_w ORDER BY id", 25, -1, "1.25;0.755;1;NULL;1234567890", ""},
		// Every TEXT rendering of the column's value is the one printer's
		// (batch.Vector.GetValueOf): the operators read the same box.
		{"cat", "SELECT v || '' FROM un_w ORDER BY id", 25, -1, "1.25;0.755;1;NULL;1234567890", ""},
		{"concat", "SELECT concat(v, '|') FROM un_w ORDER BY id", 25, -1, "1.25|;0.755|;1|;|;1234567890|", ""},
		{"format", "SELECT format('%s', v) FROM un_w ORDER BY id", 25, -1, "1.25;0.755;1;;1234567890", ""},
		// Read back across an exchange on the coordinator door (a GROUP BY):
		// the producing stage's marks ride the file (ADR-0024 §10).
		{"gk_text", "SELECT CAST(v AS TEXT) FROM (SELECT v FROM un_w GROUP BY v) q ORDER BY q.v", 25, -1, "0.755;1;1.25;1234567890;NULL", ""},
		{"gk_count", "SELECT COUNT(*) FROM (SELECT v FROM un_w GROUP BY v) q WHERE CAST(v AS TEXT) = '1'", 20, -1, "1", ""},
		{"info_schema", "SELECT numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'un_w' AND column_name = 'v'", 0, 0, "NULL|NULL", ""},
		// The controls.
		{"constrained", "SELECT n FROM un_w ORDER BY id", 1700, (10<<16 | 2) + 4, "1.25;2.50;1.00;NULL;10.00", "2;2;2;NULL;2"},
		{"legacy_record", "SELECT l FROM un_legacy ORDER BY id", 1700, (38 << 16) + 4, "1;7", "0;0"},
		{"expression_r18", "SELECT v + 1 FROM un_w ORDER BY id", 1700, -1,
			"2.2500000000;1.7550000000;2.0000000000;NULL;1234567891.0000000000", "10;10;10;NULL;10"},
	}
	// The HTTP door over the coordinator: its JSON rows are the coordinator
	// result boxed (SQLResult.Rows), the printer the gRPC stream shares.
	hs := httptest.NewServer(New(Config{Addr: ":0", Catalog: db.Catalog(), Coordinator: coord}, nil).Mux())
	t.Cleanup(hs.Close)
	for _, c := range []struct{ sql, key, want string }{
		{"SELECT v FROM un_w ORDER BY id", "v", "1.25;0.755;1;NULL;1234567890"},
		{"SELECT c FROM un_c ORDER BY id", "c", "1;2;3;4;5"},
		{"SELECT n FROM un_w ORDER BY id", "n", "1.25;2.50;1.00;NULL;10.00"},
		{"SELECT l FROM un_legacy ORDER BY id", "l", "1;7"},
		{"SELECT CAST(v AS TEXT) AS t FROM (SELECT v FROM un_w GROUP BY v) q ORDER BY q.v", "t", "0.755;1;1.25;1234567890;NULL"},
	} {
		t.Run("http/"+c.key, func(t *testing.T) {
			if got := unHTTPRows(t, hs.URL, c.sql, c.key); got != c.want {
				t.Errorf("%s over HTTP\n  got  %s\n  want %s", c.sql, got, c.want)
			}
		})
	}
	// The gRPC door, over the coordinator (the stream boxes the result
	// batches itself) and over the embedded engine: the same text.
	for _, door := range []struct {
		name string
		cfg  GRPCConfig
	}{{"grpc-coordinator", GRPCConfig{Coord: coord}}, {"grpc-embedded", GRPCConfig{DB: db}}} {
		g := NewGRPCServer(door.cfg, slog.Default())
		for _, c := range []struct{ sql, key, want string }{
			{"SELECT v FROM un_w ORDER BY id", "v", "1.25;0.755;1;NULL;1234567890"},
			{"SELECT x.v FROM (SELECT id, v FROM un_w) x ORDER BY x.id", "v", "1.25;0.755;1;NULL;1234567890"},
			{"SELECT v || '' AS t FROM un_w ORDER BY id", "t", "1.25;0.755;1;NULL;1234567890"},
			{"SELECT c FROM un_c ORDER BY id", "c", "1;2;3;4;5"},
			{"SELECT n FROM un_w ORDER BY id", "n", "1.25;2.50;1.00;NULL;10.00"},
			{"SELECT l FROM un_legacy ORDER BY id", "l", "1;7"},
			{"SELECT CAST(v AS TEXT) AS g FROM (SELECT v FROM un_w GROUP BY v) q ORDER BY q.v", "g", "0.755;1;1.25;1234567890;NULL"},
		} {
			t.Run(door.name+"/"+c.key+"/"+c.sql, func(t *testing.T) {
				fs := &dupNameStream{}
				if err := g.QueryStream(&wadjetv1.QueryRequest{Sql: c.sql}, fs); err != nil {
					t.Fatalf("QueryStream(%s): %v", c.sql, err)
				}
				var cells []string
				for _, r := range fs.sent {
					for _, row := range r.Rows {
						v := row.Fields[c.key]
						if _, null := v.GetKind().(*structpb.Value_NullValue); v == nil || null {
							cells = append(cells, "NULL")
							continue
						}
						cells = append(cells, fmt.Sprint(v.AsInterface()))
					}
				}
				if got := strings.Join(cells, ";"); got != c.want {
					t.Errorf("%s over gRPC\n  got  %s\n  want %s", c.sql, got, c.want)
				}
			})
		}
	}
	for _, door := range []struct{ name, addr string }{{"single", singleAddr}, {"dag", dagAddr}} {
		conn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", door.addr))
		if err != nil {
			t.Fatalf("%s: connect: %v", door.name, err)
		}
		t.Cleanup(func() { conn.Close(context.Background()) })
		for _, c := range cells {
			t.Run(door.name+"/"+c.name, func(t *testing.T) {
				text := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}).Read()
				if text.Err != nil {
					t.Fatalf("%s: %v", c.sql, text.Err)
				}
				if c.oid != 0 {
					f := text.FieldDescriptions[0]
					if f.DataTypeOID != c.oid || f.TypeModifier != c.typmod {
						t.Errorf("%s declares OID %d typmod %d, want %d / %d", c.sql, f.DataTypeOID, f.TypeModifier, c.oid, c.typmod)
					}
				}
				if got := unWireRows(text.Rows); got != c.text {
					t.Errorf("%s text\n  got  %s\n  want %s", c.sql, got, c.text)
				}
				if c.dscale == "" {
					return
				}
				bin := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{1}).Read()
				if bin.Err != nil {
					t.Fatalf("%s (binary): %v", c.sql, bin.Err)
				}
				var ds []string
				for _, r := range bin.Rows {
					if r[0] == nil {
						ds = append(ds, "NULL")
						continue
					}
					if len(r[0]) < 8 {
						t.Fatalf("%s: a binary numeric of %d bytes", c.sql, len(r[0]))
					}
					ds = append(ds, fmt.Sprint(binary.BigEndian.Uint16(r[0][6:8])))
				}
				if got := strings.Join(ds, ";"); got != c.dscale {
					t.Errorf("%s binary dscale\n  got  %s\n  want %s", c.sql, got, c.dscale)
				}
			})
		}
	}
}

// unHTTPRows is the HTTP door's JSON rows for a one-column statement, in the
// form unWireRows gives the wire's.
func unHTTPRows(t *testing.T, baseURL, sql, key string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+"/v1/queries", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("%s over HTTP: %v", sql, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s over HTTP: status %d", sql, resp.StatusCode)
	}
	var out struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	cells := make([]string, 0, len(out.Rows))
	for _, r := range out.Rows {
		if v := r[key]; v == nil {
			cells = append(cells, "NULL")
		} else {
			cells = append(cells, fmt.Sprint(v))
		}
	}
	return strings.Join(cells, ";")
}

func unWireRows(rows [][][]byte) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, v := range r {
			if v == nil {
				cells[i] = "NULL"
			} else {
				cells[i] = string(v)
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	return strings.Join(out, ";")
}
