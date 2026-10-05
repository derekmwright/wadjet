// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/structpb"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE UNCONSTRAINED MARK IS THE SAME ON EVERY DOOR (ADR-0024 §10; ADR-0010's
// 2026-10-05 amendment). The coordinator doors print what their result
// batches' marks say, and those marks arrive in the `.wshf` headers by
// position. At 8a093e84 the marks rode beside the files, keyed by name: a
// UNION ALL of a NUMERIC column and a NUMERIC(10,2) column of the same name
// printed the constrained arm's values trimmed on pgwire-coordinator, HTTP
// and gRPC-coordinator (d04, d05, d11, d12), and the asynchronous HTTP door,
// whose result no stamp reached, printed a marked column's stored scale
// (d01-d03, d09, d10). Each statement's answer is pinned once (PostgreSQL
// 17.11's, or the kept r18 answer of a set operation over two
// declarations) and every door must print it: pgwire single-process and
// coordinator, HTTP, HTTP async, gRPC coordinator and embedded. Rows are
// rendered by column name (every statement aliases its columns), values as
// the door's text, sorted.
func TestArcUNInBandMarkEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six doors over a coordinator with three workers")
	}
	ctx := context.Background()
	singleAddr, dagAddr, coord, db := owaDoorsOver(t, ctx, func(t *testing.T, ctx context.Context, db *wadjet.DB) {
		t.Helper()
		for _, q := range []string{
			"CREATE TABLE un_t (id BIGINT, g BIGINT, b BIGINT, v NUMERIC, w NUMERIC, n NUMERIC(10,2))",
			"INSERT INTO un_t VALUES (1,1,10,1.25,2,1.25),(2,1,20,0.755,0.5,2.50),(3,1,NULL,1,NULL,NULL),(4,2,40,NULL,3.3333333333,3.33),(5,2,1234567890,1234567890,10,10.00),(6,3,60,2.5,1.5,0.75)",
			"CREATE TABLE un_u (id BIGINT, v NUMERIC)",
			"INSERT INTO un_u VALUES (1,1.25),(2,7),(7,0.1)",
			"CREATE TABLE un_x (id BIGINT, g BIGINT, v NUMERIC)",
			"INSERT INTO un_x VALUES (1,1,1),(2,1,1.5),(3,2,7),(4,2,0.0000000001),(5,3,NULL),(6,3,1)",
			"CREATE TABLE rv_n (id BIGINT, g BIGINT, v NUMERIC(10,2))",
			"INSERT INTO rv_n VALUES (1,1,1.00),(2,1,1.50),(3,2,7.00),(4,2,0.00),(5,3,NULL),(6,3,2.50)",
		} {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	})
	hs := httptest.NewServer(New(Config{Addr: ":0", Catalog: db.Catalog(), Coordinator: coord}, nil).Mux())
	t.Cleanup(hs.Close)
	conns := map[string]*pgconn.PgConn{}
	for _, d := range []struct{ name, addr string }{{"pg-single", singleAddr}, {"pg-coordinator", dagAddr}} {
		c, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", d.addr))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		conns[d.name] = c
	}
	grpcs := map[string]*GRPCServer{
		"grpc-coordinator": NewGRPCServer(GRPCConfig{Coord: coord}, slog.Default()),
		"grpc-embedded":    NewGRPCServer(GRPCConfig{DB: db}, slog.Default()),
	}
	doors := []string{"pg-single", "pg-coordinator", "http", "http-async", "grpc-coordinator", "grpc-embedded"}
	answer := func(door, sql string) (string, error) {
		switch door {
		case "pg-single", "pg-coordinator":
			r := conns[door].ExecParams(ctx, sql, nil, nil, nil, []int16{0}).Read()
			if r.Err != nil {
				return "", r.Err
			}
			var rows []map[string]string
			for _, row := range r.Rows {
				m := map[string]string{}
				for i, v := range row {
					if v == nil {
						m[r.FieldDescriptions[i].Name] = "NULL"
					} else {
						m[r.FieldDescriptions[i].Name] = string(v)
					}
				}
				rows = append(rows, m)
			}
			return unibDoorRows(rows), nil
		case "http", "http-async":
			return unibHTTP(t, hs.URL, sql, door == "http-async")
		default:
			fs := &dupNameStream{}
			if err := grpcs[door].QueryStream(&wadjetv1.QueryRequest{Sql: sql}, fs); err != nil {
				return "", err
			}
			var rows []map[string]string
			for _, r := range fs.sent {
				for _, row := range r.Rows {
					m := map[string]string{}
					for k, v := range row.Fields {
						m[k] = unibStructText(v)
					}
					rows = append(rows, m)
				}
			}
			return unibDoorRows(rows), nil
		}
	}
	var gen *bufio.Writer
	if p := os.Getenv("UN_INBAND_DOORS_GEN"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		gen = bufio.NewWriter(f)
		defer gen.Flush()
	}
	asserted := 0
	for _, c := range unibDoorCells {
		t.Run(c.name, func(t *testing.T) {
			for _, door := range doors {
				got, err := answer(door, c.sql)
				if err != nil {
					got = "ERR " + strings.SplitN(err.Error(), "\n", 2)[0]
				}
				if gen != nil {
					fmt.Fprintf(gen, "%s\t%s\t%s\n", c.name, door, got)
				}
				asserted++
				if got != c.want {
					t.Errorf("%s\n  door %s\n  got  %s\n  want %s", c.sql, door, got, c.want)
				}
			}
		})
	}
	if asserted != len(unibDoorCells)*len(doors) {
		t.Fatalf("%d (statement, door) answers asserted", asserted)
	}
}

// unibDoorCells: want is PostgreSQL 17.11's rows unless why names the kept
// class.
var unibDoorCells = []struct{ name, sql, want, why string }{
	{name: "d01", sql: "SELECT v AS x FROM un_t WHERE id = 3", want: "rows=1 x=1"},
	{name: "d02", sql: "SELECT id AS i, v AS x FROM un_t", want: "rows=6 i=1,x=1.25 | i=2,x=0.755 | i=3,x=1 | i=4,x=NULL | i=5,x=1234567890 | i=6,x=2.5"},
	{name: "d03", sql: "SELECT v AS x FROM (SELECT v FROM un_x GROUP BY v) q", want: "rows=5 x=0.0000000001 | x=1 | x=1.5 | x=7 | x=NULL"},
	{name: "d04", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT v FROM rv_n) s", want: "rows=12 x=0.0000000000 | x=0.0000000001 | x=1.0000000000 | x=1.0000000000 | x=1.0000000000 | x=1.5000000000 | x=1.5000000000 | x=2.5000000000 | x=7.0000000000 | x=7.0000000000 | x=NULL | x=NULL", why: unibDoorR18},
	{name: "d05", sql: "SELECT count(*) AS c FROM (SELECT v FROM un_x UNION ALL SELECT v FROM rv_n) s WHERE CAST(v AS TEXT) = '1'", want: "rows=1 c=0", why: unibDoorR18},
	{name: "d06", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT v FROM un_u) s", want: "rows=9 x=0.0000000001 | x=0.1 | x=1 | x=1 | x=1.25 | x=1.5 | x=7 | x=7 | x=NULL"},
	{name: "d07", sql: "SELECT CAST(a.v AS TEXT) AS x, CAST(b.v AS TEXT) AS y FROM un_x a JOIN rv_n b ON a.id = b.id", want: "rows=6 x=0.0000000001,y=0.00 | x=1,y=1.00 | x=1,y=2.50 | x=1.5,y=1.50 | x=7,y=7.00 | x=NULL,y=NULL"},
	{name: "d08", sql: "SELECT CAST(q.v AS TEXT) AS x, CAST(q.n AS TEXT) AS y FROM (SELECT v AS n, n AS v FROM un_t GROUP BY v, n) q", want: "rows=6 x=0.75,y=2.5 | x=1.25,y=1.25 | x=10.00,y=1234567890 | x=2.50,y=0.755 | x=3.33,y=NULL | x=NULL,y=1"},
	{name: "d09", sql: "SELECT q.v AS x, r.v AS y FROM (SELECT v FROM un_x GROUP BY v) q JOIN rv_n r ON q.v = r.v", want: "rows=3 x=1,y=1.00 | x=1.5,y=1.50 | x=7,y=7.00"},
	{name: "d10", sql: "WITH c AS (SELECT v FROM un_x GROUP BY v) SELECT CAST(c1.v AS TEXT) AS x, c2.v AS y FROM c c1 JOIN c c2 ON c1.v = c2.v", want: "rows=4 x=0.0000000001,y=0.0000000001 | x=1,y=1 | x=1.5,y=1.5 | x=7,y=7"},
	{name: "d11", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x INTERSECT SELECT v FROM rv_n) s", want: "rows=4 x=1.0000000000 | x=1.5000000000 | x=7.0000000000 | x=NULL", why: unibDoorR18},
	{name: "d12", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM rv_n UNION ALL SELECT v FROM un_x) s WHERE v = 1.5", want: "rows=2 x=1.5000000000 | x=1.5000000000", why: unibDoorR18},
	{name: "d13", sql: "SELECT v AS x FROM (SELECT v FROM un_x UNION ALL SELECT v FROM rv_n) s WHERE v = 1.5", want: "rows=2 x=1.5000000000 | x=1.5000000000", why: unibDoorR18},
	{name: "d14", sql: "SELECT count(*) AS c FROM (SELECT v FROM un_x GROUP BY v) q WHERE CAST(v AS TEXT) = '1'", want: "rows=1 c=1"},
}

// unibDoorR18: a set operation over a NUMERIC(10,2) arm and a NUMERIC arm
// is the union's DECIMAL(38,10), not marked, and prints that one scale on
// every door (numeric-decimal r18); PostgreSQL prints each value at its own
// arm's scale (d04: 2.50 and 2.5).
const unibDoorR18 = "r18"

func unibDoorRows(rows []map[string]string) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		ks := make([]string, 0, len(r))
		for k := range r {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		cs := make([]string, len(ks))
		for i, k := range ks {
			cs[i] = k + "=" + r[k]
		}
		out = append(out, strings.Join(cs, ","))
	}
	sort.Strings(out)
	return fmt.Sprintf("rows=%d %s", len(out), strings.Join(out, " | "))
}

func unibStructText(v *structpb.Value) string {
	switch k := v.GetKind().(type) {
	case nil, *structpb.Value_NullValue:
		return "NULL"
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_NumberValue:
		return strconv.FormatFloat(k.NumberValue, 'f', -1, 64)
	case *structpb.Value_BoolValue:
		return strconv.FormatBool(k.BoolValue)
	default:
		return fmt.Sprint(v.AsInterface())
	}
}

func unibJSONText(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return x
	case json.Number:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

func unibHTTP(t *testing.T, base, sql string, async bool) (string, error) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	decode := func(r *http.Response, into any) error {
		defer r.Body.Close()
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		return d.Decode(into)
	}
	var out struct {
		Rows  []map[string]any `json:"rows"`
		Error string           `json:"error"`
	}
	if !async {
		resp, err := http.Post(base+"/v1/queries", "application/json", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		if err := decode(resp, &out); err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("status %d: %s", resp.StatusCode, out.Error)
		}
	} else {
		resp, err := http.Post(base+"/v1/queries/async", "application/json", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		var sub struct {
			QueryID string `json:"query_id"`
			Error   string `json:"error"`
		}
		if err := decode(resp, &sub); err != nil || sub.QueryID == "" {
			return "", fmt.Errorf("submit: status %d %v %s", resp.StatusCode, err, sub.Error)
		}
		deadline := time.Now().Add(60 * time.Second)
		for {
			r2, err := http.Get(base + "/v1/queries/" + sub.QueryID + "/results")
			if err != nil {
				return "", err
			}
			out.Rows, out.Error = nil, ""
			if err := decode(r2, &out); err != nil {
				return "", err
			}
			if r2.StatusCode == http.StatusOK && !strings.Contains(out.Error, "not completed") {
				break
			}
			if time.Now().After(deadline) {
				return "", fmt.Errorf("async result: status %d %s", r2.StatusCode, out.Error)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if out.Error != "" {
			return "", fmt.Errorf("%s", out.Error)
		}
	}
	rows := make([]map[string]string, 0, len(out.Rows))
	for _, r := range out.Rows {
		m := map[string]string{}
		for k, v := range r {
			m[k] = unibJSONText(v)
		}
		rows = append(rows, m)
	}
	return unibDoorRows(rows), nil
}
