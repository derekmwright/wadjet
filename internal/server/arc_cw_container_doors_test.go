// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/oracle/cwfixture"
)

// Arc CW on the doors a DEPLOYMENT has beyond the embedded engine: pgwire
// over the single-process engine and over the coordinator (the DAG), the HTTP
// query door local and distributed, and the async door. Each carries what it
// carried for a STORED array — a declared array OID and PostgreSQL's `{…}`
// text on pgwire, a JSON array of the engine's values on HTTP/async — for a
// constructed, function-returned, derived, VALUES and zero-row container too.
//
// At base 83cd4a93 every non-stored cell below went out as TEXT: OID 25 with
// Go's rendering on pgwire, and the JSON STRING "[1 2 3]" on HTTP/async.
type cwDoorCell struct {
	name string
	sql  string
	oids string // pgwire RowDescription
	text string // pgwire text rows
	json string // HTTP/async: each row's values in column order, compact JSON
	// asyncRefuses is the refusal the async door gives a statement it has no
	// distributed stage for (a VALUES list is a table-less relation there — a
	// refusal of the door, the same with no container in the query).
	asyncRefuses string
}

var cwDoorCells = []cwDoorCell{
	{"constructor", `SELECT ARRAY[1,2,3] AS v FROM cw WHERE id = 1`, "1007", "{1,2,3}", `[[1,2,3]]`, ""},
	{"tcp_flags", `SELECT tcp_flags(18) AS v FROM cw WHERE id = 1`, "1009", "{SYN,ACK}", `[["SYN","ACK"]]`, ""},
	{"derived-subscript-any", `SELECT a[1] AS e, 2 = ANY(a) AS m FROM (SELECT ARRAY[1,2] AS a FROM cw WHERE id = 1) s`,
		"23,16", "1|t", `[1,true]`, ""},
	{"stored-timestamps", `SELECT ats FROM cw WHERE id = 1`, "1115",
		`{"2024-06-15 12:30:45.5",NULL}`, fmt.Sprintf(`[[%d,null]]`, cwfixture.TS1), ""},
	{"map_keys", `SELECT map_keys(m) AS k FROM cw WHERE id = 1`, "1009", "{a,b}", `[["a","b"]]`, ""},
	{"values-timestamps", `SELECT v FROM cw, (VALUES (ARRAY[TIMESTAMP '2024-06-15 12:30:45.5'])) t(v) WHERE id = 1`,
		"1115", `{"2024-06-15 12:30:45.5"}`, fmt.Sprintf(`[[%d]]`, cwfixture.TS1), "table-less SELECT has no distributed stage"},
	{"union-ordered", `SELECT ai AS v FROM cw WHERE id = 3 UNION ALL SELECT ARRAY[CAST(id AS INT) * 50] FROM cw WHERE id < 3 ORDER BY v`,
		// int4[] ∪ bigint[] is bigint[] on every arm: `CAST(id AS INT)` is
		// bigint here (ADR-0012 item 12), so the second arm's element is.
		"1016", "{50} || {100} || {NULL,5}", `[[50]] || [[100]] || [[null,5]]`, ""},
	{"zero-row", `SELECT ARRAY[DATE '2024-01-02'] AS v FROM cw WHERE false`, "1182", "", "", ""},
}

func TestArcCWContainersOnEveryDeploymentDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, pmProvider(t),
		pmExtraTable{cwfixture.Table, cwfixture.Schema(), cwfixture.Rows()})
	const key = "admin-key"

	pgText := func(addr, sql string) (string, string, error) {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:%s@%s/wadjet?sslmode=disable", key, addr))
		if err != nil {
			return "", "", err
		}
		defer conn.Close(ctx)
		mrr := conn.PgConn().Exec(ctx, sql)
		var oids, rows []string
		for mrr.NextResult() {
			rr := mrr.ResultReader()
			for _, fd := range rr.FieldDescriptions() {
				oids = append(oids, fmt.Sprint(fd.DataTypeOID))
			}
			for rr.NextRow() {
				var vals []string
				for _, v := range rr.Values() {
					if v == nil {
						vals = append(vals, "NULL")
						continue
					}
					vals = append(vals, string(v))
				}
				rows = append(rows, strings.Join(vals, "|"))
			}
			if _, err := rr.Close(); err != nil {
				return "", "", err
			}
		}
		if err := mrr.Close(); err != nil {
			return "", "", err
		}
		return strings.Join(oids, ","), strings.Join(rows, " || "), nil
	}
	// The HTTP body's rows are objects keyed by column; the cell reads them
	// back in the column order the body lists.
	renderJSON := func(raw []byte) (string, error) {
		var body struct {
			Columns []string                     `json:"columns"`
			Rows    []map[string]json.RawMessage `json:"rows"`
			Error   string                       `json:"error"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return "", fmt.Errorf("unparseable body %s", raw)
		}
		if body.Error != "" {
			return "", fmt.Errorf("%s", body.Error)
		}
		var rows []string
		for _, r := range body.Rows {
			var vals []string
			for _, c := range body.Columns {
				vals = append(vals, string(r[c]))
			}
			rows = append(rows, "["+strings.Join(vals, ",")+"]")
		}
		return strings.Join(rows, " || "), nil
	}
	do := func(method, url string, payload any) ([]byte, int, error) {
		var rd io.Reader
		if payload != nil {
			b, _ := json.Marshal(payload)
			rd = bytes.NewReader(b)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, rd)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, 0, err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		return raw, resp.StatusCode, err
	}
	httpQuery := func(base, sql string) (string, error) {
		raw, _, err := do(http.MethodPost, base+"/v1/queries", map[string]string{"sql": sql})
		if err != nil {
			return "", err
		}
		return renderJSON(raw)
	}
	asyncQuery := func(sql string) (string, error) {
		raw, _, err := do(http.MethodPost, rig.asyncBase+"/v1/queries/async", map[string]string{"sql": sql})
		if err != nil {
			return "", err
		}
		var started struct {
			QueryID string `json:"query_id"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal(raw, &started); err != nil || started.QueryID == "" {
			return "", fmt.Errorf("async start: %s", raw)
		}
		deadline := time.Now().Add(60 * time.Second)
		for {
			raw, _, err := do(http.MethodGet, rig.asyncBase+"/v1/queries/"+started.QueryID, nil)
			if err != nil {
				return "", err
			}
			var st struct {
				State string `json:"state"`
				Error string `json:"error"`
			}
			_ = json.Unmarshal(raw, &st)
			if st.State == "completed" {
				break
			}
			if st.Error != "" || st.State == "failed" || time.Now().After(deadline) {
				return "", fmt.Errorf("async status: %s", raw)
			}
			time.Sleep(50 * time.Millisecond)
		}
		raw, _, err = do(http.MethodGet, rig.asyncBase+"/v1/queries/"+started.QueryID+"/results", nil)
		if err != nil {
			return "", err
		}
		return renderJSON(raw)
	}

	answered := 0
	for _, c := range cwDoorCells {
		for _, d := range []struct{ name, addr string }{{"pgwire/single", rig.pgSingle}, {"pgwire/dag", rig.pgDAG}} {
			oids, text, err := pgText(d.addr, c.sql)
			if err != nil {
				t.Errorf("%s / %s: %v", c.name, d.name, err)
				continue
			}
			answered++
			if oids != c.oids || text != c.text {
				t.Errorf("%s / %s: %s\n  got  OIDs %s  %s\n  want OIDs %s  %s", c.name, d.name, c.sql, oids, text, c.oids, c.text)
			}
		}
		for _, d := range []struct {
			name string
			run  func(string) (string, error)
		}{
			{"http/local", func(sql string) (string, error) { return httpQuery(rig.httpLocal, sql) }},
			{"http/dag", func(sql string) (string, error) { return httpQuery(rig.asyncBase, sql) }},
			{"async", asyncQuery},
		} {
			got, err := d.run(c.sql)
			if d.name == "async" && c.asyncRefuses != "" {
				if err == nil || !strings.Contains(err.Error(), c.asyncRefuses) {
					t.Errorf("%s / async: want the refusal %q, got %q %v", c.name, c.asyncRefuses, got, err)
				}
				answered++
				continue
			}
			if err != nil {
				t.Errorf("%s / %s: %v", c.name, d.name, err)
				continue
			}
			answered++
			if got != c.json {
				t.Errorf("%s / %s: %s\n  got  %s\n  want %s", c.name, d.name, c.sql, got, c.json)
			}
		}
	}
	if want := len(cwDoorCells) * 5; answered != want {
		t.Errorf("%d of %d (cell, door) pairs answered", answered, want)
	}
}
