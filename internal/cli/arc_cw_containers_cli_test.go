// SPDX-License-Identifier: MIT

package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Arc CW's CLI door: a container prints as PostgreSQL's text output in the
// table and CSV forms — `{1,2,3}`, `{"a b",c}`, `(1,q)`, a timestamp element
// as its text — which is what psql prints for the same query over this
// engine's pgwire server, and a JSON array / object in the JSON form, whose
// timestamp leaves are the text a top-level timestamp prints as.
//
// At base 83cd4a93 the constructor, tcp_flags and the element read printed
// Go's rendering (`[1 2 3]`, `[SYN ACK]`, `[1718454645500 <nil>]`,
// `1718454645500`) in every form, and a STORED container printed `[1 2 3]` /
// `map[x:1 y:q]` in the table and CSV forms while psql printed `{1,2,3}` /
// `(1,q)`.
func TestArcCWContainersRenderOnTheCLI(t *testing.T) {
	storage := newCLIStorageArgs(t)
	dir := t.TempDir()
	js := filepath.Join(dir, "arr.json")
	if err := os.WriteFile(js, []byte(`{"id":1,"a":[1,2,3],"s":["a b","c"],"r":{"x":1,"y":"q"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, sql string
		table     []string // lines the table form must contain
		csv, json string
	}{
		{
			name: "constructed",
			sql: `SELECT ARRAY[1,2,3] AS v, ARRAY[TIMESTAMP '2024-06-15 12:30:45.5', NULL] AS t, ` +
				`tcp_flags(18) AS f, (ARRAY[TIMESTAMP '2024-06-15 12:30:45.5'])[1] AS e`,
			table: []string{`| {1,2,3} | {"2024-06-15 12:30:45.5",NULL} | {SYN,ACK} | 2024-06-15 12:30:45.5 |`},
			csv:   "v,t,f,e\n\"{1,2,3}\",\"{\"\"2024-06-15 12:30:45.5\"\",NULL}\",\"{SYN,ACK}\",2024-06-15 12:30:45.5\n",
			json: "[\n  {\n    \"v\": [\n      1,\n      2,\n      3\n    ],\n" +
				"    \"t\": [\n      \"2024-06-15 12:30:45.5\",\n      null\n    ],\n" +
				"    \"f\": [\n      \"SYN\",\n      \"ACK\"\n    ],\n" +
				"    \"e\": \"2024-06-15 12:30:45.5\"\n  }\n]\n",
		},
		{
			name:  "stored",
			sql:   "SELECT a, s, r FROM read_json('" + js + "')",
			table: []string{`| {1,2,3} | {"a b",c} | (1,q) |`},
			csv:   "a,s,r\n\"{1,2,3}\",\"{\"\"a b\"\",c}\",\"(1,q)\"\n",
			json: "[\n  {\n    \"a\": [\n      1,\n      2,\n      3\n    ],\n" +
				"    \"s\": [\n      \"a b\",\n      \"c\"\n    ],\n" +
				"    \"r\": {\n      \"x\": 1,\n      \"y\": \"q\"\n    }\n  }\n]\n",
		},
		{
			name:  "through a derived table",
			sql:   `SELECT v, v[1] AS e FROM (SELECT ARRAY[TIMESTAMP '1999-12-31 23:59:59'] AS v) s`,
			table: []string{`| {"1999-12-31 23:59:59"} | 1999-12-31 23:59:59 |`},
			csv:   "v,e\n\"{\"\"1999-12-31 23:59:59\"\"}\",1999-12-31 23:59:59\n",
			json: "[\n  {\n    \"v\": [\n      \"1999-12-31 23:59:59\"\n    ],\n" +
				"    \"e\": \"1999-12-31 23:59:59\"\n  }\n]\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runCLI(t, append(storage, "query", "--format=table", tc.sql)...)
			for _, line := range tc.table {
				if !strings.Contains(got, line) {
					t.Errorf("--format=table %s\n got:\n%s\nwant a line containing %s", tc.sql, got, line)
				}
			}
			if got := runCLI(t, append(storage, "query", "--format=csv", tc.sql)...); got != tc.csv {
				t.Errorf("--format=csv %s\n got:\n%s\nwant:\n%s", tc.sql, got, tc.csv)
			}
			if got := runCLI(t, append(storage, "query", "--format=json", tc.sql)...); got != tc.json {
				t.Errorf("--format=json %s\n got:\n%s\nwant:\n%s", tc.sql, got, tc.json)
			}
		})
	}
}
