// SPDX-License-Identifier: MIT

package pgwire

// A SELECT ITEM THAT IS A GROUP BY KEY, SPELLED WITH OR WITHOUT THE RELATION'S
// QUALIFIER, ON THE WIRE (#1524): the declared OID of the RowDescription and
// the DataRow bytes, over the simple protocol, the extended protocol (text
// results) and the extended protocol with binary results, for every key type
// of coordinator.TestArcGKGroupKeySpellingEveryArm's table and the consumers
// a client sees — the issue's own three statements, ORDER BY, a window, a
// HAVING, a DISTINCT, a derived table, and the column a CREATE TABLE … AS and
// an INSERT … SELECT store. At 33e2fb92 every item spelled apart from its key
// was declared OID 25 (text) and `ORDER BY 1` sorted `20.00` before `4.50`.
//
// PostgreSQL answers are measured over the same fixture
// (setupSSAuditWireDB) with the same client (pgx; default_query_exec_mode
// simple_protocol / exec / describe_exec) — testdata/arc_gk_group_key_spelling_wire_pg17.tsv:
// name, mode, ordered, sql, answer; a cell's statements are joined by " ;; "
// and the answer is the last one's; kept rows record catalogued differences.
// store/ctas/{s,e,b} pins numeric-decimal r24: the created column is now
// numeric, as in PostgreSQL (base: text), with trailing zeros trimmed.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// gkWireCells is the cell list the measurement ran (GKW_GEN=<path> dumps it).
func gkWireCells() [][3]string {
	cells := [][3]string{
		{"issue/qualKey", "true", "SELECT t.i + 1, COUNT(*) FROM ss_t t GROUP BY i + 1 ORDER BY 1"},
		{"issue/nKeyMul", "true", "SELECT 2 * t.n, COUNT(*) FROM ss_t t GROUP BY 2 * n ORDER BY 1"},
		{"issue/absKey", "true", "SELECT ABS(-1) * t.n, COUNT(*) FROM ss_t t GROUP BY ABS(-1) * n ORDER BY 1"},
		{"keyQual/num", "true", "SELECT 2 * n AS k, count(*) AS c FROM ss_t t GROUP BY 2 * t.n ORDER BY 1"},
		{"keyQual/int", "true", "SELECT i + 1 AS k, count(*) AS c FROM ss_t t GROUP BY t.i + 1 ORDER BY 1"},
		{"consumer/window", "true", "SELECT 2 * t.n AS k, rank() OVER (ORDER BY 2 * t.n) AS r FROM ss_t t GROUP BY 2 * n ORDER BY 1"},
		{"consumer/having", "true", "SELECT 2 * t.n AS k FROM ss_t t GROUP BY 2 * n HAVING 2 * t.n > 0 ORDER BY 1"},
		{"consumer/distinct", "true", "SELECT DISTINCT 2 * t.n AS k FROM ss_t t GROUP BY 2 * n ORDER BY 1"},
		{"consumer/derived", "true", "SELECT s.k FROM (SELECT 2 * t.n AS k FROM ss_t t GROUP BY 2 * n) s ORDER BY 1"},
		{"consumer/larger", "true", "SELECT (2 * t.n) * 2 AS k FROM ss_t t GROUP BY 2 * n ORDER BY 1"},
		{"store/ctas", "true", "DROP TABLE IF EXISTS gk_w ;; CREATE TABLE gk_w AS SELECT 2 * t.n AS k FROM ss_t t GROUP BY 2 * n ;; SELECT k FROM gk_w ORDER BY k"},
		{"store/ctasKeyQual", "true", "DROP TABLE IF EXISTS gk_wq ;; CREATE TABLE gk_wq AS SELECT i + 1 AS k FROM ss_t t GROUP BY t.i + 1 ;; SELECT k FROM gk_wq ORDER BY k"},
		{"store/insert", "true", "DROP TABLE IF EXISTS gk_wi ;; CREATE TABLE gk_wi (k NUMERIC(12,2)) ;; INSERT INTO gk_wi SELECT 2 * t.n FROM ss_t t GROUP BY 2 * n ;; SELECT k FROM gk_wi ORDER BY k"},
	}
	// Controls with no GROUP BY: what the same expression sends un-grouped,
	// for the two kept classes below.
	cells = append(cells,
		[3]string{"ctl/intArithNoGroup", "true", "SELECT t.i + 1 AS k FROM ss_t t ORDER BY t.id"},
		[3]string{"ctl/dateNoGroup", "true", "SELECT t.d + 1 AS k FROM ss_t t ORDER BY t.id"},
		[3]string{"ctl/dateColumn", "true", "SELECT t.d AS k FROM ss_t t ORDER BY t.id"})
	for _, ty := range [][3]string{
		{"bigint", "t.b + 1", "b + 1"},
		{"double", "t.f * 2", "f * 2"},
		{"text", "t.s || 'x'", "s || 'x'"},
		{"date", "t.d + 1", "d + 1"},
		{"timestamp", "t.ts + INTERVAL '1 hour'", "ts + INTERVAL '1 hour'"},
		{"bool", "NOT t.o", "NOT o"},
		{"array", "ARRAY[t.i, 9]", "ARRAY[i, 9]"},
	} {
		cells = append(cells, [3]string{"type/" + ty[0], "true",
			"SELECT " + ty[1] + " AS k, count(*) AS c FROM ss_t t GROUP BY " + ty[2] + " ORDER BY 1"})
	}
	return cells
}

func TestArcGKGenerateWire(t *testing.T) {
	path := os.Getenv("GKW_GEN")
	if path == "" {
		t.Skip("GKW_GEN unset")
	}
	var b strings.Builder
	for _, c := range gkWireCells() {
		fmt.Fprintf(&b, "%s\t%s\t%s\n", c[0], c[1], c[2])
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestArcGKGroupKeySpellingOnTheWire(t *testing.T) {
	srv := setupSSAuditWireDB(t)
	ctx := context.Background()
	modes := map[string]string{"s": "simple_protocol", "e": "exec", "b": "describe_exec"}
	conns := map[string]*pgx.Conn{}
	for m, qm := range modes {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=%s", srv.Addr(), qm))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		conns[m] = conn
	}
	// A kept cell (name, mode, this engine's answer, why) is a difference
	// outside this seam, asserted as it stands so a change FAILS and is
	// re-measured.
	kept := map[string][2]string{}
	for _, k := range ssAuditWireTSV(t, "testdata/arc_gk_group_key_spelling_wire_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	cells := ssAuditWireTSV(t, "testdata/arc_gk_group_key_spelling_wire_pg17.tsv", 5)
	if want := 3 * len(gkWireCells()); len(cells) != want {
		t.Fatalf("%d measured cells, want %d (three modes per cell): re-measure", len(cells), want)
	}
	for _, c := range cells {
		name, mode, ordered, sql, pg := c[0], c[1], c[2] == "true", c[3], c[4]
		t.Run(name+"/"+mode, func(t *testing.T) {
			conn := conns[mode]
			var got, firstErr string
			for _, st := range strings.Split(sql, " ;; ") {
				r := ssAuditWireRun(ctx, conn, st, ordered, mode == "b")
				if strings.HasPrefix(r, "ERR") && firstErr == "" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(st)), "DROP") {
					firstErr = r
				}
				got = r
			}
			if firstErr != "" {
				got = firstErr
			}
			want, why := pg, "PostgreSQL 17.11 sends"
			if k, ok := kept[name+"/"+mode]; ok {
				want, why = k[0], "kept: "+k[1]
			}
			if got != want {
				t.Errorf("%s\n  got  %s\n  want %s (%s)", sql, got, want, why)
			}
		})
	}
}
