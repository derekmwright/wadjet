// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// psBinaryCells are statements whose numeric column holds values of
// different display scales (ADR-0024 §1 as amended, arc PS stage 1) over
// ss_t.n NUMERIC(10,2): a COALESCE / CASE / LEAST fold with a literal and a
// set operation with a literal arm.
func psBinaryCells() [][2]string {
	return [][2]string{
		{"coalesce15", "SELECT id, COALESCE(n, 1.5) FROM ss_t"},
		{"coalesce13", "SELECT id, COALESCE(n, 12.3456789012345) FROM ss_t"},
		{"coalesceq7", "SELECT id, COALESCE(n, '7') FROM ss_t"},
		{"caseint", "SELECT id, CASE WHEN id < 3 THEN n ELSE 0 END FROM ss_t"},
		{"leastq05", "SELECT id, LEAST(n, '0.5') FROM ss_t"},
		{"union15", "SELECT n FROM ss_t UNION ALL SELECT 1.5 FROM ss_i WHERE id = 1"},
		{"union250", "SELECT n FROM ss_t UNION ALL SELECT 2.500 FROM ss_i WHERE id = 1"},
	}
}

// psBinaryModes: the text protocol, the extended protocol with text results
// and the extended protocol with BINARY results (pgx's describe_exec).
var psBinaryModes = map[string]string{"s": "simple_protocol", "e": "exec", "b": "describe_exec"}

// TestArcPSBinaryDscaleIsTheText: every protocol sends a value's display
// scale. The binary numeric's dscale is built from the value's text
// (pgNumericDigits), so a binary client reads the same digits a text client
// reads — the raw DataRow bytes equal PostgreSQL 17.11's in all three modes
// (testdata/arc_ps_binary_dscale_wire_pg17.tsv; PS_PG_DSN regenerates it),
// and the binary cells' dscale fields are the text-mode cells' fraction
// lengths (as multisets: each mode's rows are sorted by their own bytes).
func TestArcPSBinaryDscaleIsTheText(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("PS_PG_DSN"); dsn != "" {
		psBinaryMeasure(t, ctx, dsn)
		return
	}
	srv := setupSSAuditWireDB(t)
	conns := map[string]*pgx.Conn{}
	for m, qm := range psBinaryModes {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=%s", srv.Addr(), qm))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		conns[m] = conn
	}
	cells := ssAuditWireTSV(t, "testdata/arc_ps_binary_dscale_wire_pg17.tsv", 3)
	if want := 3 * len(psBinaryCells()); len(cells) != want {
		t.Fatalf("%d measured cells, want %d: re-measure", len(cells), want)
	}
	pg := map[string]string{}
	for _, c := range cells {
		pg[c[0]+"/"+c[1]] = c[2]
	}
	for _, c := range psBinaryCells() {
		text := ""
		for _, mode := range []string{"s", "e", "b"} {
			got := ssAuditWireRun(ctx, conns[mode], c[1], false, mode == "b")
			if got != pg[c[0]+"/"+mode] {
				t.Errorf("%s [%s]\n  got  %s\n  want %s (PostgreSQL 17.11 sends)", c[1], mode, got, pg[c[0]+"/"+mode])
			}
			switch mode {
			case "s":
				text = got
			case "b":
				if d := psBinaryDscales(got); d != psTextFractionLengths(text) {
					t.Errorf("%s: binary dscales %s, text fraction lengths %s", c[1], d, psTextFractionLengths(text))
				}
			}
		}
	}
}

// psLastCells is the last cell of every row of an answer (the numeric
// column; an id, when present, comes first), in the answer's row order.
func psLastCells(ans string) []string {
	_, rest, _ := strings.Cut(ans, " ")  // {oids}
	_, rows, _ := strings.Cut(rest, " ") // rows=N
	var out []string
	for _, row := range strings.Split(rows, " | ") {
		cells := strings.Split(row, ",")
		out = append(out, cells[len(cells)-1])
	}
	return out
}

// psBinaryDscales reads the dscale field (bytes 6..8) of every non-NULL
// binary numeric cell.
func psBinaryDscales(ans string) string {
	var out []string
	for _, cell := range psLastCells(ans) {
		b, err := hex.DecodeString(cell)
		if cell == "NULL" || err != nil || len(b) < 8 {
			continue
		}
		out = append(out, fmt.Sprint(binary.BigEndian.Uint16(b[6:8])))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// psTextFractionLengths is each non-NULL text numeric cell's fraction length.
func psTextFractionLengths(ans string) string {
	var out []string
	for _, s := range psLastCells(ans) {
		if s == "NULL" {
			continue
		}
		d := 0
		if i := strings.IndexByte(s, '.'); i >= 0 {
			d = len(s) - i - 1
		}
		out = append(out, fmt.Sprint(d))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// psBinaryMeasure writes the PostgreSQL answers: PS_PG_DSN names a database
// holding ss_t (id bigint, n numeric(10,2)) and ss_i (id bigint) with
// setupSSAuditWireDB's rows.
func psBinaryMeasure(t *testing.T, ctx context.Context, dsn string) {
	var lines []string
	for _, c := range psBinaryCells() {
		for _, mode := range []string{"s", "e", "b"} {
			conn, err := pgx.Connect(ctx, dsn+"&default_query_exec_mode="+psBinaryModes[mode])
			if err != nil {
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", c[0], mode, ssAuditWireRun(ctx, conn, c[1], false, mode == "b")))
			conn.Close(ctx)
		}
	}
	body := "# PostgreSQL 17.11 (pgx raw DataRow values; hex in binary mode; rows sorted) for TestArcPSBinaryDscaleIsTheText.\n" +
		strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile("testdata/arc_ps_binary_dscale_wire_pg17.tsv", []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
