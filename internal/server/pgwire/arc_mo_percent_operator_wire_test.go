// SPDX-License-Identifier: MIT

package pgwire

// THE `%` OPERATOR IS MOD (#1527), as a client reads it: the declared OID,
// the text encoder over the simple and the extended protocol, the binary
// encoder, and a parameter's type in the ParameterDescription. `a % b`
// parses to mod(a, b), so the operator declares what MOD declares — `t.i % 3`
// is integer (OID 23), as on PostgreSQL, where the operator's own declaration
// was bigint — and a parameter beside MOD's other operand takes that
// operand's type, as the operator's always did (`MOD(t.n, $1)` was undecided,
// OID 0, and a binary value for it was refused).
//
// Every want is PostgreSQL 17.11's, measured with the same client in the same
// modes (testdata/arc_mo_percent_operator_wire_pg17.tsv and
// …_params_pg17.tsv); the cells in the two _kept.tsv files are catalogued
// divergences or recorded filing candidates, asserted as they stand. Every
// `%` cell also answers exactly what its MOD twin answers. The five-arm table
// is coordinator.TestArcMOPercentIsModEveryArm.

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// moWireMatches compares a wire answer with the wanted one: a refusal by its
// SQLSTATE (the message wording is not this seam's dimension), a result
// exactly.
func moWireMatches(got, want string) bool {
	if rest, ok := strings.CutPrefix(want, "ERR "); ok {
		state, _, _ := strings.Cut(rest, " ")
		g, ok := strings.CutPrefix(got, "ERR ")
		gs, _, _ := strings.Cut(g, " ")
		return ok && gs == state
	}
	return got == want
}

// moWireTwin checks every `%` cell against its MOD twin, mode by mode.
func moWireTwin(t *testing.T, got map[string]string) {
	t.Helper()
	for k, g := range got {
		name, mode, _ := strings.Cut(k, "\x00")
		twin, ok := strings.CutSuffix(name, "/pct")
		if !ok {
			continue
		}
		if m := got[twin+"/mod\x00"+mode]; !moWireMatches(g, m) {
			t.Errorf("%s (%s): %% sends %s, MOD sends %s (the operator is MOD)", name, mode, g, m)
		}
	}
}

func TestArcMOPercentIsModOnTheWire(t *testing.T) {
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
	kept := map[string][2]string{}
	for _, k := range ssAuditWireTSV(t, "testdata/arc_mo_percent_operator_wire_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	cells := ssAuditWireTSV(t, "testdata/arc_mo_percent_operator_wire_pg17.tsv", 5)
	seen := map[string]bool{}
	for _, c := range cells {
		seen[c[0]+"/"+c[1]] = true
	}
	for k := range kept {
		if !seen[k] {
			t.Fatalf("kept cell %s is not in the table", k)
		}
	}
	got := map[string]string{}
	for _, c := range cells {
		name, mode, ordered, sql, pg := c[0], c[1], c[2] == "true", c[3], c[4]
		t.Run(name+"/"+mode, func(t *testing.T) {
			conn := conns[mode]
			var g, firstErr string
			for _, st := range strings.Split(sql, " ;; ") {
				r := ssAuditWireRun(ctx, conn, st, ordered, mode == "b")
				if strings.HasPrefix(r, "ERR") && firstErr == "" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(st)), "DROP") {
					firstErr = r
				}
				g = r
			}
			if firstErr != "" {
				g = firstErr
			}
			got[name+"\x00"+mode] = g
			if strings.HasPrefix(g, "ERR XX000") {
				t.Errorf("%s: an internal error is never an answer: %s", sql, g)
			}
			want, why := pg, "PostgreSQL 17.11 sends"
			if k, ok := kept[name+"/"+mode]; ok {
				want, why = k[0], "kept: "+k[1]
			}
			if !moWireMatches(g, want) {
				t.Errorf("%s\n  got  %s\n  want %s (%s)", sql, g, want, why)
			}
		})
	}
	moWireTwin(t, got)
}

// moParamRun prepares sql with no parameter types and executes it with
// values bound as text (mode t) or binary (mode b, results binary), each
// value encoded for the type the ParameterDescription names, and renders
// params={OIDs} {declared OIDs} rows=N values.
func moParamRun(ctx context.Context, pc *pgconn.PgConn, stmt, mode, sql string, args []string) string {
	sd, err := pc.Prepare(ctx, stmt, sql, nil)
	if err != nil {
		return ssAuditWireErr(err)
	}
	var po []string
	for _, o := range sd.ParamOIDs {
		po = append(po, fmt.Sprint(o))
	}
	head := "params={" + strings.Join(po, ",") + "} "
	m := pgtype.NewMap()
	vals := make([][]byte, len(args))
	pf := make([]int16, len(args))
	for i, a := range args {
		if a == `\N` {
			continue
		}
		if mode == "t" {
			vals[i] = []byte(a)
			continue
		}
		pf[i] = 1
		var v any = a
		switch sd.ParamOIDs[i] {
		case 21, 23, 20:
			x, e := strconv.ParseInt(a, 10, 64)
			if e != nil {
				return head + "ENC " + e.Error()
			}
			v = x
		case 701, 700:
			x, e := strconv.ParseFloat(a, 64)
			if e != nil {
				return head + "ENC " + e.Error()
			}
			v = x
		case 1700:
			var x pgtype.Numeric
			if e := x.Scan(a); e != nil {
				return head + "ENC " + e.Error()
			}
			v = x
		}
		b, e := m.Encode(sd.ParamOIDs[i], 1, v, nil)
		if e != nil {
			return head + "ENC " + e.Error()
		}
		vals[i] = b
	}
	rf := []int16{0}
	if mode == "b" {
		rf = []int16{1}
	}
	res := pc.ExecPrepared(ctx, stmt, vals, pf, rf).Read()
	if res.Err != nil {
		return head + ssAuditWireErr(res.Err)
	}
	var fo []string
	for _, fd := range res.FieldDescriptions {
		fo = append(fo, fmt.Sprint(fd.DataTypeOID))
	}
	rows := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		c := make([]string, len(r))
		for i, x := range r {
			switch {
			case x == nil:
				c[i] = "NULL"
			case mode == "b":
				c[i] = hex.EncodeToString(x)
			default:
				c[i] = string(x)
			}
		}
		rows = append(rows, strings.Join(c, ","))
	}
	return fmt.Sprintf("%s{%s} rows=%d %s", head, strings.Join(fo, ","), len(rows), strings.Join(rows, " | "))
}

// moParamMatches compares by SQLSTATE after the params={…} head for a
// refusal, exactly otherwise.
func moParamMatches(got, want string) bool {
	gh, gr, _ := strings.Cut(got, " ")
	wh, wr, _ := strings.Cut(want, " ")
	if strings.HasPrefix(wr, "ERR ") {
		return gh == wh && moWireMatches(gr, wr)
	}
	return got == want
}

func TestArcMOPercentIsModParameters(t *testing.T) {
	srv := setupSSAuditWireDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	kept := map[string][2]string{}
	for _, k := range ssAuditWireTSV(t, "testdata/arc_mo_percent_operator_params_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	got := map[string]string{}
	for i, c := range ssAuditWireTSV(t, "testdata/arc_mo_percent_operator_params_pg17.tsv", 5) {
		name, mode, sql, argText, pg := c[0], c[1], c[2], c[3], c[4]
		var args []string
		if argText != "" {
			args = strings.Split(argText, "|")
		}
		t.Run(name+"/"+mode, func(t *testing.T) {
			g := moParamRun(ctx, conn.PgConn(), fmt.Sprintf("mo_param_%d", i), mode, sql, args)
			got[name+"\x00"+mode] = g
			if strings.Contains(g, "ERR XX000") {
				t.Errorf("%s: an internal error is never an answer: %s", sql, g)
			}
			want, why := pg, "PostgreSQL 17.11 sends"
			if k, ok := kept[name+"/"+mode]; ok {
				want, why = k[0], "kept: "+k[1]
			}
			if !moParamMatches(g, want) {
				t.Errorf("%s %v\n  got  %s\n  want %s (%s)", sql, args, g, want, why)
			}
		})
	}
	for k, g := range got {
		name, mode, _ := strings.Cut(k, "\x00")
		if twin, ok := strings.CutSuffix(name, "/pct"); ok && !moParamMatches(g, got[twin+"/mod\x00"+mode]) {
			t.Errorf("%s (%s): %% %s, MOD %s (the operator is MOD)", name, mode, g, got[twin+"/mod\x00"+mode])
		}
	}
}
