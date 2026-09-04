package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A PORT, PROTOCOL or DURATION column keeps its VALUE on every rung the
// numeric ladder moves it to (#648, ADR-0012 item 12).
//
// The three declare int4 / int8 on the wire but store a domain vector of their
// own, and setOpColTypeFromColumn — the single-process path's adapter from a
// runtime column onto the ladder — did not resolve them. The fold then
// DECLINED the pair and materialised the union in the FIRST arm's carrier, so
// a value the ladder had widened to bigint was written back into an int4 box:
//
//	SELECT c_port … UNION ALL SELECT 4000000000     ->  -294967296
//	SELECT c_port … UNION ALL SELECT c_i64*1000000  ->  -724379968, -1448759936
//
// Silently, with the right ROW COUNT, and only on the single-process path —
// which is also the coordinator's local fast path, so it is the default for a
// small query.
//
// The 324-cell type-pair matrix cannot see this: it unions COLUMNS, and every
// value in the fixture's int4-backed columns fits int4, so the wrap has nothing
// to wrap. This gate supplies what it lacks — an arm whose value is OUTSIDE
// int32, as a literal and as an expression — and asserts the DECLARED type
// beside the values on all three arms, since the declaration is the other half
// of the same defect: a client reading int4 for a column carrying int8 values
// is the wrong OID for a right value.
//
// PostgreSQL declares `integer` where the equal-rank int4 family resolves to
// bigint here (`c_port ∪ c_proto`, `c_port ∪ 443`): no value moves, and this
// engine has no CAST spelling that produces an int4 carrier, so declaring int4
// would put the type on a box that is not one. The divergence is ADR-0012 item
// 12's, recorded there, and it is a DECLARED WIDTH — never a value.
func setOpWireIntCells() []setOpUnkDeclCell {
	return []setOpUnkDeclCell{
		// The literal outside int32. This is the cell the wrap fix owes: with
		// setOpColTypeFromColumn's PORT/PROTOCOL/DURATION case reverted, the
		// single arm answers -294967296 here and the matrix stays green.
		{issue: "#648", name: "a_port_beside_a_literal_outside_int32",
			sql: `SELECT c_port AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT 4000000000 FROM typemx WHERE id = 0`,
			wantDecl: "v:INT64",
			wantRows: []string{"1024", "1025", "1026", "4000000000"}},
		{issue: "#648", name: "a_protocol_beside_a_literal_outside_int32",
			sql: `SELECT c_proto AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT 4000000000 FROM typemx WHERE id = 0`,
			wantDecl: "v:INT64",
			wantRows: []string{"0", "1", "2", "4000000000"}},
		{issue: "#648", name: "a_duration_beside_a_literal_outside_int32",
			sql: `SELECT c_dur AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT 4000000000 FROM typemx WHERE id = 0`,
			wantDecl: "v:INT64",
			wantRows: []string{"0", "1000000", "2000000", "4000000000"}},
		// The EXPRESSION spelling, whose values are outside int32 on two rows
		// rather than one: the literal arm could be answered by a literal
		// special case, a computed column cannot.
		{issue: "#648", name: "a_port_beside_an_expression_outside_int32",
			sql: `SELECT c_port AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT c_i64 * 1000000 FROM typemx WHERE id < 3`,
			wantDecl: "v:INT64",
			wantRows: []string{"0", "1000003000000", "1024", "1025", "1026", "2000006000000"}},
		// The arm order reversed: the wide arm FIRST is the shape that always
		// worked, and it is the control that says the fix did not simply move
		// the box.
		{issue: "#648", name: "ctl_a_wide_expression_beside_a_port",
			sql: `SELECT c_i64 * 1000000 AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT c_port FROM typemx WHERE id < 3`,
			wantDecl: "v:INT64",
			wantRows: []string{"0", "1000003000000", "1024", "1025", "1026", "2000006000000"}},
		// The FLOAT rung. An earlier draft refused REAL beside these three,
		// which turned rows PostgreSQL answers as `real` into hard errors —
		// round 2's second blocker. Both orders.
		{issue: "#648", name: "a_port_beside_a_real",
			sql: `SELECT c_port AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT c_f32 FROM typemx WHERE id < 3`,
			wantDecl: "v:FLOAT32",
			wantRows: []string{"0", "0.14285715", "0.2857143", "1024", "1025", "1026"}},
		{issue: "#648", name: "a_real_beside_a_duration",
			sql: `SELECT c_f32 AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT c_dur FROM typemx WHERE id < 3`,
			wantDecl: "v:FLOAT32",
			// 1e+06 / 2e+06 is float32's own shortest round-trip spelling of
			// those two DURATION values, on all three arms — the FLOAT rung is
			// lossy by design and `real ∪ bigint` is real in PostgreSQL too.
			wantRows: []string{"0", "0", "0.14285715", "0.2857143", "1e+06", "2e+06"}},
		// A pair that is already ONE type is not widened: PORT ∪ PORT stays
		// PORT, which is `integer` in PostgreSQL too.
		{issue: "#648", name: "ctl_a_port_beside_a_port_stays_a_port",
			sql: `SELECT c_port AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT c_port FROM typemx WHERE id < 3`,
			wantDecl: "v:PORT",
			wantRows: []string{"1024", "1024", "1025", "1025", "1026", "1026"}},
	}
}

func TestAWireDeclaredIntegerKeepsItsValueOnEveryRung(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range setOpWireIntCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			check := func(arm, decl string, rows []string, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
				}
				if decl != tc.wantDecl {
					t.Errorf("%s arm DECLARES %s, want %s\n  SQL: %s", arm, decl, tc.wantDecl, tc.sql)
				}
				if got := strings.Join(rows, "|"); got != strings.Join(tc.wantRows, "|") {
					t.Errorf("%s arm RENDERS\n  %s\nwant\n  %s\n"+
						"(a negative here is the int4 box: the union was materialised in the "+
						"first arm's carrier)\n  SQL: %s",
						arm, got, strings.Join(tc.wantRows, "|"), tc.sql)
				}
			}
			decl, rows, err := sudSingle(ctx, single, tc.sql)
			check("single", decl, rows, err)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				decl, rows, err := sudDAG(ctx, arm.c, tc.sql)
				check(arm.name, decl, rows, err)
			}
		})
	}
}
