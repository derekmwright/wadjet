// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestNetworkTextGrammarAnswersTheSameOnEveryArm is arc NT's distribution arm.
//
// wadjet.TestEveryNetworkTypeReadsOneTextGrammarAtEveryBoundary walks the
// type × form × boundary table on ONE arm; this one walks the CAST and the
// round-trip through TEXT on four — single, DAG, DAG-shuffled and spilled —
// because a cast's box and its DECLARATION are produced by different code on
// each (#813's shape: `CAST(x AS IPV4)` declared STRING until #1092, and the
// DAG builds its gather vector from the declaration). A cell that answers on
// one arm and refuses on another is the defect this catches.
func TestNetworkTextGrammarAnswersTheSameOnEveryArm(t *testing.T) {
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
	spilled := na2Standalone(t, ctx, 512*1024)
	// The FIFTH arm: the DAG with morsel-parallel breakers, where a fragment's
	// operators run in clones. A refusal that a clone decides has to be the
	// same refusal every other clone and every other arm decides.
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
		{"spilled", func(sql string) ([]string, error) {
			restore := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			out, err := na2Run(tmdRunSingle(ctx, spilled, sql))
			restoreRuns()
			exec.ForceAggDrainEvery(restore)
			return out, err
		}},
	}

	for _, c := range []struct {
		name string
		sql  string
		want string // na2Run's rendering of the one row every arm must give, or "" for a refusal
		code string // the SQLSTATE when want is ""
	}{
		// A literal in every spelling the type's grammar takes, canonicalized
		// by the cast — which is what a column of that type boxes as.
		{"ipv4 abbreviated-zeros", `SELECT CAST('010.1.2.3' AS IPV4) AS v FROM typemx WHERE id = 1`,
			"v=10.1.2.3", ""},
		{"ipv4 host prefix", `SELECT CAST('10.0.0.1/32' AS IPV4) AS v FROM typemx WHERE id = 1`,
			"v=10.0.0.1", ""},
		{"ipv6 upper", `SELECT CAST('2001:DB8::1' AS IPV6) AS v FROM typemx WHERE id = 1`,
			"v=2001:db8::1", ""},
		{"cidr abbreviated", `SELECT CAST('192.168/16' AS CIDR) AS v FROM typemx WHERE id = 1`,
			"v=192.168/16", ""},
		{"mac grouped hex", `SELECT CAST('aabbcc:ddeeff' AS MACADDR) AS v FROM typemx WHERE id = 1`,
			"v=aa:bb:cc:dd:ee:ff", ""},
		{"mac variable width", `SELECT CAST('a:b:c:d:e:f' AS MACADDR) AS v FROM typemx WHERE id = 1`,
			"v=0a:0b:0c:0d:0e:0f", ""},
		{"uuid braced", `SELECT CAST('{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}' AS UUID) AS v ` +
			`FROM typemx WHERE id = 1`, "v=a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", ""},
		// na2Run prints the Go TYPE of every non-string box, so these two cells
		// also pin that a PROTOCOL cast lands in an int32 on EVERY arm — "the
		// right number under the wrong Go type" is what #728 and #784 were.
		{"protocol name", `SELECT CAST('udp' AS PROTOCOL) AS v FROM typemx WHERE id = 1`, "v=int32:17", ""},

		// The ROUND TRIP through the type's own text, over a real column: the
		// cast has to read back exactly what the column prints, on every arm.
		{"ipv4 round trip", `SELECT CAST(CAST(c_ipv4 AS TEXT) AS IPV4) AS v FROM typemx WHERE id = 1`,
			"v=10.0.0.1", ""},
		{"mac round trip", `SELECT CAST(CAST(c_mac AS TEXT) AS MACADDR) AS v FROM typemx WHERE id = 1`,
			"v=aa:bb:cc:00:00:01", ""},
		{"protocol round trip",
			`SELECT CAST(protocol_name(c_proto) AS PROTOCOL) AS v FROM typemx WHERE id = 1`, "v=int32:1", ""},

		// And the two refusal classes, which must also be the same everywhere:
		// text naming no value is 22P02, PostgreSQL-valid text naming a NETWORK
		// is 0A000 (ADR-0012 item 5).
		{"ipv4 garbage", `SELECT CAST('abc' AS IPV4) AS v FROM typemx WHERE id = 1`, "", "22P02"},
		{"mac regrouped", `SELECT CAST('aabb:ccdd:eeff' AS MACADDR) AS v FROM typemx WHERE id = 1`,
			"", "22P02"},
		{"uuid loose hyphen",
			`SELECT CAST('a-0eebc999c0b4ef8bb6d6bb9bd380a11' AS UUID) AS v FROM typemx WHERE id = 1`,
			"", "22P02"},
		{"ipv4 network", `SELECT CAST('10/8' AS IPV4) AS v FROM typemx WHERE id = 1`, "", "0A000"},

		// A value ENTERING PORT or PROTOCOL is held to the TYPE's range, at
		// every door and on every arm (Derek, 2026-09-15; the round-2 review's
		// FC-2). The in-range cells are here too, so a repair that refused
		// everything could not pass.
		{"port in range", `SELECT CAST(65535 AS PORT) AS v FROM typemx WHERE id = 1`, "v=int32:65535", ""},
		{"port from text in range", `SELECT CAST('65535' AS PORT) AS v FROM typemx WHERE id = 1`,
			"v=int32:65535", ""},
		{"protocol in range", `SELECT CAST(255 AS PROTOCOL) AS v FROM typemx WHERE id = 1`,
			"v=int32:255", ""},
		{"port one past", `SELECT CAST(65536 AS PORT) AS v FROM typemx WHERE id = 1`, "", "22003"},
		{"port from text one past", `SELECT CAST('65536' AS PORT) AS v FROM typemx WHERE id = 1`,
			"", "22003"},
		{"port negative", `SELECT CAST(-1 AS PORT) AS v FROM typemx WHERE id = 1`, "", "22003"},
		{"port from a wider int", `SELECT CAST(3000000000 AS PORT) AS v FROM typemx WHERE id = 1`,
			"", "22003"},
		{"protocol one past", `SELECT CAST(256 AS PROTOCOL) AS v FROM typemx WHERE id = 1`, "", "22003"},
		{"protocol negative", `SELECT CAST(-1 AS PROTOCOL) AS v FROM typemx WHERE id = 1`, "", "22003"},
		// ARITHMETIC is NOT a type boundary and keeps int4's range, which is
		// PostgreSQL's `smallint + 1` rule and #901's position.
		{"port arithmetic leaves the range", `SELECT c_port + 70000 AS v FROM typemx WHERE id = 1`,
			"v=int64:71025", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				rows, err := arm.run(c.sql)
				if c.want == "" {
					if err == nil {
						t.Errorf("%s: answered %v; every arm must refuse with %s",
							arm.name, rows, c.code)
						continue
					}
					if st := sqlerr.StateOf(err); st != c.code {
						t.Errorf("%s: SQLSTATE %q, want %q (%v)", arm.name, st, c.code, err)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s: refused: %v", arm.name, err)
					continue
				}
				if len(rows) != 1 || rows[0] != c.want {
					t.Errorf("%s: = %v, want [%s]", arm.name, rows, c.want)
				}
			}
		})
	}
}
