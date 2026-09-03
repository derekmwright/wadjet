package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// One literal, one disposition, at every site (#627, #579).
//
// PostgreSQL takes six spellings for a macaddr and six for a uuid. Wadjet's
// parsers are thin wrappers over Go's, which take four of each, so three MAC
// notations and the braced UUID were 22P02 — and the same literal was refused
// at `=` and `IN`, silently ACCEPTED at `CASE`, `IS DISTINCT FROM`, `GREATEST`
// and `LEAST`, and skipped entirely over an empty scan. A query's disposition
// depended on its SHAPE and on its DATA, which is #579's original defect.
//
// Two changes, and the second is the one that made the first reach every site:
//
//   - `kernel.parseMACToInt64` and `parseUUIDToRawString` take PostgreSQL's
//     remaining spellings. Value-preserving: every one names the same bytes.
//   - the grammar had FOUR independent implementations — the vectorized
//     kernel, `exec.parseMACFilterVal` (row-at-a-time), and
//     `expr.macLitToInt64` / `expr.parseUUIDHex` (boxed pair, which is what
//     `CASE` and `GREATEST` take). Widening the kernel alone left the DAG
//     raising 22P02 for a literal the single-process path had just accepted.
//     They delegate to one parser now (protocol method 6).
//
// The sites NOT covered are named rather than implied: the INGEST door
// (`parquet.writer`, `batch.Vector.SetValue`) and the `mac_*` formatting
// functions keep Go's grammar. Widening a comparison literal cannot change a
// stored value; widening the store is a parquet change and is not this
// commit's.
//
// The abbreviated CIDR/inet half of #627 is DEFERRED — PostgreSQL's
// abbreviation is CLASSFUL address inference (`'10'` -> 10.0.0.0/8,
// `'192.168'` -> 192.168.0.0/24) and reproducing `inet_net_pton` bit-exactly
// is its own decision with its own oracle corpus.
type a2NetCell struct {
	issue, name, sql string
	want             string
	wantErrLike      string
	wantState        string
	pgSays           string
}

// a2NetSites renders one literal at every site the census measured, so a
// spelling that is right at `=` and wrong at `GREATEST` cannot pass.
func a2NetSites(col, lit string) []struct{ name, sql string } {
	q := "'" + lit + "'"
	return []struct{ name, sql string }{
		{"eq", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s = %s`, col, q)},
		{"in", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s IN (%s)`, col, q)},
		{"case", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE CASE %s WHEN %s THEN 1 ELSE 0 END = 1`, col, q)},
		{"is_distinct", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s IS DISTINCT FROM %s`, col, q)},
		{"greatest", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE GREATEST(%s, %s) = %s`, col, q, col)},
		{"least", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE LEAST(%s, %s) = %s`, col, q, col)},
		// The EMPTY-SCAN shape, which is #579's own: the refusal is per ROW,
		// so a predicate no row reaches used to answer where the same
		// predicate over rows raised.
		{"empty_scan", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND %s = %s`, col, q)},
	}
}

func TestANetworkLiteralHasOneDispositionAtEverySite(t *testing.T) {
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

	// The literals PostgreSQL ACCEPTS. typemx holds aa:bb:cc:xx:xx:xx MACs and
	// 00000000-0000-4000-8000-xxxxxxxxxxxx UUIDs, so none of these matches a
	// row — what is asserted is that the query ANSWERS rather than raising,
	// identically at every site and on every arm.
	accepted := []struct{ col, lit, pgSays string }{
		{"c_mac", "08002b:010203", "08:00:2b:01:02:03"},
		{"c_mac", "08002b-010203", "08:00:2b:01:02:03"},
		{"c_mac", "0800-2b01-0203", "08:00:2b:01:02:03"},
		// The four Go already took, as controls.
		{"c_mac", "08:00:2b:01:02:03", "08:00:2b:01:02:03"},
		{"c_mac", "08-00-2b-01-02-03", "08:00:2b:01:02:03"},
		{"c_mac", "0800.2b01.0203", "08:00:2b:01:02:03"},
		{"c_uuid", "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"c_uuid", "a0eebc999c0b4ef8bb6d6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"c_uuid", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"c_uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
	}

	arms := func() []struct {
		name string
		run  func(string) ([]string, error)
	} {
		// Every DAG arm reads the routing counters around its own run and
		// asserts that NOTHING routed (rule 11). Every shape in this gate
		// names a real table, so a route here would mean one of the nine
		// refusals fired and the "DAG" arm was the coordinator-local pipeline
		// -- which is exactly what five cells in the arc's other gates turned
		// out to be.
		dag := func(c *Coordinator, name, sql string) ([]string, error) {
			before := a2ReadRoutes(c)
			got, err := na2Run(tmdRunDAG(ctx, c, sql))
			a2CheckRoutes(t, name, before, a2ReadRoutes(c), a2Routes{}, sql)
			return got, err
		}
		return []struct {
			name string
			run  func(string) ([]string, error)
		}{
			{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
			{"dag", func(sql string) ([]string, error) { return dag(coord, "dag", sql) }},
			{"dag-shuffled", func(sql string) ([]string, error) { return dag(coordB, "dag-shuffled", sql) }},
		}
	}

	for _, lit := range accepted {
		t.Run("#627/accepted/"+lit.col+"/"+strings.NewReplacer(":", "_", "-", "_", "{", "", "}", "", ".", "_").Replace(lit.lit),
			func(t *testing.T) {
				for _, site := range a2NetSites(lit.col, lit.lit) {
					for _, arm := range arms() {
						got, err := arm.run(site.sql)
						if err != nil {
							t.Errorf("%s arm, site %s: %v\n  PostgreSQL 17 reads this literal as %s\n  SQL: %s",
								arm.name, site.name, err, lit.pgSays, site.sql)
							continue
						}
						if len(got) != 1 {
							t.Errorf("%s arm, site %s: %d rows, want 1\n  SQL: %s",
								arm.name, site.name, len(got), site.sql)
						}
					}
				}
			})
	}

	// A literal PostgreSQL REFUSES, at every site. This is the other half of
	// "one disposition": before, `= 'zzz'` raised 22P02 and
	// `CASE c_mac WHEN 'zzz'` answered 0, and `GREATEST(c_cidr, 'zzz')`
	// answered 4926 — a number produced by byte-comparing the four characters
	// "zzz" against stored addresses.
	//
	// It is a PIN, not an assertion of the right behaviour: the four
	// non-equality sites still ANSWER where PostgreSQL raises, because the
	// boxed-pair comparators reach them and the refusal lives in the kernel
	// and the row-at-a-time path. Unifying the REFUSAL is the rest of #579 and
	// is not in this commit; unifying the GRAMMAR is, and the accepted half
	// above proves it.
	//
	// One entry has an EMPTY refusedAt and it is not an oversight:
	// `c_mac = 'not-a-mac-at-all'` is refused by the single-process path and
	// ANSWERS 0 on both DAG arms, measured. The literal parses on neither, so
	// the difference is which layer meets it first — the scan's row-group
	// prune withholds a domain it cannot build and the DAG's fragment then
	// never evaluates the predicate, while the single-process filter does and
	// raises. It is the same DATA-dependence as the empty-scan site, one
	// engine over, and it is pinned rather than fixed for the same reason the
	// rest of #579 is.
	for _, tc := range []struct {
		col, lit  string
		refusedAt map[string]bool
		// singleOnly names the sites where the SINGLE path refuses and the
		// DAG answers. Those cells assert the split rather than one
		// disposition, so closing it is visible.
		singleOnly map[string]bool
	}{
		{"c_mac", "zzz", map[string]bool{"eq": true, "in": true}, nil},
		{"c_uuid", "not-a-uuid", map[string]bool{"eq": true, "in": true}, nil},
		{"c_cidr", "zzz", map[string]bool{"eq": true, "in": true}, nil},
		{"c_mac", "not-a-mac-at-all", nil,
			map[string]bool{"eq": true, "in": true}},
		// The five REGROUPINGS. Twelve hex digits with one or two separators
		// ANYWHERE parsed while the widening counted separators instead of
		// GROUP SIZES, so wadjet accepted five spellings PostgreSQL 17
		// refuses with 22P02 — a superset the code's own comment and
		// ADR-0012's entry both denied. Measured on 17.11: the grouped-hex
		// grammar is 6+6 and 4+4+4 and nothing else.
		//
		// They sit in the REFUSED table rather than in a note because a bound
		// with no fixture is the shape this arc keeps finding: the accepted
		// half had ten literals x seven sites and the refused half had none
		// of these.
		{"c_mac", "0-8-002b010203", map[string]bool{"eq": true, "in": true}, nil},
		{"c_mac", "0:8002b010203", map[string]bool{"eq": true, "in": true}, nil},
		{"c_mac", "08-002b010203", map[string]bool{"eq": true, "in": true}, nil},
		{"c_mac", "08002b:01:0203", map[string]bool{"eq": true, "in": true}, nil},
		{"c_mac", "08:002b:010203", map[string]bool{"eq": true, "in": true}, nil},
		// A sixth regrouping, added because the five above all keep 12
		// digits in 2 or 3 groups: this one is 6+4+2, which the SIZE check
		// refuses and a COUNT check would not.
		{"c_mac", "08002b:0102:03", map[string]bool{"eq": true, "in": true}, nil},
	} {
		t.Run("#579/pin_refused_at_some_sites/"+tc.col+"/"+tc.lit, func(t *testing.T) {
			for _, site := range a2NetSites(tc.col, tc.lit) {
				for _, arm := range arms() {
					_, err := arm.run(site.sql)
					if tc.singleOnly[site.name] {
						// The split: single refuses, the DAG answers.
						if arm.name == "single" {
							if err == nil {
								t.Errorf("single arm, site %s: ANSWERED; this pin records that "+
									"it REFUSES and the DAG does not\n  SQL: %s", site.name, site.sql)
							}
							continue
						}
						if err != nil {
							t.Errorf("%s arm, site %s now REFUSES like the single-process path — "+
								"the two-path split this pin records is CLOSED, so move this "+
								"literal into refusedAt\n  err: %v\n  SQL: %s",
								arm.name, site.name, err, site.sql)
						}
						continue
					}
					if tc.refusedAt[site.name] {
						if err == nil {
							t.Errorf("%s arm, site %s: ANSWERED a literal PostgreSQL refuses\n  SQL: %s",
								arm.name, site.name, site.sql)
						} else if s := sqlerr.StateOf(err); s != "22P02" {
							t.Errorf("%s arm, site %s: SQLSTATE %q, want 22P02\n  SQL: %s",
								arm.name, site.name, s, site.sql)
						}
						continue
					}
					// PINNED: PostgreSQL refuses here too and wadjet answers.
					// A site that starts REFUSING has closed part of #579 —
					// delete it from this pin and assert the refusal.
					if err != nil {
						t.Errorf("%s arm, site %s now REFUSES, which is PostgreSQL's answer — "+
							"#579 is closed for this site, so add it to refusedAt\n  err: %v\n  SQL: %s",
							arm.name, site.name, err, site.sql)
					}
				}
			}
		})
	}
}
