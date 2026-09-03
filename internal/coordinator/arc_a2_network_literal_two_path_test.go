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
		return []struct {
			name string
			run  func(string) ([]string, error)
		}{
			{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
			{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
			{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
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
	for _, tc := range []struct {
		col, lit  string
		refusedAt map[string]bool
	}{
		{"c_mac", "zzz", map[string]bool{"eq": true, "in": true}},
		{"c_uuid", "not-a-uuid", map[string]bool{"eq": true, "in": true}},
		{"c_cidr", "zzz", map[string]bool{"eq": true, "in": true}},
	} {
		t.Run("#579/pin_refused_at_some_sites/"+tc.col+"/"+tc.lit, func(t *testing.T) {
			for _, site := range a2NetSites(tc.col, tc.lit) {
				for _, arm := range arms() {
					_, err := arm.run(site.sql)
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
