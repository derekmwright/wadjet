// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// ARC FR — NO DOOR OPENS A FILE BEFORE THE IDENTITY IS AUTHORIZED.
//
// This arc lets the planner read a file READER's schema before execution
// (#1230, #1231), which is the thing ADR-0039 §3 refused to do while the
// binder ran before the capability was decided. The order is now the other
// way round — `auth.AuthorizeTableFunctions` is the first thing every
// statement door does — and this is the gate on it.
//
// `physical.ReaderSchemaReads` counts the times the planner actually OPENED a
// reader's input. The property, per door: a statement whose identity the
// policy refuses moves that counter by ZERO, and the SAME statement under an
// identity that holds the capability moves it. A counter is the discriminator
// because the refusal alone is not one: a plan that opened the file and THEN
// refused answers 42501 too.
//
// #943's own gate (TestTableFunctionsAreACapabilityOnEveryDoor) keeps the two
// complementary proofs: a path that would error if opened refuses with 42501
// rather than "no such file", and a loopback HTTP source fails the test if a
// request ever arrives.
func TestArcFRNoDoorOpensAReaderBeforeTheIdentityIsAuthorized(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)
	doors := append(append([]sec4Door{}, rig.doors...), tfGRPCDoor(t, ctx, rig))

	dir := t.TempDir()
	// A file that EXISTS and is readable. That is what makes this cell a
	// measurement: the planner CAN read its schema, so a counter that does
	// not move is the order holding rather than the file being unreachable.
	real := filepath.Join(dir, "frorder.csv")
	if err := os.WriteFile(real, []byte("secret\nserver-local-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	realJSON := filepath.Join(dir, "frorder.json")
	if err := os.WriteFile(realJSON, []byte("{\"secret\":\"server-local-value\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Every shape a door can be asked, including the ones whose reader is not
	// in the statement's own plan when the capability pass runs.
	cells := []struct{ name, sql string }{
		{"direct", fmt.Sprintf("SELECT * FROM read_csv('%s')", real)},
		{"direct_json", fmt.Sprintf("SELECT * FROM read_json('%s')", realJSON)},
		{"star_qualified", fmt.Sprintf("SELECT f.* FROM read_csv('%s') AS f", real)},
		{"aggregate", fmt.Sprintf("SELECT COUNT(*) AS n FROM read_csv('%s')", real)},
		{"cte", fmt.Sprintf("WITH c AS (SELECT * FROM read_csv('%s')) SELECT * FROM c", real)},
		{"derived_table", fmt.Sprintf("SELECT * FROM (SELECT * FROM read_csv('%s')) d", real)},
		{"scalar_subquery", fmt.Sprintf("SELECT (SELECT COUNT(*) FROM read_csv('%s')) AS c", real)},
		{"join_arm", fmt.Sprintf(
			"SELECT u.id FROM users u JOIN read_csv('%s') f ON u.name = f.secret", real)},
		{"two_reader_join", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM read_csv('%s') a JOIN read_json('%s') b ON b.secret = a.secret",
			real, realJSON)},
		{"union_arm", fmt.Sprintf(
			"SELECT name FROM users UNION ALL SELECT secret FROM read_csv('%s')", real)},
		{"explain", fmt.Sprintf("EXPLAIN SELECT * FROM read_csv('%s')", real)},
	}

	for _, d := range doors {
		d := d
		t.Run("refused-identity-opens-nothing/"+d.name, func(t *testing.T) {
			for _, c := range cells {
				before := physical.ReaderSchemaReads.Load()
				_, class, err := d.run(t, sec4Reader, c.sql)
				sec4Refused(t, d, c.name, class, err, "42501")
				if after := physical.ReaderSchemaReads.Load(); after != before {
					t.Errorf("%s/%s: the planner opened the reader's input %d time(s) for an "+
						"identity the policy refuses — the capability must be decided first",
						d.name, c.name, after-before)
				}
			}
		})
	}

	// The other half, and the one that makes the zeros above mean something:
	// an identity that HOLDS the capability reads the schema, and the
	// statement answers from it.
	for _, d := range doors {
		d := d
		t.Run("authorized-identity-reads/"+d.name, func(t *testing.T) {
			before := physical.ReaderSchemaReads.Load()
			rows, _, err := d.run(t, sec4Ops, fmt.Sprintf("SELECT f.* FROM read_csv('%s') AS f", real))
			if err != nil {
				t.Fatalf("%s: an admin identity must read the file: %v", d.name, err)
			}
			if physical.ReaderSchemaReads.Load() == before {
				t.Errorf("%s: the planner read NO schema for an authorized identity, so the "+
					"zero above proves nothing — the counter is not wired to this door", d.name)
			}
			if len(rows) != 1 || !strings.Contains(rows[0], "server-local-value") {
				t.Errorf("%s: qualified star over a reader answered %v", d.name, rows)
			}
		})
	}
}
