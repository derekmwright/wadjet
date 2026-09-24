// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc PC's door gates. The system catalog is relations the engine scans
// (ADR-0044), materialized from the storage catalog through the calling
// identity's view — so the catalog is a relation the fix MATERIALIZES, and
// COMMON's masking gate applies to it: on every door, the catalog must list
// exactly what the identity can query (a DENIED column is no column of the
// relation; a MASKED one is), and a catalog relation joined to a policed one
// must not carry a policed value out.

// pcCatalogProvider is pmProvider with e7other taken away from the analyst:
// a relation the identity may not read, so the catalog's table decision is
// exercised as well as its column decision.
func pcCatalogProvider(t *testing.T) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "pc-catalog", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "analyst-masks", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "deny_column", Target: "salary"},
					{Type: "mask_column", Target: "ssn", Value: "'" + pmMaskSSN + "'"},
					{Type: "mask_column", Target: "acct", Value: pmMaskAcct},
				},
			},
			{
				ID: "analyst-bal", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmBal}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "mask_column", Target: "bal", Value: "0"},
				},
			},
			{
				ID: "analyst-other-denied", EffectStr: "deny", Priority: 100,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmOther}},
				Actions:   []auth.Action{auth.ActionRead},
			},
			{
				ID: "admin-raw", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions:  []auth.Action{auth.ActionRead},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "analyst-key", Name: "analyst", Role: "analyst"},
			{Key: "admin-key", Name: "admin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// TestArcPCTheCatalogFollowsThePolicyOnEveryDoor is the masking gate over the
// catalog relations: nine doors, the analyst identity.
func TestArcPCTheCatalogFollowsThePolicyOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, pcCatalogProvider(t))
	leaks := pmTrueValues()

	type want struct {
		has, hasNot []string // cells that must appear, and must not, in the answer
	}
	cells := []struct {
		name, sql string
		want      want
	}{
		{"columns_of_the_policed_table",
			`SELECT column_name FROM information_schema.columns WHERE table_name = 'e7emp' ORDER BY ordinal_position`,
			want{has: []string{"id", "dept", "ssn", "acct", "amt"}, hasNot: []string{"salary"}}},
		{"attributes_through_pg_class",
			`SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid ` +
				`WHERE c.relname = 'e7emp' AND a.attnum > 0 ORDER BY a.attnum`,
			want{has: []string{"ssn", "acct"}, hasNot: []string{"salary"}}},
		{"count_of_the_denied_column",
			`SELECT count(*) AS n FROM information_schema.columns WHERE table_name = 'e7emp' AND column_name = 'salary'`,
			want{has: []string{"0"}}},
		{"relations_the_identity_may_read",
			`SELECT relname FROM pg_class WHERE relname LIKE 'e7%' ORDER BY 1`,
			want{has: []string{"e7bal", "e7emp"}, hasNot: []string{"e7other", "e7net"}}},
		{"tables_view",
			`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1`,
			want{has: []string{"e7emp"}, hasNot: []string{"e7other"}}},
		{"regclass_of_the_denied_relation",
			`SELECT to_regclass('e7other') IS NULL AS hidden`,
			want{has: []string{"true"}}},
		{"catalog_joined_to_the_masked_data",
			`SELECT e.ssn, c.relname FROM e7emp e JOIN pg_class c ON c.relname = 'e7emp' WHERE e.id <= 3`,
			want{has: []string{pmMaskSSN, "e7emp"}}},
		{"catalog_as_a_subquery_over_masked_data",
			`SELECT e.bal FROM e7bal e WHERE e.id IN ` +
				`(SELECT a.attnum FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'e7bal')`,
			want{has: []string{"0"}}},
	}
	answered := 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				// A refusal is a disposition the leak claim accepts; the
				// answered count below keeps the gate from passing vacuously.
				t.Logf("%s / %s: refused: %v", cell.name, door.name, err)
				continue
			}
			answered++
			var text []string
			for _, row := range got.vals {
				text = append(text, row...)
			}
			if got.vals == nil {
				for _, row := range got.rows {
					for _, v := range row {
						text = append(text, v)
					}
				}
			}
			joined := "|" + strings.Join(text, "|") + "|"
			for _, h := range cell.want.has {
				if !strings.Contains(joined, "|"+h+"|") {
					t.Errorf("%s / %s: %q missing from %v\n  %s", cell.name, door.name, h, text, cell.sql)
				}
			}
			for _, h := range cell.want.hasNot {
				if strings.Contains(joined, "|"+h+"|") {
					t.Errorf("%s / %s: %q published to an identity that may not see it: %v\n  %s",
						cell.name, door.name, h, text, cell.sql)
				}
			}
			for _, bad := range leaks {
				if strings.Contains(joined, "|"+bad+"|") {
					t.Errorf("%s / %s: the policed value %s reached the client\n  %s",
						cell.name, door.name, bad, cell.sql)
				}
			}
		}
	}
	if want := len(cells) * len(rig.doors); answered != want {
		t.Errorf("%d of %d (cell, door) pairs answered; every door answers the catalog", answered, want)
	}
}

// TestArcPCTheTwoServerDoorsSendOneMessage is #1145: the embedded server's
// pgwire door (wadjet serve) and the DAG server's (wadjetd serve) send the
// same SQLSTATE AND the same sentence for every refusal class a statement
// earns — PostgreSQL's sentence, with no stage label in front of a coded
// refusal. The embedded door prefixed `executing query: ` and doubled
// `parsing SQL: parsing SQL: ` where the DAG door did neither.
func TestArcPCTheTwoServerDoorsSendOneMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	byName := map[string]pmDoor{}
	for _, d := range rig.doors {
		byName[d.name] = d
	}
	for _, tc := range []struct {
		name, sql, state string
	}{
		{"syntax", `SELEC id FROM e7other`, "42601"},
		{"syntax_mid_statement", `SELECT id FROM e7other WHERE`, "42601"},
		{"unknown_column", `SELECT nosuch FROM e7other`, "42703"},
		{"unknown_relation", `SELECT id FROM no_such_relation_pc`, "42P01"},
		{"unknown_qualifier", `SELECT x.id FROM e7other o`, "42P01"},
		{"division_by_zero", `SELECT id / 0 FROM e7other`, "22012"},
		{"bad_integer_text", `SELECT id FROM e7other WHERE id = 'x'::int`, "22P02"},
		{"unknown_regclass", `SELECT 'no_such_relation_pc'::regclass`, "42P01"},
		{"unsupported_collation", `SELECT id FROM e7other ORDER BY note COLLATE "en_US"`, "0A000"},
		{"catalog_unknown_column", `SELECT nosuch FROM pg_catalog.pg_class`, "42703"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var codes, msgs []string
			for _, door := range []string{"pgwire/single", "pgwire/dag"} {
				_, err := byName[door].run(t, "admin-key", tc.sql)
				var pe *pgconn.PgError
				if !errors.As(err, &pe) {
					t.Fatalf("%s: %v is not a PostgreSQL error", door, err)
				}
				codes = append(codes, pe.Code)
				msgs = append(msgs, pe.Message)
			}
			if codes[0] != tc.state || codes[1] != tc.state {
				t.Errorf("SQLSTATE embedded %s, DAG %s; PostgreSQL 17.11 raises %s", codes[0], codes[1], tc.state)
			}
			if msgs[0] != msgs[1] {
				t.Errorf("the two doors send different sentences:\n  embedded %q\n  DAG      %q", msgs[0], msgs[1])
			}
			for _, m := range msgs {
				if strings.Contains(m, "parsing SQL: parsing SQL:") || strings.HasPrefix(m, "executing query: ") {
					t.Errorf("a stage label in front of a coded refusal: %q", m)
				}
			}
		})
	}
	// The embedded API's own two doors agree too (DB.Query and the
	// coordinator), since the pgwire doors are built on them.
	for _, sql := range []string{`SELECT id / 0 FROM e7other`, `SELEC 1`} {
		_, e1 := byName["embedded/single"].run(t, "admin-key", sql)
		_, e2 := byName["embedded/dag"].run(t, "admin-key", sql)
		if e1 == nil || e2 == nil || sqlerr.StateOf(e1) != sqlerr.StateOf(e2) || e1.Error() != e2.Error() {
			t.Errorf("%s: embedded/single %v, embedded/dag %v", sql, e1, e2)
		}
	}
}

// TestArcPCADerivedTableRefusalIsOneSentenceOnEveryDoor holds the
// one-sentence rule for an error raised INSIDE a parenthesised body — a
// derived table, a CTE, an IN subquery — on all nine doors: the client
// receives PostgreSQL's sentence and NOTHING ELSE. The parser used to wrap
// its own stages around a failure before Parse assigned 42601 ("parsing SQL:
// parsing WHERE: unexpected token "" at position 22"), and sqlerr.SentenceOf
// kept what was inside the coded wrapper, so every door sent the labels. The
// sentence is now chosen where the code is (the parser's syntaxError, from
// the token it stopped at), and a body's end is the ")" that closes it
// (arc PC round 3, B6). Every expected sentence is PostgreSQL 17.11's.
func TestArcPCADerivedTableRefusalIsOneSentenceOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	for _, c := range []struct{ sql, sentence string }{
		{`SELECT * FROM (SELECT id FROM e7emp WHERE) d`, `syntax error at or near ")"`},
		{`WITH c AS (SELECT id FROM e7emp WHERE) SELECT * FROM c`, `syntax error at or near ")"`},
		{`SELECT id FROM e7emp WHERE id IN (SELECT id FROM e7emp WHERE)`, `syntax error at or near ")"`},
		{`SELECT id FROM e7emp WHERE`, `syntax error at end of input`},
		{`SELECT id FROM e7emp WHERE id = 1 GARBAGE`, `syntax error at or near "GARBAGE"`},
		// This engine's 42703 sentence names the columns in scope (policy-
		// filtered, validate_policy.go) where PostgreSQL says `column
		// "nosuch" does not exist` — a recorded difference; the point here
		// is that nothing is wrapped around it.
		{`SELECT * FROM (SELECT nosuch FROM e7emp) d`, `unknown column "nosuch" (available: acct, amt, dept, id, salary, ssn)`},
		// The three #1307 sentences carry PostgreSQL's own `at or near "…"`
		// suffix now: the character that broke a pending surrogate pair, or
		// the offending escape's own source text.
		{`SELECT E'\uD83D' AS v`, `invalid Unicode surrogate pair at or near "'"`},
		{`SELECT E'\uDE00' AS v`, `invalid Unicode surrogate pair at or near "\uDE00"`},
		{`SELECT E'\U00110000' AS v`, `invalid Unicode escape value at or near "\U00110000"`},
		{`SELECT E'\u12' AS v`, `invalid Unicode escape`},
	} {
		for _, d := range rig.doors {
			_, err := d.run(t, "admin-key", c.sql)
			if err == nil {
				t.Errorf("%s answered %q; PostgreSQL refuses it", d.name, c.sql)
				continue
			}
			// A pgwire door's error is the ErrorResponse's Message field; the
			// embedded doors' is the error itself; the HTTP runner reads the
			// body's "error" text. Each must be the sentence and no more.
			m := err.Error()
			var pe *pgconn.PgError
			if errors.As(err, &pe) {
				m = pe.Message
			}
			if m != c.sentence {
				t.Errorf("%s: %q\n  sent %q\n  want PostgreSQL's %q", d.name, c.sql, m, c.sentence)
			}
		}
	}
}
