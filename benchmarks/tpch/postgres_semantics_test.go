package tpch

import (
	"strings"
	"testing"
)

// TestPostgresOracleIsConfiguredForByteCollation guards the one configuration
// choice that decides whether the PostgreSQL arm compares the same question on
// both sides.
//
// Wadjet compares strings by BYTES. PostgreSQL compares them by the database's
// collation, and on a default install that is a locale collation where 'a' <
// 'B'. Neither engine is wrong; they answer different questions. The oracle is
// therefore configured into byte ordering — initdb --locale=C for the container
// it starts, and COLLATE "C" on every text column of the fixture so a
// caller-supplied database is carried too.
//
// Why this needs a test rather than a comment: the alternative to configuring
// the oracle is EXEMPTING the string-ordering entries, and an exemption is
// invisible once written. Drop the collation and nothing fails loudly — the
// string ORDER BY entries start failing for a reason that has nothing to do
// with Wadjet, somebody exempts them, and from then on the arm is blind to
// every real ordering bug in the same queries. That is the shape of the
// default_null_order decision on the DuckDB arm
// (TestOracleIsConfiguredForPostgresSemantics), which exists for exactly this
// reason and which this test is the sibling of.
//
// It runs with no server, which is the point: CI has no PostgreSQL and the
// premise still has to be asserted somewhere.
func TestPostgresOracleIsConfiguredForByteCollation(t *testing.T) {
	src := readTestSource(t, "postgres_oracle_test.go")

	for _, want := range []struct {
		needle string
		why    string
	}{
		{`--locale=C`,
			"the container the harness starts must be initdb'd with the C locale, so byte ordering is the " +
				"database-wide default and not only the fixture columns'"},
		{`postgresCollation = ` + "`" + `"C"` + "`",
			"the collation the fixture's text columns declare must be C, so a caller-supplied en_US database " +
				"still compares by bytes"},
		{`text COLLATE " + postgresCollation`,
			"every TEXT column of the fixture must carry that collation; a column that does not is compared " +
				"under the server's default and reports Wadjet's correct byte ordering as a divergence"},
	} {
		if !strings.Contains(src, want.needle) {
			t.Errorf("postgres_oracle_test.go no longer contains %q.\n%s\n"+
				"If the collation decision itself changed, change it in the engine, in this test, and in the "+
				"harness together, and say so in the commit. Exempting the string-ordering entries instead is "+
				"what this test exists to prevent.", want.needle, want.why)
		}
	}

	// The other half: the arm must never grow a stored baseline. A stored
	// answer is what let a wrong one masquerade as ground truth twice in this
	// repository (the Q05 DuckDB baseline, baseline-local-small.json), and the
	// wire arm has nothing a file could stand in for anyway.
	if strings.Contains(src, "baseline") && strings.Contains(src, "os.WriteFile") {
		t.Error("postgres_oracle_test.go looks like it writes a stored baseline. Both arms read PostgreSQL " +
			"LIVE, in the same process invocation that reads Wadjet; a file recorded from one run cannot be " +
			"checked against the engine it claims to come from, and the wire arm's subject (OIDs, format " +
			"codes, SQLSTATE) is not something a fingerprint file holds.")
	}
}

// TestPostgresOracleCorpusPinsAreAccountable keeps every exemption traceable.
// It needs no server: the corpora are pure functions, which is what lets this
// run in CI where the oracle itself skips.
//
// A pin is the one thing in this suite that can hide a defect, so it carries
// its own rules: it says WHICH KIND of divergence it is, and — for a Wadjet
// defect — WHERE the fix is tracked. Without that, "knownBug" degrades into a
// shrug and the corpus quietly stops being a gate.
func TestPostgresOracleCorpusPinsAreAccountable(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range postgresCorpus() {
		if seen[c.name] {
			t.Errorf("two corpus entries are named %q; the second shadows the first in the report", c.name)
		}
		seen[c.name] = true
		if c.knownBug == "" {
			if c.issue != "" {
				t.Errorf("%s names issue %s but is not pinned — an entry that agrees needs no issue reference",
					c.name, c.issue)
			}
			continue
		}
		switch {
		case strings.HasPrefix(c.knownBug, pgBugWadjet):
			if c.issue == "" {
				t.Errorf("%s is pinned as a Wadjet bug with no issue reference. A pin with no tracker entry "+
					"is an exemption nobody owns; file the issue and name it here.", c.name)
			}
		case strings.HasPrefix(c.knownBug, pgBugUnsupported):
			// An unimplemented spelling may be pinned without an issue — an
			// error is an acceptable answer — but an issue is preferred.
		case strings.HasPrefix(c.knownBug, pgDivergenceCarrier):
			// A DELIBERATE carrier divergence is not a bug and has no fix to
			// track, so it does not need an issue — but it does need the
			// RECORD that decided it, or "deliberate" is just a shrug with a
			// prefix. The reason text must name the ADR.
			if !strings.Contains(c.knownBug, "ADR-") {
				t.Errorf("%s is pinned as a deliberate divergence but names no ADR. A deliberate "+
					"divergence that cites no record is an exemption nobody decided; name the "+
					"record that did.", c.name)
			}
		default:
			t.Errorf("%s has a knownBug that opens with neither %q nor %q, so the divergence's KIND is not "+
				"stated: %.60s...", c.name, pgBugWadjet, pgBugUnsupported, c.knownBug)
		}
	}

	// The wire arm pins per PROPERTY. A pin naming a property the comparison
	// never produces would sit there forever looking like coverage.
	props := map[string]bool{
		wirePropFieldCount: true, wirePropFieldNames: true, wirePropTypeOIDs: true,
		wirePropTypeSizes: true, wirePropTypeMods: true, wirePropTextFormat: true,
		wirePropBinFormat: true, wirePropValuesText: true, wirePropFloatRender: true,
		wirePropNullRep: true, wirePropBinaryDecode: true, wirePropCommandTag: true,
		wirePropParamOIDs: true, wirePropSQLState: true,
	}
	for _, c := range wireCorpus() {
		for prop, reason := range c.pins {
			if !props[prop] {
				t.Errorf("wire entry %s pins the property %q, which nothing compares — the pin can never be "+
					"retired because it can never be contradicted", c.name, prop)
			}
			if reason == "" {
				t.Errorf("wire entry %s pins %q with no reason", c.name, prop)
			}
		}
	}
}

// TestPostgresOracleSkipsWithoutAServer documents, and checks, the behaviour CI
// depends on: with no DSN and no runnable image the suite SKIPS rather than
// failing. The check is on the source because the skip path is only reachable
// when there is genuinely no server, which is not a state a test can create on
// a developer machine that has one.
func TestPostgresOracleSkipsWithoutAServer(t *testing.T) {
	src := readTestSource(t, "postgres_oracle_test.go")
	if !strings.Contains(src, "t.Skipf(") {
		t.Error("postgresOracleDSN no longer skips when no PostgreSQL is reachable. CI has neither a server " +
			"nor the container image, exactly as it has no /tmp/duckdb; a hard failure there would make the " +
			"whole benchmarks/tpch package red for a missing dependency.")
	}
	if !strings.Contains(src, `exec.Command("docker", "image", "inspect"`) {
		t.Error("startPostgresContainer no longer checks the image is present before running it. The image " +
			"must never be PULLED by a test: a 400 MB download is not something `go test` should decide to " +
			"do, and CI would take it every run only to throw it away.")
	}
}
