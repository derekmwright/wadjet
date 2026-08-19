package tpch

import (
	"os"
	"strings"
	"testing"
)

// TestOracleIsConfiguredForPostgresSemantics guards the configuration that
// makes the DuckDB differential gate mean what it claims.
//
// Wadjet follows PostgreSQL for default NULL placement — NULLS LAST for ASC,
// NULLS FIRST for DESC — because it ships the PostgreSQL wire protocol and a
// psql/DataGrip/Superset client expects that. DuckDB defaults the other way.
// So the oracle is run with default_null_order set to PostgreSQL's rule, and
// the whole corpus is compared against a DuckDB configured to agree.
//
// Why this needs a test rather than a comment: the stored baseline is
// REGENERATED from whatever the oracle says, by anyone running
// WADJET_REGENERATE_DUCKDB_BASELINE=1. Drop the SET and nothing fails — the
// next regeneration quietly re-pins every ordering answer to DuckDB's default,
// and from then on wadjet's correct PostgreSQL behavior reads as the
// divergence. A later "fix" to make the engine match would be verified green
// by a gate that had moved underneath it.
//
// That is the same shape as the poisoned baseline-local-small.json (2026-08-18),
// where a value signature captured from a broken engine meant a CORRECT engine
// failed our own gate. A baseline regenerated from a wrong premise is
// indistinguishable from one regenerated from a right premise, so the premise
// has to be asserted somewhere the regeneration cannot rewrite.
func TestOracleIsConfiguredForPostgresSemantics(t *testing.T) {
	// The setting the oracle must carry, and what it means.
	const pgNullOrder = "nulls_last_on_asc_first_on_desc"

	for _, path := range []string{"duckdb_compare_test.go", "shape_fuzz_test.go"} {
		src := readTestSource(t, path)
		if !strings.Contains(src, pgNullOrder) {
			t.Errorf("%s no longer sets default_null_order=%q on the DuckDB oracle.\n"+
				"Wadjet follows PostgreSQL's NULL placement (NULLS FIRST on DESC) because it "+
				"speaks the PostgreSQL wire protocol; DuckDB defaults to NULLS LAST in both "+
				"directions. Without this setting the oracle answers a different question, and "+
				"the next baseline regeneration silently re-pins every ordering entry to "+
				"DuckDB's rule — after which the engine's CORRECT behavior looks like the bug.\n"+
				"If the semantics decision itself changed, update resolveNullsLast, "+
				"SortKeySpec.PlaceNullsLast and this test together, and say so in the commit.",
				path, pgNullOrder)
		}
	}
}

// readTestSource reads a source file from this package. The guard above works
// on the source text because the setting is a literal in a command script — it
// is not reachable as a value from a test without running DuckDB itself, and
// this check has to hold even where the binary is absent (CI).
func readTestSource(tb testing.TB, name string) string {
	tb.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		tb.Fatalf("reading %s: %v", name, err)
	}
	return string(b)
}
