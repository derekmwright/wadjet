package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A DERIVED TABLE in FROM that references an ENCLOSING QUERY LEVEL (#614).
//
// The reference is legal SQL and PostgreSQL 17 answers it: LATERAL governs
// references to SIBLINGS in the same FROM list, not to outer levels. Measured,
// both halves:
//
//	SELECT a.n, (SELECT COUNT(*) FROM (SELECT s FROM i WHERE i.n = a.n) d)
//	FROM o a                                    -- five rows, one per a
//	SELECT COUNT(*) FROM o a, (SELECT s FROM i WHERE n = a.n) d
//	  -- ERROR: invalid reference to FROM-clause entry for table "a"
//
// This engine does not support the first, and this gate is about the DISPOSITION
// rather than the answer. Refusing is right: the logical builder plans a derived
// table's body as its own query block and the correlation analysis never reads
// its terms, so a reference let through binds INSIDE the body. Measured, with
// the binder's scope opened to let it through: the EXISTS spelling answered 5000
// of 5000 rows where 4616 match, and the scalar-subquery spelling answered one
// constant for every outer row — a loud refusal turned into a silent wrong
// answer, on every arm. That is why the shape stays refused and why supporting
// it is a dependent join (ADR-0021 §1c), not a scope widening.
//
// What WAS wrong is the class: 42P01 `missing FROM-clause entry for table "a"`
// says the query is malformed, and ADR-0021 §1i said the same in prose. Both are
// corrected: 0A000 naming the two workarounds, for the LEGAL spellings only.
func TestArcF4DerivedTableCorrelationIsRefusedNotMisread(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	// LEGAL SQL this engine does not support: the reference names a relation
	// of an OUTER QUERY LEVEL. Three spellings, because the refusal must not
	// be one subquery kind's.
	unsupported := map[string]string{
		"in-an-exists": "SELECT COUNT(*) FROM typemx a WHERE EXISTS " +
			"(SELECT 1 FROM (SELECT id, g FROM typemx b WHERE b.g = a.g) d)",
		"in-a-scalar-subquery": "SELECT a.id, (SELECT COUNT(*) FROM " +
			"(SELECT id FROM typemx b WHERE b.g = a.g) d) AS c FROM typemx a WHERE a.id < 3",
		"in-an-in-subquery": "SELECT COUNT(*) FROM typemx a WHERE a.id IN " +
			"(SELECT id FROM (SELECT id, g FROM typemx b WHERE b.g = a.g) d)",
	}
	for name, sql := range unsupported {
		t.Run("unsupported/"+name, func(t *testing.T) {
			for _, arm := range arms {
				_, _, err := arm.run(sql)
				if err == nil {
					t.Fatalf("%s arm ANSWERED a shape this engine cannot plan. Letting the "+
						"reference through binds it inside the derived table's own body, which "+
						"is a silent wrong answer — see the measurement in this file's comment\n"+
						"  SQL: %s", arm.name, sql)
				}
				if !strings.Contains(err.Error(), "is not supported") {
					t.Fatalf("%s arm: %v\n  want the UNSUPPORTED refusal (0A000), not a claim "+
						"that the SQL is malformed\n  SQL: %s", arm.name, err, sql)
				}
				// The message has to be actionable: it names the relation and
				// both workarounds.
				for _, want := range []string{`"a"`, "LATERAL", "lift the correlated predicate"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("%s arm: the refusal does not mention %q: %v", arm.name, want, err)
					}
				}
			}
		})
	}

	// A SIBLING reference without LATERAL is what LATERAL actually governs,
	// and PostgreSQL refuses it too. It keeps 42P01, so the new class cannot
	// be read as "any unresolved qualifier under a derived table".
	t.Run("sibling-reference-stays-42P01", func(t *testing.T) {
		const sql = "SELECT COUNT(*) FROM typemx a, (SELECT id FROM typemx b WHERE b.g = a.g) d"
		for _, arm := range arms {
			_, _, err := arm.run(sql)
			if err == nil {
				t.Fatalf("%s arm answered a sibling reference PostgreSQL refuses\n  SQL: %s",
					arm.name, sql)
			}
			if !strings.Contains(err.Error(), "missing FROM-clause entry") {
				t.Fatalf("%s arm: %v\n  want PostgreSQL's own 42P01 for a sibling reference\n"+
					"  SQL: %s", arm.name, err, sql)
			}
		}
	})

	// The CONTROL, and the workaround the message names: with the correlated
	// predicate written ABOVE the derived table, the same question answers on
	// every arm. `d` holds the five ids under 5, whose g values are 0..4, so
	// the outer rows that match are the five non-NULL groups 0..4.
	t.Run("ctl-the-correlation-above-the-derived-table-answers", func(t *testing.T) {
		const sql = "SELECT COUNT(*) AS n FROM typemx a WHERE EXISTS " +
			"(SELECT 1 FROM (SELECT id, g FROM typemx b WHERE b.id < 5) d WHERE d.g = a.g)"
		for _, arm := range arms {
			cols, rows, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused the workaround its own message names: %v", arm.name, err)
			}
			if got, want := e3Render(cols, rows), "n | 3297"; got != want {
				t.Fatalf("%s arm: %s, want %s", arm.name, got, want)
			}
		}
	})
}
