package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The DML doors, asked ADR-0022 rule 1's question.
//
// The round-4 review's survey of the remaining qualifier-strip sites outside
// `batch.RowFieldPath` left two UNTESTED rather than cleared — `wadjet/dml.go`'s
// `splitQualifiedCol` and the SET-clause target's own `strings.LastIndex(col,
// ".")` — because the reviewer could not build a colliding fixture through the
// probe harness. This drives them.
//
// The answer is that neither can produce a wrong answer, and the reason is not
// luck: they name a WRITE TARGET, not a value reference. A field path cannot
// reach them at all — `SET c_row.b = 1` does not parse and a MERGE ON keyed on
// one is refused at binding — while the PREDICATE door, which is a value
// reference, resolves the field the way every other reader does. Both halves
// are asserted, because "it refuses" and "it answers correctly" are different
// claims and the file is one edit away from either becoming false.
//
// One arm on purpose: DML is single-writer and each case MUTATES the fixture,
// so the standalone engine is the whole of the door. The four-arm gates cover
// the SELECT face of the same references.
func TestRowFieldPathThroughTheDMLDoors(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate loads the type-matrix fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	nested := typematrix.Nested

	// --- the PREDICATE door: a value reference, and it must ANSWER ---
	for _, tc := range []struct {
		name, probe, dml string
		wantTag          string
	}{
		{
			// `c_row.b = 44` selects exactly one row of the fixture, so an
			// UPDATE through the same predicate must report exactly one. A
			// qualifier-strip here would look for a column `b`, which
			// typemx_nested does not have — the shape that answered a join
			// arm's column in #769 and a whole cross product in the join key.
			name:    "update-where-a-field-path",
			probe:   "SELECT COUNT(*) AS c FROM " + nested + " WHERE c_row.b = 44",
			dml:     "UPDATE " + nested + " SET id = id + 100000 WHERE c_row.b = 44",
			wantTag: "UPDATE 1",
		},
		{
			name:    "delete-where-a-field-path",
			probe:   "SELECT COUNT(*) AS c FROM " + nested + " WHERE c_row.b = 55",
			dml:     "DELETE FROM " + nested + " WHERE c_row.b = 55",
			wantTag: "DELETE 1",
		},
		{
			// PostgreSQL's own spelling reaches the same door.
			name:    "delete-where-a-parenthesised-field-path",
			probe:   "SELECT COUNT(*) AS c FROM " + nested + " WHERE (c_row).b = 66",
			dml:     "DELETE FROM " + nested + " WHERE (c_row).b = 66",
			wantTag: "DELETE 1",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := tmdStandalone(t, ctx)
			res, err := db.Query(ctx, tc.probe)
			if err != nil {
				t.Fatalf("probe: %v", err)
			}
			if len(res.Rows) != 1 || res.Rows[0]["c"] != int64(1) {
				t.Fatalf("the probe selects %v, want exactly one row — the fixture moved and "+
					"the DML assertion below no longer means anything\n  SQL: %s",
					res.Rows, tc.probe)
			}
			res, err = db.Query(ctx, tc.dml)
			if err != nil {
				t.Fatalf("%s: %v", tc.dml, err)
			}
			got, _ := res.Rows[0]["result"].(string)
			if got != tc.wantTag {
				t.Errorf("the DML door reports %q, want %q — the predicate selects one row "+
					"and the statement must touch exactly that one\n  SQL: %s",
					got, tc.wantTag, tc.dml)
			}
		})
	}

	// --- the TARGET doors: a write target, and they must REFUSE ---
	for _, tc := range []struct{ name, sql, refuse string }{
		{
			// `wadjet/dml.go`'s SET-clause target strips a qualifier to name
			// the column it writes. A field path never gets there: assigning
			// INTO a container's field is not a statement this parser reads,
			// and it says so rather than writing somewhere else.
			name:   "set-a-container-field",
			sql:    "UPDATE " + nested + " SET c_row.b = 1 WHERE id = 1",
			refuse: `expected = after column c_row`,
		},
		{
			// `splitQualifiedCol`, through MERGE's ON keys. The binder refuses
			// the reference before the split is reached, with the message
			// PostgreSQL gives for the unparenthesised spelling.
			name:   "merge-on-a-field-path",
			sql:    "MERGE INTO " + nested + " t USING decpair s ON c_row.b = s.b WHEN MATCHED THEN UPDATE SET id = t.id",
			refuse: `missing FROM-clause entry for table "c_row"`,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := tmdStandalone(t, ctx)
			res, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("the door ANSWERED %v; this gate asserts a refusal, and a write "+
					"target reached through a qualifier strip is the one thing it must not "+
					"silently accept\n  SQL: %s", res.Rows, tc.sql)
			}
			if !strings.Contains(err.Error(), tc.refuse) {
				t.Errorf("refused with %q, want a refusal carrying %q\n  SQL: %s",
					err.Error(), tc.refuse, tc.sql)
			}
		})
	}
}
