// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC TF round 2 / N10 — A UNARY SIGN IS A NUMBER, WHATEVER ITS OPERAND.
//
// `SELECT a FROM read_json('s.json') ORDER BY -a` answered 1;2;3 where
// PostgreSQL 17.11 answers 3;2;1 — a wrong ORDER, silently. The declaration
// walk types a unary ± from its OPERAND, and a relation with no plan-time
// column types (a file or database reader) leaves the operand undecided, so
// the item fell to the STRING fallback: the hidden sort key materialized as
// TEXT and "-1" < "-2" < "-3" is ascending by `a`.
//
// The discriminator is the RELATION, not the term, and it is in the table
// below: a catalog table and `generate_series` both sort by the negation, and
// so do `0 - a` and `a * -1` over the reader — because a BINARY arithmetic
// node over an undecided operand already declares a number. One rule for the
// sign, whichever way it is written (round-1 review, N10).
func TestArcTFANegatedSortTermSortsByTheNegation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "s.json")
	if err := os.WriteFile(jsonPath, []byte("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	csvPath := filepath.Join(dir, "s.csv")
	if err := os.WriteFile(csvPath, []byte("a\n1\n2\n3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE tfneg (a BIGINT)`,
		`INSERT INTO tfneg VALUES (1),(2),(3)`,
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	rj := "read_json('" + jsonPath + "')"
	rc := "read_csv('" + csvPath + "')"

	for _, c := range []struct{ name, sql, want, pg string }{
		// ---- the reader, which sorted the other way ----------------------
		{"a_negated_key_over_read_json",
			`SELECT a FROM ` + rj + ` ORDER BY -a`, "[a] 3;2;1", "3;2;1"},
		{"a_negated_key_over_read_csv",
			`SELECT a FROM ` + rc + ` ORDER BY -a`, "[a] 3;2;1", "3;2;1"},
		{"a_parenthesised_negated_key_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY (-a)`, "[a] 3;2;1", "3;2;1"},
		{"a_negated_key_with_desc_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY -a DESC`, "[a] 1;2;3", "1;2;3"},
		{"a_unary_plus_key_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY +a DESC`, "[a] 3;2;1", "3;2;1"},
		{"a_negated_key_in_the_select_list_over_a_reader",
			`SELECT -a AS v FROM ` + rj + ` ORDER BY v`, "[v] -3;-2;-1", "-3;-2;-1"},
		// ---- the same term over the relations that already sorted right ---
		{"control_a_catalog_table",
			`SELECT a FROM tfneg ORDER BY -a`, "[a] 3;2;1", "3;2;1"},
		{"control_a_declared_table_function",
			`SELECT x FROM generate_series(1,3) gs(x) ORDER BY -x`, "[x] 3;2;1", "3;2;1"},
		// ---- the two spellings that were already right over a reader ------
		{"control_a_subtraction_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY 0 - a`, "[a] 3;2;1", "3;2;1"},
		{"control_a_multiplication_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY a * -1`, "[a] 3;2;1", "3;2;1"},
		{"control_an_addition_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY a + 1 DESC`, "[a] 3;2;1", "3;2;1"},
		// ---- a negated key must not make an UNRELATED order wrong ---------
		{"control_a_plain_key_over_a_reader",
			`SELECT a FROM ` + rj + ` ORDER BY a DESC`, "[a] 3;2;1", "3;2;1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers %s: %v\n  SQL: %s",
					c.pg, err, c.sql)
			}
			if got != c.want {
				t.Errorf("answered\n  got  %s\n  want %s (PostgreSQL 17.11: %s)\n  SQL: %s",
					got, c.want, c.pg, c.sql)
			}
		})
	}
}
