package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// aliasServerSchema and aliasServerRows are the same three-row fixture the
// embedded arm uses (wadjet.TestDMLTableAliasMatchesPostgres), so the two
// doors are compared against the same PostgreSQL answers.
var aliasServerSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	{Name: "name", Type: parquet.TypeString, Nullable: true},
}}

func aliasServerRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "n": int64(10), "name": "a"},
		{"id": int64(2), "n": int64(20), "name": "b"},
		{"id": int64(3), "n": int64(30), "name": "c"},
	}
}

// The HTTP DML executors are a second copy of the embedded ones, with their
// own call into BuildDMLPredicate — so the aliased DELETE emptied a table
// through this door too (#686). Same statements, same PostgreSQL answers.
func TestServerDMLTableAliasMatchesPostgres(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		count int64
		state string
		rows  []string
	}{
		{name: "DELETE AS alias", sql: "DELETE FROM sa686 AS a WHERE a.id = 1",
			count: 1, rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE bare alias", sql: "DELETE FROM sa686 a WHERE a.id = 1",
			count: 1, rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE AS alias, unqualified WHERE", sql: "DELETE FROM sa686 AS a WHERE id = 1",
			count: 1, rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE schema-qualified", sql: "DELETE FROM public.sa686 AS a WHERE a.id = 1",
			count: 1, rows: []string{"2:20:b", "3:30:c"}},
		{name: "DELETE aliased, table-qualified WHERE", sql: "DELETE FROM sa686 AS a WHERE sa686.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE aliased, unknown column", sql: "DELETE FROM sa686 AS a WHERE a.nosuch = 1",
			state: "42703", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "DELETE unknown schema", sql: "DELETE FROM nosuchschema.sa686 WHERE id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE AS alias", sql: "UPDATE sa686 AS a SET n = 99 WHERE a.id = 1",
			count: 1, rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE bare alias", sql: "UPDATE sa686 a SET n = 99 WHERE a.id = 1",
			count: 1, rows: []string{"1:99:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE AS alias, SET reads the alias", sql: "UPDATE sa686 AS a SET n = a.n + 1 WHERE a.id = 1",
			count: 1, rows: []string{"1:11:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, table-qualified WHERE", sql: "UPDATE sa686 AS a SET n = 99 WHERE sa686.id = 1",
			state: "42P01", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
		{name: "UPDATE aliased, unknown SET target", sql: "UPDATE sa686 AS a SET nosuch = 9 WHERE a.id = 1",
			state: "42703", rows: []string{"1:10:a", "2:20:b", "3:30:c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cat, filePath := nestServerDMLSetup(t, "sa686", aliasServerSchema, aliasServerRows())

			parsed, err := plansql.Parse(tc.sql)
			var res *dmlResult
			if err == nil {
				switch {
				case parsed.Delete != nil:
					res, err = executeDMLDelete(ctx, cat, parsed.Delete)
				case parsed.Update != nil:
					res, err = executeDMLUpdate(ctx, cat, parsed.Update)
				default:
					t.Fatalf("%q is neither a DELETE nor an UPDATE", tc.sql)
				}
			}

			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s affected %d rows; PostgreSQL refuses it with %s",
						tc.sql, res.rowsAffected, tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
			} else {
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if res.rowsAffected != tc.count {
					t.Errorf("%s affected %d rows, want %d", tc.sql, res.rowsAffected, tc.count)
				}
			}
			if got := aliasServerRowSet(t, cat, filePath); strings.Join(got, " ") != strings.Join(tc.rows, " ") {
				t.Errorf("%s left the table as %v, want %v", tc.sql, got, tc.rows)
			}
		})
	}
}

// The empty-predicate backstop on the HTTP door: a DELETE or an UPDATE that
// WRITES a WHERE beside a clause that parsed to nothing is refused, and the
// table is untouched.
func TestServerDMLRefusesAWhereThatParsedToNothing(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(cat *catalog.Catalog) error
	}{
		{"DELETE", func(cat *catalog.Catalog) error {
			_, err := executeDMLDelete(ctx, cat, &plansql.DeleteInfo{DMLTarget: plansql.DMLTarget{
				Table:   "sa686",
				StmtSQL: "DELETE FROM sa686 AS a WHERE a.id = 1",
			}})
			return err
		}},
		{"UPDATE", func(cat *catalog.Catalog) error {
			_, err := executeDMLUpdate(ctx, cat, &plansql.UpdateInfo{
				DMLTarget: plansql.DMLTarget{
					Table:   "sa686",
					StmtSQL: "UPDATE sa686 AS a SET n = 99 WHERE a.id = 1",
				},
				SetClauses: []plansql.SetClause{{Column: "n", Value: "99"}},
			})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, filePath := nestServerDMLSetup(t, "sa686", aliasServerSchema, aliasServerRows())
			before := aliasServerRowSet(t, cat, filePath)
			err := tc.run(cat)
			if err == nil {
				t.Fatalf("a %s whose WHERE parsed to nothing ran; the table is now %v",
					tc.name, aliasServerRowSet(t, cat, filePath))
			}
			if got := sqlerr.StateOf(err); got != "XX000" {
				t.Errorf("SQLSTATE %q, want XX000 (err: %v)", got, err)
			}
			if after := aliasServerRowSet(t, cat, filePath); strings.Join(after, " ") != strings.Join(before, " ") {
				t.Errorf("the refused %s changed the table: %v -> %v", tc.name, before, after)
			}
		})
	}
}

func aliasServerRowSet(t *testing.T, cat *catalog.Catalog, originalFile string) []string {
	t.Helper()
	all := nestServerAllRowsAfterUpdate(t, cat, "sa686", originalFile, aliasServerSchema)
	out := make([]string, 0, len(all))
	for _, r := range all {
		out = append(out, fmt.Sprintf("%v:%v:%v", r["id"], r["n"], r["name"]))
	}
	sort.Strings(out)
	return out
}
