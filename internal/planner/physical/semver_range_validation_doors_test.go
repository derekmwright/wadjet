package physical

import (
	"context"
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE DOORS A CONSTANT SEMVER RANGE IS REFUSED THROUGH (#967).
//
// The twin of TestTCPFlagValidationDoors, over the same three doors and both
// planning paths, because the two refusals ride the same two layers: the
// binder decides, and `expr.compileFuncCallNamed` is the backstop for what the
// binder does not see. `WHERE FALSE` is the whole point — a range that names
// no range must be an error with NO ROWS AT ALL, or whether a typo is an error
// depends on the data.
func TestSemverRangeValidationDoors(t *testing.T) {
	for _, door := range []string{"catalogless", "policy", "unparseable_order"} {
		for _, distributed := range []bool{false, true} {
			t.Run(door+map[bool]string{false: "/Plan", true: "/PlanDistributed"}[distributed], func(t *testing.T) {
				ctx := context.Background()
				p := NewPlanner(nil)
				q := `SELECT semver_satisfies('1.2.3','^^1.0') AS v WHERE FALSE`
				if door != "catalogless" {
					cat, _ := setupCatalog(t)
					p = NewPlanner(cat)
					if err := cat.CreateTable(ctx, "empty_pkgs", parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt32}}}, nil); err != nil {
						t.Fatal(err)
					}
					q = `SELECT id FROM empty_pkgs WHERE FALSE`
				}
				parsed, err := plansql.Parse(q)
				if err != nil {
					t.Fatal(err)
				}
				if door == "unparseable_order" {
					parsed.SelectInfo.OrderBy = []plansql.OrderByItem{{Column: `semver_satisfies('1.2.3','^^1.0') +`}}
				}
				if err = p.ValidateColumns(ctx, parsed.SelectInfo); err != nil {
					t.Fatal(err)
				}
				n, err := logical.BuildFromSelect(parsed.SelectInfo)
				if door == "unparseable_order" {
					if sqlerr.StateOf(err) != "42601" {
						t.Fatalf("unparseable ORDER BY: %v (%s)", err, sqlerr.StateOf(err))
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if door == "policy" {
					n = logical.InjectRowFilter(n, "empty_pkgs", `semver_satisfies('1.2.3','^^1.0')`)
				}
				if distributed {
					_, err = p.PlanDistributed(ctx, n)
				} else {
					_, err = p.Plan(ctx, n)
				}
				switch {
				case door == "catalogless" && distributed:
					if !errors.Is(err, ErrTableLessSelectDistributed) {
						t.Fatalf("catalogless DAG refusal changed: %v", err)
					}
				case door == "policy" && distributed:
					// The same pin the flag family records: a policy filter on
					// an empty distributed stage may never compile.
					if err != nil {
						t.Fatalf("policy DAG pin changed: %v", err)
					}
				default:
					if sqlerr.StateOf(err) != "22023" {
						t.Fatalf("want 22023, got %v", err)
					}
				}
				t.Logf("door=%s distributed=%t state=%s err=%v", door, distributed, sqlerr.StateOf(err), err)
			})
		}
	}
}

// THE BINDER REFUSES A CONSTANT RANGE IN EVERY EXPRESSION POSITION, over an
// input that reaches NO ROWS.
//
// The positions are the ones #1018 round 6 measured as the DAG's blind spots —
// HAVING, an ORDER BY key, a set-operation arm, a projection above a GROUP BY,
// a subquery body, a window argument — asked here of the binder, which both
// planning paths reach before any stage exists.
func TestTheBinderRefusesAConstantSemverRangeInEveryExpressionPosition(t *testing.T) {
	ctx := context.Background()
	cat, _ := setupCatalog(t)
	p := NewPlanner(cat)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "pkgs", schema, nil); err != nil {
		t.Fatal(err)
	}
	const bad = `'^^1.0'`
	for _, tc := range []struct{ name, sql string }{
		{"select_item", `SELECT semver_satisfies(v, ` + bad + `) AS s FROM pkgs WHERE id < 0`},
		{"where", `SELECT id FROM pkgs WHERE id < 0 AND semver_satisfies(v, ` + bad + `)`},
		{"having", `SELECT id FROM pkgs WHERE id < 0 GROUP BY id HAVING semver_satisfies(MIN(v), ` + bad + `)`},
		{"order_by", `SELECT id FROM pkgs WHERE id < 0 ORDER BY semver_satisfies(v, ` + bad + `)`},
		{"group_by", `SELECT COUNT(*) AS n FROM pkgs WHERE id < 0 GROUP BY semver_satisfies(v, ` + bad + `)`},
		{"projection_above_group_by", `SELECT semver_satisfies(MIN(v), ` + bad + `) AS s FROM pkgs WHERE id < 0 GROUP BY id`},
		{"union_arm", `SELECT id FROM pkgs WHERE id < 0 UNION ALL SELECT id FROM pkgs WHERE semver_satisfies(v, ` + bad + `)`},
		{"derived_body", `SELECT s FROM (SELECT semver_satisfies(v, ` + bad + `) AS s FROM pkgs WHERE id < 0) d`},
		{"cte_body", `WITH c AS (SELECT semver_satisfies(v, ` + bad + `) AS s FROM pkgs WHERE id < 0) SELECT s FROM c`},
		{"window_argument", `SELECT COUNT(semver_satisfies(v, ` + bad + `)) OVER () AS s FROM pkgs WHERE id < 0`},
		{"join_on", `SELECT a.id FROM pkgs a JOIN pkgs b ON a.id = b.id AND semver_satisfies(a.v, ` + bad + `) WHERE a.id < 0`},
		{"case_when", `SELECT CASE WHEN semver_satisfies(v, ` + bad + `) THEN 1 ELSE 0 END AS s FROM pkgs WHERE id < 0`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			err = p.ValidateColumns(ctx, parsed.SelectInfo)
			if sqlerr.StateOf(err) != "22023" {
				t.Fatalf("the binder answered %v (%s); a constant range that names no range "+
					"is refused from the declaration, rows or no rows",
					err, sqlerr.StateOf(err))
			}
		})
	}
}

// AND IT REFUSES NOTHING IT CANNOT SEE. A range that is a COLUMN, an
// expression or a NULL literal is not a constant this layer can fold, and a
// refusal made on a guess is the false positive the binder's standing contract
// forbids.
func TestTheBinderLeavesANonConstantSemverRangeAlone(t *testing.T) {
	ctx := context.Background()
	cat, _ := setupCatalog(t)
	p := NewPlanner(cat)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
		{Name: "r", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "pkgs2", schema, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"range_is_a_column", `SELECT semver_satisfies(v, r) AS s FROM pkgs2`},
		{"range_is_an_expression", `SELECT semver_satisfies(v, CONCAT('^', r)) AS s FROM pkgs2`},
		{"range_is_null", `SELECT semver_satisfies(v, NULL) AS s FROM pkgs2`},
		{"the_range_is_valid", `SELECT semver_satisfies(v, '^1.2.3') AS s FROM pkgs2`},
		{"a_parenthesised_valid_range", `SELECT semver_satisfies(v, ('^1.2.3')) AS s FROM pkgs2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if err := p.ValidateColumns(ctx, parsed.SelectInfo); err != nil {
				t.Fatalf("the binder refused a range it cannot see: %v", err)
			}
		})
	}
	// A PARENTHESISED constant, on the other hand, IS one it can see.
	parsed, err := plansql.Parse(`SELECT semver_satisfies(v, ('^^1.0')) AS s FROM pkgs2`)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateColumns(ctx, parsed.SelectInfo); sqlerr.StateOf(err) != "22023" {
		t.Fatalf("a parenthesised constant range answered %v (%s), want 22023", err, sqlerr.StateOf(err))
	}
}
