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

func TestTCPFlagValidationDoors(t *testing.T) {
	for _, door := range []string{"catalogless", "policy", "unparseable_order"} {
		for _, distributed := range []bool{false, true} {
			t.Run(door+map[bool]string{false: "/Plan", true: "/PlanDistributed"}[distributed], func(t *testing.T) {
				ctx := context.Background()
				p := NewPlanner(nil)
				q := `SELECT tcp_flag_mask('BOGUS') AS v WHERE FALSE`
				if door != "catalogless" {
					cat, _ := setupCatalog(t)
					p = NewPlanner(cat)
					if err := cat.CreateTable(ctx, "empty_flags", parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt32}}}, nil); err != nil {
						t.Fatal(err)
					}
					q = `SELECT id FROM empty_flags WHERE FALSE`
				}
				parsed, err := plansql.Parse(q)
				if err != nil {
					t.Fatal(err)
				}
				if door == "unparseable_order" {
					parsed.SelectInfo.OrderBy = []plansql.OrderByItem{{Column: `tcp_flag_mask('BOGUS') +`}}
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
					n = logical.InjectRowFilter(n, "empty_flags", `tcp_flag_mask('BOGUS')=1`)
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
