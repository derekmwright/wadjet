package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestSemverRowComponentsOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("five execution arms")
	}
	ctx := context.Background()
	for _, mode := range []string{"single", "spilled", "dag", "dag-shuffled", "dag-morsel"} {
		t.Run(mode, func(t *testing.T) {
			db, c := FixedRowWireArm(t, mode)
			query := `SELECT semver_parse(v) AS p, semver_major(v) AS major, semver_minor(v) AS minor, semver_patch(v) AS patch, semver_prerelease(v) AS prerelease, semver_build(v) AS build FROM semverpkg`
			var r *oracle.Result
			var err error
			if c != nil {
				before := a2fReadRoutes(c)
				r, err = tmdRunDAG(ctx, c, query)
				a2fCheckRoutes(t, mode, c, before, query)
			} else {
				r, err = tmdRunSingle(ctx, db, query)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != len(svData()) {
				t.Fatalf("rows=%d want %d", len(r.Rows), len(svData()))
			}
			for i, row := range r.Rows {
				if row["p"] == nil {
					for _, field := range []string{"major", "minor", "patch", "prerelease", "build"} {
						if row[field] != nil {
							t.Errorf("row %d NULL parse beside %s=%v", i, field, row[field])
						}
					}
					continue
				}
				p, ok := row["p"].(map[string]any)
				if !ok {
					t.Fatalf("ROW box %T", row["p"])
				}
				for _, field := range []string{"major", "minor", "patch", "prerelease", "build"} {
					if fmt.Sprint(p[field]) != fmt.Sprint(row[field]) {
						t.Errorf("row %d %s: %v != %v", i, field, p[field], row[field])
					}
				}
			}
			// The refusal is reached inside a worker on DAG arms, with no local route.
			q := `SELECT semver_parse_strict(v) AS p FROM semverpkg WHERE v='not-a-version'`
			if c != nil {
				before := a2fReadRoutes(c)
				_, err = tmdRunDAG(ctx, c, q)
				a2fCheckRoutes(t, mode, c, before, q)
			} else {
				_, err = tmdRunSingle(ctx, db, q)
			}
			if err == nil || !strings.Contains(err.Error(), "not-a-version") {
				t.Fatalf("strict refusal: %v", err)
			}
			checkSemverNullDecl(t, ctx, db)
		})
	}
}
func checkSemverNullDecl(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	r, e := db.Query(ctx, `SELECT semver_parse(NULL) AS p, semver_parse_strict(NULL) AS s`)
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range r.ColumnMetas {
		if m.TypeID != parquet.TypeRow || len(m.Fields) != 5 {
			t.Errorf("NULL declaration %+v", m)
		}
	}
	if r.Rows[0]["p"] != nil || r.Rows[0]["s"] != nil {
		t.Fatal(r.Rows)
	}
}
