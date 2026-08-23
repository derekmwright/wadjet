package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #416: a zero-row result declared no column TYPES on any coordinator route,
// and no column NAMES at all on the correlated-local one.
//
// Wadjet derived a result's schema from DATA FLOW — CollectSink captured it
// from the first batch it consumed, exec.Project resolved its types from the
// first batch it saw — so a query that eliminates every row had no schema to
// report. pgwire's coord path then declared OID 25 (text) for every column
// (coordColumnMetas returns nil, sendRowDescription's fallback fires), while
// the same statement through the embedded API declared real OIDs: the two
// entry points disagreed on an empty result. On the correlated-local route,
// which builds Columns FROM sink.Schema(), the client got a RowDescription
// with ZERO FIELDS — not an empty table with headers, no table at all.
//
// The assertion is an INVARIANT rather than a table of expected types: the
// same query with a predicate that matches and one that matches nothing must
// declare the same columns. Nothing is hardcoded, so it cannot drift from
// what the engine actually emits for a non-empty result — which is the
// property a client depends on.
func TestEmptyResultDeclaresPlanSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	// LocalFastPathBytes=0: every query below takes the stage DAG unless the
	// planner refuses it, which is what routes the correlated case to the
	// coordinator-local pipeline.
	coord := tmdCluster(t, ctx)

	cases := []struct {
		name string
		// tmpl carries one %s, the WHERE predicate.
		tmpl string
		// correlated marks the shape that must route to the local pipeline.
		correlated bool
	}{
		{
			name: "projection",
			tmpl: `SELECT id, c_str, c_dec FROM ` + typematrix.Table + ` WHERE %s`,
		},
		{
			name: "projection_ordered",
			tmpl: `SELECT id, c_str, c_dec FROM ` + typematrix.Table + ` WHERE %s ORDER BY id`,
		},
		{
			name: "grouped_aggregate",
			tmpl: `SELECT g AS grp, COUNT(*) AS n, MIN(c_i64) AS lo FROM ` + typematrix.Table +
				` WHERE %s GROUP BY g ORDER BY grp`,
		},
		{
			// A correlated scalar subquery in the SELECT LIST: the shape the
			// distributed planner refuses, which is what routes it to the
			// coordinator-local pipeline.
			name: "correlated_scalar_subquery",
			tmpl: `SELECT t0.id AS id, t0.c_i64 AS v, ` +
				`(SELECT AVG(t2.m2) FROM ` + typematrix.Table + ` t2 WHERE t2.g = t0.g) AS avg_m2 ` +
				`FROM ` + typematrix.Table + ` t0 WHERE %s`,
			correlated: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			before := coord.CorrelatedLocalRoutes()
			full, err := coord.ExecuteSQL(ctx, fmt.Sprintf(tc.tmpl, "id < 200"))
			if err != nil {
				t.Fatalf("non-empty arm: %v", err)
			}
			if full.TotalRows == 0 {
				t.Fatalf("the non-empty arm returned no rows; the reference is meaningless")
			}
			empty, err := coord.ExecuteSQL(ctx, fmt.Sprintf(tc.tmpl, "id < 0"))
			if err != nil {
				t.Fatalf("empty arm: %v", err)
			}
			if empty.TotalRows != 0 {
				t.Fatalf("the empty arm returned %d rows", empty.TotalRows)
			}
			if tc.correlated && coord.CorrelatedLocalRoutes() == before {
				t.Fatalf("this shape no longer routes to the coordinator-local pipeline, " +
					"so the route #416 left with NO COLUMNS AT ALL is not covered any more")
			}

			if len(empty.Columns) == 0 {
				t.Fatalf("empty result declares no columns at all; the non-empty one declares %v",
					full.Columns)
			}
			if strings.Join(empty.Columns, ",") != strings.Join(full.Columns, ",") {
				t.Errorf("column NAMES differ between the empty and non-empty arms:\n"+
					" empty %v\n full  %v", empty.Columns, full.Columns)
			}
			gotSchema, wantSchema := empty.OutputSchema(), full.OutputSchema()
			if len(gotSchema) == 0 {
				t.Fatalf("empty result declares no column TYPES; pgwire falls back to "+
					"OID 25 (text) for all of %v", empty.Columns)
			}
			if got, want := describeSchema(gotSchema), describeSchema(wantSchema); got != want {
				t.Errorf("column TYPES differ between the empty and non-empty arms:\n"+
					" empty %s\n full  %s", got, want)
			}
		})
	}
}

func describeSchema(cols []parquet.Column) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.Name + ":" + c.Type.String()
	}
	return strings.Join(parts, ",")
}
