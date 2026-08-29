package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/dmlassign"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The HTTP arm of the SET-value matrix, over the SAME cells and the same
// fixture as wadjet.TestDMLAssignmentCastFollowsPostgres.
//
// These executors are a second copy of the embedded ones and shared the R1
// defect: an integer box out of the expression engine reached
// DecimalValueFromBox, which reads an integer as the already-unscaled carrier
// (ADR-0018 §4), so `UPDATE mv SET d = n` with n = 10 stored 0.10 and returned
// success. Running the matrix here rather than trusting the shared function is
// the point — these two doors have drifted before (#647).
func TestHTTPDMLAssignmentCastFollowsPostgres(t *testing.T) {
	ctx := context.Background()
	for _, tc := range dmlassign.Matrix() {
		t.Run(tc.Name, func(t *testing.T) {
			cat, schema := assignmentCatalog(t)
			sql := "UPDATE mv SET " + tc.Set
			parsed, perr := plansql.Parse(sql)
			if perr != nil {
				t.Fatalf("parsing %q: %v", sql, perr)
			}
			_, err := runHTTPDML(ctx, cat, parsed)

			if tc.State != "" {
				if err == nil {
					t.Fatalf("%s succeeded; want %s. Row is now %v", sql, tc.State,
						liveRows(t, cat, "mv", schema))
				}
				if got := sqlerr.StateOf(err); got != tc.State {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", sql, got, tc.State, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			live := liveRows(t, cat, "mv", schema)
			if len(live) != 1 {
				t.Fatalf("%d live rows after %s, want 1: %v", len(live), sql, live)
			}
			if got := fmt.Sprint(live[0][tc.Col]); got != tc.Want {
				t.Errorf("%s stored %s = %s, want %s (PostgreSQL 17)", sql, tc.Col, got, tc.Want)
			}
		})
	}
}

// assignmentCatalog is the fixture the PostgreSQL expectations were read
// against: mv(1, 10, 1.50, 2.5, 'ab'). The HTTP door has no MERGE executor, so
// only the target half is built here.
func assignmentCatalog(t *testing.T) (*catalog.Catalog, parquet.Schema) {
	t.Helper()
	ctx := context.Background()
	cat := visibilityCatalog(t)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "mv", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := ingest.New(cat, "mv", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "n": int64(10), "d": "1.50", "f": 2.5, "s": "ab"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return cat, schema
}
