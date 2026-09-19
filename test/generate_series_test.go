// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestGenerateSeriesE2E(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Every `rows`/`first`/`last` below is PostgreSQL 17.11's answer for the
	// same call, measured live. Two of them changed with the arc-TF fix and
	// the old expectations were this engine's own:
	//
	//   - a call whose bounds run the other way from its step is EMPTY. The
	//     step was negated whenever start > stop, so `generate_series(5,1)`
	//     answered five rows where 17.11 answers none; the descending series
	//     is written `generate_series(5,1,-1)`, which now parses.
	//   - the column is `integer` for a call whose arguments fit int4 —
	//     PostgreSQL resolves the int4 overload — so the value boxes int32.
	//     It boxed int64 for every call.
	tests := []struct {
		name  string
		query string
		rows  int
		first int64
		last  int64
	}{
		{
			name:  "basic ascending",
			query: "SELECT * FROM generate_series(1, 5)",
			rows:  5, first: 1, last: 5,
		},
		{
			name:  "with step",
			query: "SELECT * FROM generate_series(0, 10, 3)",
			rows:  4, first: 0, last: 9,
		},
		{
			name:  "descending with an explicit negative step",
			query: "SELECT * FROM generate_series(5, 1, -1)",
			rows:  5, first: 5, last: 1,
		},
		{
			name:  "single value",
			query: "SELECT * FROM generate_series(42, 42)",
			rows:  1, first: 42, last: 42,
		},
		{
			name:  "descending bounds with the default step are empty",
			query: "SELECT * FROM generate_series(5, 1)",
			rows:  0,
		},
		{
			name:  "an int8 argument publishes a bigint column",
			query: "SELECT * FROM generate_series(3000000000, 3000000002)",
			rows:  3, first: 3000000000, last: 3000000002,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(r.Rows) != tt.rows {
				t.Fatalf("got %d rows, want %d", len(r.Rows), tt.rows)
			}
			// A relation with no rows still publishes its column.
			if len(r.Columns) != 1 || r.Columns[0] != "generate_series" {
				t.Fatalf("columns %v, want [generate_series]", r.Columns)
			}
			if tt.rows == 0 {
				return
			}
			firstVal, ok := seriesValue(r.Rows[0]["generate_series"])
			if !ok {
				t.Fatalf("first row: expected an integer, got %T (%v)", r.Rows[0]["generate_series"], r.Rows[0]["generate_series"])
			}
			if firstVal != tt.first {
				t.Errorf("first value: got %d, want %d", firstVal, tt.first)
			}
			lastVal, _ := seriesValue(r.Rows[len(r.Rows)-1]["generate_series"])
			if lastVal != tt.last {
				t.Errorf("last value: got %d, want %d", lastVal, tt.last)
			}
		})
	}
}

// seriesValue reads the series column whichever integer width the call's
// arguments resolved — int4 for a call that fits it, int8 otherwise, which is
// the overload PostgreSQL resolves for the same call.
func seriesValue(v any) (int64, bool) {
	switch t := v.(type) {
	case int32:
		return int64(t), true
	case int64:
		return t, true
	}
	return 0, false
}

func TestGenerateSeriesWithExpressions(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test using generate_series with expressions (SUM, WHERE, etc.)
	r, err := db.Query(ctx, "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	// SUM(1..100) = 5050
	var total float64
	switch v := r.Rows[0]["total"].(type) {
	case int64:
		total = float64(v)
	case float64:
		total = v
	default:
		t.Fatalf("total: unexpected type %T (%v)", r.Rows[0]["total"], r.Rows[0]["total"])
	}
	if total != 5050 {
		t.Errorf("SUM(1..100) = %v, want 5050", total)
	}
}
