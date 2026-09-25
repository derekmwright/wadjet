// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDecimalLiteralIsNumericInEveryContext is the SELECT side of arc VL
// round 5's decimal-literal declaration (round-4 review B2): a fractional
// literal is PostgreSQL's numeric wherever it sits, and every context that
// holds one keeps the VALUE PostgreSQL answers. PostgreSQL 17.11 values; the
// `pg` column differs only where a numeric choice over constants of mixed
// scales prints every value at the fold's one scale (ADR-0024's #764 class:
// the same number, trailing zeros) — before this round those choices were
// declared by their FIRST argument, and `LEAST(3, 2.5)` answered 2.
//
// The ARRAY row is the store hazard the declaration had to respect: an
// ARRAY[…] of constants materializes each constant's own box, and an integer
// box in a DECIMAL element vector is the already-scaled carrier — the first
// cut answered `unnest(ARRAY[1,2.5])` as 0.1, 2.5. An array of constants keeps
// its double-precision element (postgres-differences).
func TestDecimalLiteralIsNumericInEveryContext(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		sql, want, pg string
		typ       parquet.TypeID
	}{
		{"SELECT 2.50 AS v", "[2.50]", "", parquet.TypeDecimal},
		{"SELECT -2.50 AS v", "[-2.50]", "", parquet.TypeDecimal},
		{"SELECT 1e3 AS v", "[1000]", "", parquet.TypeDecimal},
		{"SELECT CASE WHEN true THEN 2.50 END AS v", "[2.50]", "", parquet.TypeDecimal},
		{"SELECT COALESCE(2.50, 1) AS v", "[2.50]", "", parquet.TypeDecimal},
		{"SELECT GREATEST(0.5, 1.5) AS v", "[1.5]", "", parquet.TypeDecimal},
		{"SELECT LEAST(3, 2.5) AS v", "[2.5]", "", parquet.TypeDecimal},
		{"SELECT v FROM (SELECT 2.50 AS v) q", "[2.50]", "", parquet.TypeDecimal},
		{"WITH c AS (SELECT 2.50 AS v) SELECT v FROM c", "[2.50]", "", parquet.TypeDecimal},
		{"SELECT v FROM (VALUES (2.50)) t(v)", "[2.50]", "", parquet.TypeDecimal},
		{"SELECT 2.50 + 1 AS v", "[3.50]", "", parquet.TypeDecimal},
		{"SELECT unnest(ARRAY[1, 2.5]) AS v", "[1 2.5]", "", parquet.TypeFloat64},
		{"SELECT unnest(ARRAY[2.5, 1]) AS v", "[2.5 1]", "", parquet.TypeFloat64},
		{"SELECT COALESCE(1, 2.5) AS v", "[1.0]", "[1]", parquet.TypeDecimal},
		{"SELECT CASE WHEN x > 1 THEN 1 ELSE 2.5 END AS v FROM (VALUES (1), (2)) t(x)", "[2.5 1.0]", "[2.5 1]", parquet.TypeDecimal},
	} {
		res, err := db.Query(ctx, tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		var got []string
		for _, r := range res.Rows {
			got = append(got, fmt.Sprint(r["v"]))
		}
		if fmt.Sprint(got) != tc.want || len(res.OutputSchema) != 1 || res.OutputSchema[0].Type != tc.typ {
			t.Errorf("%s = %v declared %v, want %s declared %v (PostgreSQL 17.11 %s)",
				tc.sql, got, res.OutputSchema, tc.want, tc.typ, map[bool]string{true: tc.want, false: tc.pg}[tc.pg == ""])
		}
	}
}
