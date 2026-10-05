// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A typed numeric NULL supplies a numeric type even beside an integer.
// The later write detects a result incorrectly created as an integer.
func TestArcUNTypedNullSetOperationWrite(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{"CREATE TABLE un_u (v NUMERIC)", "INSERT INTO un_u VALUES (1),(2),(3)"} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for i, source := range []string{
		"SELECT CAST(NULL AS NUMERIC) AS v FROM un_u UNION ALL SELECT 2 AS v FROM un_u",
		"SELECT 2 AS v FROM un_u UNION ALL SELECT CAST(NULL AS NUMERIC) AS v FROM un_u",
	} {
		name := fmt.Sprintf("typed_null_%d", i)
		for _, q := range []string{"CREATE TABLE " + name + " AS " + source, "INSERT INTO " + name + " VALUES (1.255)"} {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
		r, err := db.Query(ctx, "SELECT v FROM "+name+" ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprint(r.Cells(0)[0]); got != "1.255" {
			t.Errorf("%s: inserted 1.255, read %s", source, got)
		}
	}
}
