// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

func TestArcRPPastSampleQueries(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, kind := range []string{"csv", "json"} {
		for _, row := range []int{101, 2049, 2200, 5000} {
			var body strings.Builder
			if kind == "csv" {
				body.WriteString("a\n")
			}
			for i := 1; i <= row; i++ {
				v := fmt.Sprint(i)
				if i == row {
					v = "0.75"
				}
				if kind == "csv" {
					fmt.Fprintln(&body, v)
				} else {
					fmt.Fprintf(&body, "{\"a\":%s}\n", v)
				}
			}
			path := filepath.Join(t.TempDir(), "values."+kind)
			if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{"SELECT SUM(a)", "SELECT COUNT(*)", "SELECT a"} {
				t.Run(fmt.Sprintf("%s/%d/%s", kind, row, query), func(t *testing.T) {
					sql := fmt.Sprintf("%s FROM read_%s('%s')", query, kind, path)
					if query == "SELECT a" {
						sql += " LIMIT 1"
					}
					result, err := db.Query(ctx, sql)
					// The pipeline pulls another batch before the LIMIT stops execution.
					stopsBefore := query == "SELECT a" && ((kind == "csv" && row > 2148) || row > 4096)
					if stopsBefore {
						if err != nil {
							t.Fatal(err)
						}
						if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0]["a"]) != "1" {
							t.Fatalf("LIMIT result: %+v", result)
						}
						return
					}
					if sqlerr.StateOf(err) != "22P02" {
						t.Fatalf("want 22P02, got result=%+v err=%v", result, err)
					}
					for _, part := range []string{"read_" + kind, path, fmt.Sprintf("row %d", row), `column "a"`} {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q: %v", part, err)
						}
					}
				})
			}
		}
	}
}
