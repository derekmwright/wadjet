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

// TestArcRPPastSampleQueries: the 5050.75 fixture of #1242 through the
// embedded API — 100 whole numbers, then 0.75 — answers 22P02 for SUM and
// COUNT(*), naming the reader, the file, the row and the column; at 16b924d1
// SUM answered 5050 and COUNT(*) 101.
//
// A LIMIT answers rows when the reader never reaches the row. The pipeline
// reads ONE batch past the batch that satisfies the LIMIT (exec.Limit reports
// Done on the push after the one that fills it), so LIMIT 1 reads the
// reader's first two batches: rows 1-2048 and 2049-4096 of read_json, and
// rows 1-100 (the CSV reader's buffered sample) and 101-2148 of read_csv. A
// change inside those refuses; one past them is never read.
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
					want := []string{
						fmt.Sprintf("read_%s: %s: row %d column \"a\": value ", kind, path, row),
						"(double precision) is not of type bigint (the column's type was inferred from the file's first 100 rows)",
					}
					for _, part := range want {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q: %v", part, err)
						}
					}
				})
			}
		}
	}
}

// TestArcRPGlobRefusalNamesTheFileAndItsRow: across a glob the refusal names
// the FILE holding the value and the row within that file, not the glob and
// the row of the concatenated stream (review N3: file 2's 5th row was "row
// 155" of the glob at f58a653e).
func TestArcRPGlobRefusalNamesTheFileAndItsRow(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, kind := range []string{"csv", "json"} {
		for _, first := range []int{150, 3000} {
			t.Run(fmt.Sprintf("%s/%d", kind, first), func(t *testing.T) {
				dir := t.TempDir()
				write := func(name string, from, to, bad int) string {
					var b strings.Builder
					if kind == "csv" {
						b.WriteString("a\n")
					}
					for i := from; i <= to; i++ {
						v := fmt.Sprint(i)
						if i == bad {
							v = "0.75"
						}
						if kind == "csv" {
							fmt.Fprintln(&b, v)
						} else {
							fmt.Fprintf(&b, "{\"a\":%s}\n", v)
						}
					}
					p := filepath.Join(dir, name+"."+kind)
					if err := os.WriteFile(p, []byte(b.String()), 0600); err != nil {
						t.Fatal(err)
					}
					return p
				}
				write("f1", 1, first, 0)
				f2 := write("f2", first+1, first+10, first+5)
				_, err := db.Query(ctx, fmt.Sprintf("SELECT SUM(a) FROM read_%s('%s')", kind, filepath.Join(dir, "*."+kind)))
				if sqlerr.StateOf(err) != "22P02" {
					t.Fatalf("want 22P02, got %v", err)
				}
				want := fmt.Sprintf("%s row 5 column \"a\": value ", f2)
				if !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "the first 100 rows of the input") {
					t.Fatalf("missing %q in: %v", want, err)
				}
			})
		}
	}
}

// TestArcRPByteCappedSampleQuery: the review's B2 fixture through read_json —
// 150 objects of ~120 KiB, so the 8 MiB sample holds fewer than 100 of them —
// with 0.75 at row 90 or 100 of a bigint column. At f58a653e both read 0.
func TestArcRPByteCappedSampleQuery(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pad := strings.Repeat("x", 120<<10)
	for _, bad := range []int{90, 100} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			var b strings.Builder
			for i := 1; i <= 150; i++ {
				v := fmt.Sprint(i)
				if i == bad {
					v = "0.75"
				}
				fmt.Fprintf(&b, "{\"a\":%s,\"pad\":\"%s\"}\n", v, pad)
			}
			path := filepath.Join(t.TempDir(), "big.json")
			if err := os.WriteFile(path, []byte(b.String()), 0600); err != nil {
				t.Fatal(err)
			}
			res, err := db.Query(ctx, fmt.Sprintf("SELECT SUM(a) AS s FROM read_json('%s')", path))
			if sqlerr.StateOf(err) != "22P02" || !strings.Contains(err.Error(), fmt.Sprintf("row %d column \"a\": value 0.75", bad)) {
				t.Fatalf("got %v (result %v), want 22P02 at row %d", err, res, bad)
			}
		})
	}
}
