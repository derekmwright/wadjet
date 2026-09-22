// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestArcRPCSVGlobReadsEachHeaderOnce: a glob of CSV files with headers reads
// every file's header as a header. At 16b924d1 the second file's header was a
// DATA row — COUNT(*) one too many per file, and inside the 100-row sample
// the header's names made every column text (SUM(age) NULL); with the
// past-sample check alone it became a 22P02 on "age" past row 100. A later
// file whose first record is not the header continues the first file.
func TestArcRPCSVGlobReadsEachHeaderOnce(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, cell := range []struct {
		name          string
		files         []string
		count, sumAge int64
	}{
		{"small", []string{"name,age\na,1\n", "name,age\nz,7\n"}, 2, 8},
		{"past_sample", []string{"name,age\n" + rpCSVRows(150), "name,age\nz,7\n"}, 151, 11332},
		{"three_files_no_trailing_newline", []string{"name,age\na,1", "name,age\nb,2", "name,age\nc,3"}, 3, 6},
		{"empty_first_file", []string{"", "name,age\nb,2\n", "name,age\nc,3\n"}, 2, 5},
		// The review's fixture (B1): at f58a653e the second header, past row
		// 100, was a 22P02 on "a"; PostgreSQL's two COPY … HEADER answer
		// 12880 / 160 / 160.
		{"review_150_10", []string{"name,age\n" + rpCSVRows(150), "name,age\n" + rpCSVRowsFrom(151, 160)}, 160, 12880},
		{"review_3000_10", []string{"name,age\n" + rpCSVRows(3000), "name,age\n" + rpCSVRowsFrom(3001, 3010)}, 3010, 4531555},
		// A later file that does not repeat the header continues the first.
		{"continuation", []string{"name,age\n" + rpCSVRows(150), "z,7\n"}, 151, 11332},
		{"quoted_header", []string{"\"name\",\"age\"\na,1\n", "\"name\",\"age\"\nb,2\n"}, 2, 3},
	} {
		t.Run(cell.name, func(t *testing.T) {
			dir := t.TempDir()
			for i, body := range cell.files {
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("p%d.csv", i)), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			sql := fmt.Sprintf("SELECT COUNT(*) AS n, SUM(age) AS s, COUNT(age) AS na FROM read_csv('%s')", filepath.Join(dir, "*.csv"))
			result, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatal(err)
			}
			got := fmt.Sprint(result.Rows[0]["n"], " ", result.Rows[0]["s"])
			if na := fmt.Sprint(result.Rows[0]["na"]); na != fmt.Sprint(cell.count) {
				t.Fatalf("COUNT(age) = %s, want %d (every row has an age)", na, cell.count)
			}
			if want := fmt.Sprint(cell.count, " ", cell.sumAge); got != want {
				t.Fatalf("COUNT(*), SUM(age) = %s, want %s", got, want)
			}
		})
	}
}

func rpCSVRows(n int) string { return rpCSVRowsFrom(1, n) }

func rpCSVRowsFrom(from, to int) string {
	var b strings.Builder
	for i := from; i <= to; i++ {
		fmt.Fprintf(&b, "n%d,%d\n", i, i)
	}
	return b.String()
}
