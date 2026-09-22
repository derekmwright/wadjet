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

// TestArcRPSampleAndPastSampleShareOneGrammar: a file whose 150 rows all
// hold the same spelling answers, and row 1 (inside the 100-row sample) and
// row 150 (past it) read the SAME value — the inference types a number with
// the input functions that read it later, so the sample can never type a
// column its own spelling is then refused from. Each expected value is what
// PostgreSQL 17.11's int8in / float8in yields (bigint first, then double
// precision; a spelling both refuse is text), measured in
// tooling/arcs/rp_reader_past_sample/rp_finisher/pg_input_functions_17.11.txt.
// At 1c183bc3 `1_000` was 22P02 and `1e-400` 22003 at row 101 (review
// B-r2-1), and read_json typed `1e-400`/`1e999` double precision and read 0.
func TestArcRPSampleAndPastSampleShareOneGrammar(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cells := []struct {
		kind, spelling, want string
	}{
		// read_csv: bigint by int8in
		{"csv", "1_000", "1000"}, {"csv", " 5", "5"}, {"csv", "5 ", "5"}, {"csv", "\t5", "5"},
		{"csv", "0x10", "16"}, {"csv", "0X1F", "31"}, {"csv", "0o17", "15"}, {"csv", "0b101", "5"},
		{"csv", "017", "17"}, {"csv", "-0", "0"}, {"csv", "-9223372036854775808", "-9223372036854775808"},
		// read_csv: double precision by float8in
		{"csv", "1e3", "1000"}, {"csv", "1.0", "1"}, {"csv", " 1.5 ", "1.5"}, {"csv", ".5", "0.5"},
		{"csv", "0x1p-2", "0.25"}, {"csv", "Infinity", "+Inf"}, {"csv", "-inf", "-Inf"}, {"csv", "NaN", "NaN"},
		{"csv", "9223372036854775808", "9.223372036854776e+18"},
		// read_csv: text — both input functions refuse the spelling
		{"csv", "1e-400", "1e-400"}, {"csv", "1e400", "1e400"}, {"csv", "1_000.5", "1_000.5"},
		{"csv", "1__0", "1__0"}, {"csv", "_1", "_1"}, {"csv", "0x", "0x"}, {"csv", "1e", "1e"},
		// read_json: a number float8 cannot hold is text
		{"json", "1e-400", "1e-400"}, {"json", "1e999", "1e999"}, {"json", "1e3", "1000"},
		{"json", "99999999999999999999", "1e+20"}, {"json", "0.75", "0.75"},
	}
	for _, cell := range cells {
		t.Run(fmt.Sprintf("%s/%q", cell.kind, cell.spelling), func(t *testing.T) {
			var b strings.Builder
			if cell.kind == "csv" {
				b.WriteString("id,c\n")
			}
			for i := 1; i <= 150; i++ {
				if cell.kind == "csv" {
					fmt.Fprintf(&b, "%d,\"%s\"\n", i, cell.spelling)
				} else {
					fmt.Fprintf(&b, "{\"id\":%d,\"c\":%s}\n", i, cell.spelling)
				}
			}
			path := filepath.Join(t.TempDir(), "same."+cell.kind)
			if err := os.WriteFile(path, []byte(b.String()), 0600); err != nil {
				t.Fatal(err)
			}
			res, err := db.Query(ctx, fmt.Sprintf("SELECT id, c FROM read_%s('%s') ORDER BY id", cell.kind, path))
			if err != nil {
				t.Fatalf("every row holds %q; want 150 rows, got %v", cell.spelling, err)
			}
			if len(res.Rows) != 150 {
				t.Fatalf("%d rows, want 150", len(res.Rows))
			}
			first, last := fmt.Sprint(res.Rows[0]["c"]), fmt.Sprint(res.Rows[149]["c"])
			if first != cell.want || last != cell.want {
				t.Fatalf("row 1 = %q, row 150 = %q, want %q for both", first, last, cell.want)
			}
		})
	}
}
