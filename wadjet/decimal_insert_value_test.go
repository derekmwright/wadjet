package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The INSERT half of ADR-0024 item 4 (#647).
//
// A DECIMAL literal used to reach the parquet file writer's
// decimalUnscaledInt64, whose string arm ran strconv.ParseFloat and then
// int64(math.Round(t*pow)): a literal too wide for the column wrapped the
// int64 (99999999999999999999.99 into a DECIMAL(9,2) stored
// -92233720368547758.08), unparseable text stored 0, ' 3.50 ' stored 0
// because ParseFloat refuses the surrounding space, and every value past
// float64's ~16 significant digits lost its exactness on the way in.
//
// PostgreSQL 17.11, verified live on postgres:17-alpine, is the authority
// for every row of this table (ADR-0012): the overflow is 22003 with
// "A field with precision 9, scale 2 must round to an absolute value less
// than 10^7", 'abc' is 22P02 "invalid input syntax for type numeric", a
// literal finer than the column's scale ROUNDS half away from zero on
// assignment (1.239 -> 1.24), an integer literal is the VALUE (5 -> 5.00)
// and surrounding C whitespace is stripped (' 3.50 ' -> 3.50).
func TestInsertDecimalLiteralFollowsPostgres(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ins", schema, nil); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		literal string
		want    string // expected stored rendering, "" when the INSERT must fail
		state   string // expected SQLSTATE when it must fail
	}{
		{name: "in range", literal: "12.34", want: "12.34"},
		{name: "past the declared precision", literal: "99999999999999999999.99", state: "22003"},
		{name: "exponent past the declared precision", literal: "1e40", state: "22003"},
		{name: "not a number", literal: "abc", state: "22P02"},
		{name: "quoted, not a number", literal: "'abc'", state: "22P02"},
		{name: "surrounding whitespace", literal: "' 3.50 '", want: "3.50"},
		{name: "finer scale rounds half away from zero", literal: "1.239", want: "1.24"},
		{name: "finer scale rounds down", literal: "1.234", want: "1.23"},
		{name: "negative finer scale rounds away from zero", literal: "-1.235", want: "-1.24"},
		{name: "integer literal is the value", literal: "5", want: "5.00"},
		{name: "widest value the precision allows", literal: "9999999.99", want: "9999999.99"},
		{name: "rounding INTO the overflow", literal: "9999999.999", state: "22003"},
		{name: "NaN has no stored value", literal: "'NaN'", state: "22003"},
		{name: "Infinity has no stored value", literal: "'Infinity'", state: "22003"},
		{name: "-Infinity has no stored value", literal: "'-Infinity'", state: "22003"},
	}

	id := 0
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id++
			_, err := db.Execute(ctx, "INSERT INTO ins (id, d) VALUES ("+fmt.Sprint(id)+", "+tc.literal+")")
			if tc.state != "" {
				if err == nil {
					t.Fatalf("INSERT %s: want SQLSTATE %s, got no error", tc.literal, tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Fatalf("INSERT %s: SQLSTATE %q, want %q (err: %v)", tc.literal, got, tc.state, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("INSERT %s: %v", tc.literal, err)
			}
			res, err := db.Query(ctx, "SELECT d FROM ins WHERE id = "+fmt.Sprint(id))
			if err != nil {
				t.Fatalf("SELECT after INSERT %s: %v", tc.literal, err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("INSERT %s: %d rows back, want 1", tc.literal, len(res.Rows))
			}
			if got := fmt.Sprint(res.Rows[0]["d"]); got != tc.want {
				t.Fatalf("INSERT %s stored %q, want %q", tc.literal, got, tc.want)
			}
		})
	}
}

// A DECIMAL(38,10) column holds 28 integer digits and 10 fraction digits, and
// every one of them survives the write: the float64 the old path went through
// carries ~16 significant digits, so this value used to read back as a
// different number.
func TestInsertWideDecimalKeepsEveryDigit(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "wide", schema, nil); err != nil {
		t.Fatal(err)
	}

	// 30 significant digits: 20 before the point, 10 after.
	const exact = "12345678901234567890.1234567891"
	if _, err := db.Execute(ctx, "INSERT INTO wide (id, d) VALUES (1, "+exact+")"); err != nil {
		t.Fatalf("INSERT %s: %v", exact, err)
	}
	// One digit past the 38 the precision declares.
	if _, err := db.Execute(ctx, "INSERT INTO wide (id, d) VALUES (2, 123456789012345678901234567890.1234567891)"); err == nil {
		t.Fatal("INSERT of a 40-digit value into DECIMAL(38,10) succeeded; want 22003")
	} else if got := sqlerr.StateOf(err); got != "22003" {
		t.Fatalf("SQLSTATE %q, want 22003 (err: %v)", got, err)
	}

	res, err := db.Query(ctx, "SELECT d FROM wide WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%d rows back, want 1", len(res.Rows))
	}
	if got := fmt.Sprint(res.Rows[0]["d"]); got != exact {
		t.Fatalf("stored %q, want %q", got, exact)
	}
}
