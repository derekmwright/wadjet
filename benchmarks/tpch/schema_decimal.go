package tpch

import (
	"os"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DECIMAL(15,2) fixture variant (ADR-0024).
//
// The TPC-H specification (v3, §1.3) declares eight columns DECIMAL(15,2).
// schema.go carries them as FLOAT64 and is the PUBLISHED-NUMBER benchmark; it
// stays the default and nothing here touches it. The DECIMAL variant is a
// second fixture over the SAME VALUES with the spec-conformant carrier: a
// correctness gate first (22 queries whose arithmetic, aggregation, comparison
// and ORDER BY all run on exact fixed-point, on both execution paths) and the
// decimal performance baseline second.
//
// l_quantity is decimal in the specification too and stays FLOAT64 in BOTH
// fixtures, so the only difference between them is these eight columns. It
// holds whole numbers in this generator, so no exactness rides on it.

// Fixture selects the carrier of the eight monetary columns.
type Fixture int

const (
	// FloatFixture is the FLOAT64 schema — the default, and the one the
	// published benchmark numbers are measured on.
	FloatFixture Fixture = iota
	// DecimalFixture is the spec-conformant DECIMAL(15,2) schema.
	DecimalFixture
)

func (f Fixture) String() string {
	if f == DecimalFixture {
		return "decimal"
	}
	return "float"
}

// MoneyPrecision and MoneyScale are the specification's DECIMAL(15,2).
const (
	MoneyPrecision = 15
	MoneyScale     = 2
)

// MoneyColumns are the eight columns TPC-H v3 §1.3 declares DECIMAL(15,2).
var MoneyColumns = map[string]bool{
	"s_acctbal":       true,
	"p_retailprice":   true,
	"ps_supplycost":   true,
	"c_acctbal":       true,
	"o_totalprice":    true,
	"l_extendedprice": true,
	"l_discount":      true,
	"l_tax":           true,
}

// AllTablesDecimal is AllTables with the eight monetary columns declared
// DECIMAL(15,2). It is BUILT by rewriting the FLOAT64 set rather than written
// out again, so the two can never drift apart in any other column: a column
// added to schema.go appears in both.
var AllTablesDecimal = buildDecimalSchemas()

func buildDecimalSchemas() map[string]parquet.Schema {
	out := make(map[string]parquet.Schema, len(AllTables))
	for name, s := range AllTables {
		cols := make([]parquet.Column, len(s.Columns))
		copy(cols, s.Columns)
		for i := range cols {
			if !MoneyColumns[cols[i].Name] {
				continue
			}
			cols[i].Type = parquet.TypeDecimal
			cols[i].Precision = MoneyPrecision
			cols[i].Scale = MoneyScale
		}
		out[name] = parquet.Schema{Columns: cols}
	}
	return out
}

// TablesFor returns the table schemas for the fixture.
func TablesFor(f Fixture) map[string]parquet.Schema {
	if f == DecimalFixture {
		return AllTablesDecimal
	}
	return AllTables
}

// SchemaFor returns one table's schema for the fixture.
func SchemaFor(f Fixture, table string) parquet.Schema {
	return TablesFor(f)[table]
}

// money boxes a monetary value the fixture's way: a float64 in the FLOAT64
// schema, exact decimal TEXT in the DECIMAL(15,2) variant.
//
// cents is the value's EXACT hundredths — every generator computes it as an
// integer and converts here, so the decimal fixture ingests the digits it
// means rather than a float64 rounded back to two places. The float arm
// divides that same integer by 100, which is bit-for-bit what the generators
// produced before this variant existed, so the published fixture is unmoved.
func (f Fixture) money(cents int64) any {
	if f == DecimalFixture {
		return MoneyText(cents)
	}
	return float64(cents) / 100.0
}

// MoneyText renders cents as exact DECIMAL(15,2) text.
func MoneyText(cents int64) string {
	if cents < 0 {
		return "-" + moneyDigits(-cents)
	}
	return moneyDigits(cents)
}

func moneyDigits(cents int64) string {
	frac := strconv.FormatInt(cents%100, 10)
	if len(frac) == 1 {
		frac = "0" + frac
	}
	return strconv.FormatInt(cents/100, 10) + "." + frac
}

// FixtureFromEnv reads TPCH_DECIMAL. Anything but "1"/"true"/"yes" is the
// FLOAT64 default, so an unset or misspelled variable can never silently swap
// the published benchmark's fixture.
//
// It is read by the tests whose fixture is a CHOICE — the PostgreSQL oracle
// (both arms) and the wire corpus. The tests that hold a banked expectation —
// TestTPCHQueries, the baseline files, the DuckDB float gate — take
// FloatFixture explicitly and are unaffected by the variable, because their
// stored answers are the float schema's. The decimal gates name
// DecimalFixture explicitly for the same reason.
func FixtureFromEnv() Fixture {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("TPCH_DECIMAL"))) {
	case "1", "true", "yes":
		return DecimalFixture
	}
	return FloatFixture
}
