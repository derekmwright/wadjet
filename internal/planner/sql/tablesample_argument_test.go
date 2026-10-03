// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The TABLESAMPLE argument is an EXPRESSION, as PostgreSQL 17.11's grammar
// has it (#1411): every spelling below is an argument there, typed and
// evaluated as real by the planner (physical.TablesampleArgument). At
// 6184761c the clause read one number token or a float parameter's CAST, so
// a negative literal, NULL, '50', 25 * 2 and CAST(50 AS NUMERIC) were 42601.
// The clause keeps the argument's source text for the one rewrite that
// writes it back out (a column-alias list lowers the relation to a derived
// body, lowerNamedRelationColumnAliases).
func TestTablesampleArgumentIsAnExpression(t *testing.T) {
	for _, arg := range []string{
		"0", "100", "-1", "-0.0", "1e400", "NULL", "'50'", "' 50 '", "25 * 2", "-(-50)", "+50",
		"CAST(50 AS NUMERIC)", "CAST('1e400' AS DOUBLE PRECISION)", "CAST('NaN' AS DOUBLE PRECISION)",
		"abs(-50)", "CASE WHEN true THEN 50 END", "((100))", "id", "(SELECT 50)", "true",
	} {
		t.Run(arg, func(t *testing.T) {
			sql := "SELECT count(*) FROM p TABLESAMPLE BERNOULLI (" + arg + ")"
			parsed, err := Parse(sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			tr := info.Tables[0]
			if tr.SampleMethod != "BERNOULLI" || tr.SamplePercent != arg {
				t.Fatalf("method %q text %q; want BERNOULLI and the argument's text %q",
					tr.SampleMethod, tr.SamplePercent, arg)
			}
		})
	}
}

// PostgreSQL parses an argument LIST and refuses its length for a method
// taking one (2202H, `tablesample method bernoulli requires 1 argument, not
// 2`); an empty list is its syntax error.
func TestTablesampleArgumentCount(t *testing.T) {
	for _, c := range []struct{ sql, state string }{
		{"SELECT count(*) FROM p TABLESAMPLE BERNOULLI (50, 1)", "2202H"},
		{"SELECT count(*) FROM p TABLESAMPLE SYSTEM (1, 2, 3)", "2202H"},
		{"SELECT count(*) FROM p TABLESAMPLE BERNOULLI ()", "42601"},
		{"SELECT count(*) FROM p TABLESAMPLE BERNOULLI (50", "42601"},
	} {
		_, err := Parse(c.sql)
		if err == nil {
			t.Errorf("%s: parsed; want %s", c.sql, c.state)
			continue
		}
		if got := sqlerr.StateOf(err); got != c.state {
			t.Errorf("%s: %s (%v); want %s", c.sql, got, err, c.state)
		}
	}
}

// The lowering of a column-alias list writes the argument back as it was
// written, so the derived body samples by the same argument.
func TestTablesampleArgumentSurvivesTheColumnAliasLowering(t *testing.T) {
	parsed, err := Parse("SELECT x FROM p TABLESAMPLE BERNOULLI (CAST('-1' AS DOUBLE PRECISION)) a(x, y)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	body := info.Tables[0].Name
	if !strings.Contains(body, "TABLESAMPLE BERNOULLI(CAST('-1' AS DOUBLE PRECISION))") {
		t.Fatalf("lowered body %q does not carry the argument", body)
	}
	inner, err := Parse(strings.TrimSuffix(strings.TrimPrefix(body, "("), ")"))
	if err != nil {
		t.Fatalf("the lowered body does not parse: %v", err)
	}
	innerInfo, err := ExtractSelect(inner)
	if err != nil || innerInfo.Tables[0].SamplePercent != "CAST('-1' AS DOUBLE PRECISION)" {
		t.Fatalf("the lowered body lost its argument: %v", err)
	}
}
