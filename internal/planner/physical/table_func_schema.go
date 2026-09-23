// SPDX-License-Identifier: MIT

package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// tableFuncDeclaredSchema returns the columns a table function publishes when
// its SIGNATURE declares them — the function's NAME and its ARGUMENTS alone,
// with no access to whatever the function reads.
//
// It is the plan-time half of "a table function in FROM is a relation"
// (ADR-0039 §1): the binder refuses a reference to a column the function does
// not publish with 42703 as it does over a base table, instead of leaving the
// scope open and answering NULL for every row (#1210); the aggregate
// result-type rules see an integer as an integer, so `SUM(x) FROM
// generate_series(1,3) gs(x)` declares bigint rather than float8 (#1211,
// ADR-0024 §2a); a qualified star expands; and a relation that produces NO
// batch still publishes its columns.
//
// A function whose columns are its INPUT's — every file and database reader —
// is deliberately NOT here: this is a pure function of the call. A local file
// reader's schema is read by readerPlanTimeSchema, only after the door has
// authorized the capability (ADR-0039 §3); a reader that gets none there is
// refused at its FIRST BATCH (table_func_required.go). ok=false means "not
// knowable from the call", the caller's signal to ask the reader path.
func tableFuncDeclaredSchema(funcName string, args []string, withOrdinality bool) ([]parquet.Column, bool) {
	switch strings.ToLower(funcName) {
	case "generate_series":
		if len(args) < 2 || len(args) > 3 {
			return nil, false
		}
		typ := parquet.TypeInt32
		for _, a := range args {
			v, err := strconv.ParseInt(strings.TrimSpace(a), 10, 64)
			if err != nil {
				// A non-integer series (a timestamp or a numeric one) is a
				// signature this engine does not have; the source build says
				// so, and nothing is declared for it here.
				return nil, false
			}
			// PostgreSQL resolves generate_series(int4,…) for a call whose
			// arguments all fit int4 and generate_series(int8,…) otherwise,
			// and the column it publishes is the overload's — `integer` for
			// generate_series(1,2), `bigint` for generate_series(3e9,…)
			// (measured on 17.11). The declaration is what SUM reads, so
			// the overload has to be resolved the same way or an exact
			// bigint total declares numeric.
			if v > 2147483647 || v < -2147483648 {
				typ = parquet.TypeInt64
			}
		}
		return []parquet.Column{{Name: "generate_series", Type: typ}}, true
	case "unnest":
		if len(args) == 0 {
			return nil, false
		}
		cols := []parquet.Column{{Name: "unnest", Type: inferUnnestType(args)}}
		if withOrdinality {
			cols = append(cols, parquet.Column{Name: "ordinality", Type: parquet.TypeInt64})
		}
		return cols, true
	}
	return nil, false
}

// applyFuncColumnAliases renames a table function's declared columns by a FROM
// item's COLUMN-ALIAS LIST, positionally, and raises PostgreSQL's own 42P10
// for a list longer than the relation.
//
// It is withColumnAliases' plan-time twin: the SOURCE wrapper applies the same
// rule at the first batch for a function whose width only its input knows, and
// this one applies it where the width is declared — so the over-long list is
// refused BEFORE anything runs, which is where PostgreSQL refuses it.
func applyFuncColumnAliases(cols []parquet.Column, aliases []string, relName string) ([]parquet.Column, error) {
	if len(aliases) == 0 {
		return cols, nil
	}
	if relName == "" {
		relName = "table"
	}
	if len(aliases) > len(cols) {
		return nil, sqlerr.New("42P10",
			"table %q has %d columns available but %d columns specified",
			relName, len(cols), len(aliases))
	}
	out := make([]parquet.Column, len(cols))
	copy(out, cols)
	for i, name := range aliases {
		out[i].Name = name
	}
	return out, nil
}
