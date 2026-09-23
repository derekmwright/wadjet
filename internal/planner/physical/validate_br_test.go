// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// brSchemaCatalog is a tableColumnSource over COMPLETE schemas, for the binder
// refusals arc BR adds: every one of them is a question about a declared TYPE,
// which the name-only fakeCatalog cannot answer.
type brSchemaCatalog map[string]parquet.Schema

func (c brSchemaCatalog) GetTable(_ context.Context, name string) (*catalog.TableMeta, error) {
	s, ok := c[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("table %q %w", name, catalog.ErrTableNotFound)
	}
	return &catalog.TableMeta{Name: name, Schema: s}, nil
}

// brCatalog mirrors the coordinator's lat_ord / lat_item fixtures plus one
// column of every flat type and every container, which is the shape each
// PostgreSQL measurement in the gates below was taken over.
func brCatalog() brSchemaCatalog {
	row := parquet.Column{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
		{Name: "a", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
	rownest := parquet.Column{Name: "c_rownest", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64, Nullable: true},
	}}
	arr := parquet.Column{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}}
	return brSchemaCatalog{
		"lat_ord": {Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "customer", Type: parquet.TypeString},
			{Name: "total", Type: parquet.TypeFloat64},
		}},
		"lat_item": {Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "order_id", Type: parquet.TypeInt64},
			{Name: "product", Type: parquet.TypeString},
			{Name: "amount", Type: parquet.TypeFloat64},
		}},
		"tm": {Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "c_bool", Type: parquet.TypeBool, Nullable: true},
			{Name: "c_i32", Type: parquet.TypeInt32, Nullable: true},
			{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
			{Name: "c_f32", Type: parquet.TypeFloat32, Nullable: true},
			{Name: "c_f64", Type: parquet.TypeFloat64, Nullable: true},
			{Name: "c_str", Type: parquet.TypeString, Nullable: true},
			{Name: "c_bytes", Type: parquet.TypeBytes, Nullable: true},
			{Name: "c_ts", Type: parquet.TypeTimestamp, Nullable: true},
			{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
			{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
			{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
			{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
			{Name: "c_port", Type: parquet.TypePort, Nullable: true},
			{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
			{Name: "c_dur", Type: parquet.TypeDuration, Nullable: true},
			{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
			{Name: "c_date", Type: parquet.TypeDate, Nullable: true},
			{Name: "c_dec", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
			arr, row, rownest,
			{Name: "c_map", Type: parquet.TypeMap, Nullable: true, ElementType: &parquet.Column{
				Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeString},
					{Name: "value", Type: parquet.TypeInt64, Nullable: true},
				}}},
			{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 4},
		}},
	}
}

// brCell is one statement and PostgreSQL 17.11's verdict on it: a SQLSTATE and
// a sentence the refusal must carry, or "" for a statement the server answers
// (a CONTROL, which the binder must let through).
type brCell struct {
	sql, state, msg string
}

func runBRCells(t *testing.T, cells []brCell) {
	t.Helper()
	cat := brCatalog()
	answered := 0
	for _, tc := range cells {
		err := validateColumns(context.Background(), cat, mustExtract(t, tc.sql))
		if tc.state == "" {
			answered++
			if err != nil {
				t.Errorf("%s\n  refused: %v\n  PostgreSQL 17.11 answers it", tc.sql, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s\n  answered\n  PostgreSQL 17.11 raises %s %s", tc.sql, tc.state, tc.msg)
			continue
		}
		if got := sqlerr.StateOf(err); got != tc.state || !strings.Contains(err.Error(), tc.msg) {
			t.Errorf("%s\n  got  %s %v\n  want %s %q", tc.sql, got, err, tc.state, tc.msg)
		}
	}
	if answered == 0 {
		t.Fatal("no control cell: a table that only refuses proves a ban, not a rule")
	}
}

// A window in HAVING and an aggregate in WHERE are refused in PostgreSQL's
// node order (#1205, #1216 item 3). Every verdict measured on 17.11.
func TestArcBRMisplacedCallsAreRefusedInPostgresOrder(t *testing.T) {
	runBRCells(t, []brCell{
		{"SELECT id, COUNT(*) AS n FROM lat_ord GROUP BY id HAVING row_number() OVER () = 1",
			"42P20", "window functions are not allowed in HAVING"},
		{"SELECT id FROM lat_ord GROUP BY id HAVING COUNT(*) OVER () > 1",
			"42P20", "window functions are not allowed in HAVING"},
		{"SELECT id FROM lat_ord GROUP BY id HAVING SUM(total) OVER () > 1",
			"42P20", "window functions are not allowed in HAVING"},
		{"SELECT id FROM lat_ord GROUP BY id HAVING NOT (row_number() OVER () = 1)",
			"42P20", "window functions are not allowed in HAVING"},
		{"SELECT COUNT(*) AS n FROM lat_ord HAVING row_number() OVER () = 1",
			"42P20", "window functions are not allowed in HAVING"},
		// The OVER clause is transformed after the placement check...
		{"SELECT id FROM lat_ord GROUP BY id HAVING COUNT(*) OVER (PARTITION BY zz) > 1",
			"42P20", "window functions are not allowed in HAVING"},
		{"SELECT id FROM lat_ord GROUP BY id HAVING row_number() OVER () = 1 AND zz > 0",
			"42P20", "window functions are not allowed in HAVING"},
		// ...and the function's arguments, and anything written before it,
		// before it.
		{"SELECT id FROM lat_ord GROUP BY id HAVING zz > 0 AND row_number() OVER () = 1",
			"42703", `"zz"`},
		{"SELECT id FROM lat_ord GROUP BY id HAVING SUM(zz) OVER () > 0", "42703", `"zz"`},
		{"SELECT id FROM lat_ord GROUP BY id HAVING EXISTS (SELECT 1 FROM lat_item i GROUP BY i.id HAVING row_number() OVER () = 1)",
			"42P20", "window functions are not allowed in HAVING"},

		{"SELECT id FROM lat_ord WHERE SUM(total) > 0 AND zz > 0",
			"42803", "aggregate functions are not allowed in WHERE"},
		{"SELECT id FROM lat_ord WHERE (SUM(total) > 0) = zz",
			"42803", "aggregate functions are not allowed in WHERE"},
		{"SELECT id FROM lat_ord WHERE COUNT(*) > 0",
			"42803", "aggregate functions are not allowed in WHERE"},
		{"SELECT id FROM lat_ord WHERE zz > 0 AND SUM(total) > 0", "42703", `"zz"`},
		{"SELECT id FROM lat_ord WHERE SUM(zz) > 0", "42703", `"zz"`},

		// Controls: a window beside a grouped query, and a HAVING without one.
		{"SELECT id, SUM(total) OVER () AS s FROM lat_ord GROUP BY id, total HAVING SUM(total) > 1", "", ""},
		{"SELECT id FROM lat_ord GROUP BY id HAVING COUNT(*) > 0", "", ""},
		{"SELECT id FROM lat_ord WHERE id IN (SELECT MAX(order_id) FROM lat_item)", "", ""},
		// An aggregate over only an OUTER block's column belongs to that
		// block (PostgreSQL's agglevelsup), not to the subquery's WHERE.
		{"SELECT id, COUNT(*) AS n FROM lat_ord GROUP BY id HAVING (SELECT MAX(i.id) FROM lat_item i WHERE i.order_id = SUM(lat_ord.id)) > 0", "", ""},
	})
}

// A set operation's own ORDER BY names a RESULT COLUMN or a position and
// nothing else (#1236). Every verdict measured on 17.11.
func TestArcBRSetOperationOrderByNamesAResultColumn(t *testing.T) {
	const u = "SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY "
	runBRCells(t, []brCell{
		{u + "zz.id", "42P01", `missing FROM-clause entry for table "zz"`},
		{u + "lat_ord.id", "42P01", `missing FROM-clause entry for table "lat_ord"`},
		// Kept superset: the first arm's selected `a.id` (ADR-0012 §5).
		{u + "a.id", "", ""},
		{u + "a.id DESC", "", ""},
		{`SELECT a.id AS v FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY a.id`,
			"42P01", `missing FROM-clause entry for table "a"`},
		{u + `"ID"`, "42703", `column "ID" does not exist`},
		{`SELECT a.id AS "V" FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY v`,
			"42703", `column "v" does not exist`},
		// The transform's own error comes before the 0A000.
		{u + "id = true", "42883", "operator does not exist: bigint = boolean"},
		{u + "id = 'x'", "22P02", `invalid input syntax for type bigint: "x"`},
		{u + "CAST(id AS boolean)", "42846", "cannot cast type bigint to boolean"},
		{u + "length(id)", "42883", "function length(bigint) does not exist"},
		{u + "b.id", "42P01", `missing FROM-clause entry for table "b"`},
		{u + `"zz".id`, "42P01", `missing FROM-clause entry for table "zz"`},
		{u + "a.id + 1", "0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"},
		{u + "zz.id, nosuch", "42P01", `missing FROM-clause entry for table "zz"`},
		{u + "nosuch, zz.id", "42703", `column "nosuch" does not exist`},
		{u + "nosuch", "42703", `column "nosuch" does not exist`},
		{u + "-nosuch", "42703", `column "nosuch" does not exist`},
		{u + "id + 1", "0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"},
		{u + "-id", "0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"},
		{u + "SUM(id)", "0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"},
		{u + "random()", "0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"},
		// The term is transformed before it is judged.
		{u + "tcp_flag_mask('BOGUS')", "22023", "BOGUS"},
		{u + "no_such_fn(id)", "42883", "no_such_fn"},
		{"SELECT a.id FROM lat_ord a INTERSECT SELECT b.id FROM lat_item b ORDER BY zz.id",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"SELECT a.id FROM lat_ord a EXCEPT SELECT b.id FROM lat_item b ORDER BY zz.id",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"SELECT * FROM lat_ord a UNION ALL SELECT * FROM lat_ord b ORDER BY zz.id",
			"42P01", `missing FROM-clause entry for table "zz"`},
		{"SELECT a.id FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b UNION ALL SELECT c.id FROM lat_ord c ORDER BY c.id",
			"42P01", `missing FROM-clause entry for table "c"`},
		{"SELECT * FROM (" + u + "zz.id) s", "42P01", `missing FROM-clause entry for table "zz"`},
		{"WITH w AS (" + u + "zz.id) SELECT * FROM w", "42P01", `missing FROM-clause entry for table "zz"`},
		{"SELECT a.id FROM lat_ord a WHERE a.id IN (SELECT b.id FROM lat_item b UNION SELECT c.id FROM lat_ord c ORDER BY zz.id)",
			"42P01", `missing FROM-clause entry for table "zz"`},
		// The right arm's names are not the result's.
		{"SELECT id, total FROM lat_ord UNION ALL SELECT id, amount FROM lat_item ORDER BY amount",
			"42703", `column "amount" does not exist`},

		// Controls.
		{u + "id", "", ""},
		{u + "id DESC NULLS FIRST", "", ""},
		{u + "1", "", ""},
		{`SELECT a.id AS "V" FROM lat_ord a UNION ALL SELECT b.id FROM lat_item b ORDER BY "V"`, "", ""},
		{"SELECT id, total FROM lat_ord UNION ALL SELECT id, amount FROM lat_item ORDER BY total", "", ""},
		{"SELECT a.id, a.customer FROM lat_ord a UNION ALL SELECT b.id, b.product FROM lat_item b ORDER BY customer, 1", "", ""},
		{"(SELECT a.id FROM lat_ord a ORDER BY a.id LIMIT 2) UNION ALL SELECT b.id FROM lat_item b", "", ""},
		{"SELECT a.id FROM lat_ord a UNION ALL (SELECT b.id FROM lat_item b ORDER BY b.id LIMIT 2)", "", ""},
	})
}

// Every aggregate the parser knows x every column type of the matrix: the
// argument class is refused where PostgreSQL 17.11 has no overload (#1249,
// #1061). The accepted sets below are written out from the measurement, not
// derived from the rule's own table.
func TestArcBRAggregateArgumentClassMatchesPostgres(t *testing.T) {
	numeric := []string{"c_i32", "c_i64", "c_f32", "c_f64", "c_dec", "c_port", "c_proto", "c_dur"}
	all := []string{"c_bool", "c_i32", "c_i64", "c_f32", "c_f64", "c_str", "c_bytes", "c_ts",
		"c_ipv4", "c_ipv6", "c_cidr", "c_mac", "c_port", "c_proto", "c_dur", "c_uuid", "c_date",
		"c_dec", "c_arr", "c_row", "c_rownest", "c_map", "c_vec"}
	notRow := []string{}
	for _, c := range all {
		if c != "c_row" && c != "c_rownest" {
			notRow = append(notRow, c)
		}
	}
	accepts := map[string][]string{
		"sum": numeric, "avg": numeric, "stddev": numeric, "stddev_samp": numeric,
		"stddev_pop": numeric, "variance": numeric, "var_samp": numeric, "var_pop": numeric,
		"median":   numeric,
		"bool_and": {"c_bool"}, "bool_or": {"c_bool"}, "every": {"c_bool"},
		"min": notRow, "max": notRow,
		"count": all, "approx_distinct": all,
	}
	var cells []brCell
	// mode is an ORDERED-SET aggregate in PostgreSQL: the plain call over a
	// number is the kept DuckDB extension, and refused it is 42809.
	for _, c := range all {
		sql := fmt.Sprintf("SELECT mode(%s) AS v FROM tm", c)
		if contains(numeric, c) {
			cells = append(cells, brCell{sql, "", ""})
		} else {
			cells = append(cells, brCell{sql, "42809", "WITHIN GROUP is required for ordered-set aggregate mode"})
		}
	}
	for agg, ok := range accepts {
		okSet := map[string]bool{}
		for _, c := range ok {
			okSet[c] = true
		}
		for _, c := range all {
			sql := fmt.Sprintf("SELECT %s(%s) AS v FROM tm", agg, c)
			if okSet[c] {
				cells = append(cells, brCell{sql, "", ""})
				continue
			}
			cells = append(cells, brCell{sql, "42883", "function " + agg + "("})
		}
	}
	// string_agg: text, and the kept renderings measured per type at base.
	rendered := map[string]bool{"c_str": true, "c_bool": true, "c_i32": true, "c_i64": true,
		"c_f32": true, "c_f64": true, "c_dec": true, "c_ipv4": true, "c_ipv6": true,
		"c_cidr": true, "c_mac": true, "c_port": true, "c_proto": true, "c_dur": true,
		"c_uuid": true, "c_date": true}
	for _, c := range all {
		switch {
		case rendered[c]:
			cells = append(cells, brCell{"SELECT string_agg(" + c + ", ',') AS v FROM tm", "", ""})
		case c == "c_bytes":
			cells = append(cells, brCell{"SELECT string_agg(c_bytes, ',') AS v FROM tm", "0A000", "string_agg over bytea"})
		default:
			cells = append(cells, brCell{"SELECT string_agg(" + c + ", ',') AS v FROM tm", "42883", "function string_agg("})
		}
		for _, f := range []string{"corr", "covar_samp", "covar_pop"} {
			sql := fmt.Sprintf("SELECT %s(%s, c_f64) AS v FROM tm", f, c)
			if contains(numeric, c) {
				cells = append(cells, brCell{sql, "", ""})
			} else {
				cells = append(cells, brCell{sql, "42883", "function " + f + "("})
			}
		}
		// min_by's ORDERING argument is the ordered position.
		sql := "SELECT min_by(id, " + c + ") AS v FROM tm"
		if c == "c_row" || c == "c_rownest" {
			cells = append(cells, brCell{sql, "42883", "function min_by(bigint, record)"})
		} else {
			cells = append(cells, brCell{sql, "", ""})
		}
	}
	cells = append(cells,
		// PostgreSQL's own sentences, verbatim where the types have its names.
		brCell{"SELECT SUM(customer) AS v FROM lat_ord", "42883", "function sum(text) does not exist"},
		brCell{"SELECT AVG(c_ts) AS v FROM tm", "42883", "function avg(timestamp without time zone) does not exist"},
		brCell{"SELECT bool_and(c_i32) AS v FROM tm", "42883", "function bool_and(integer) does not exist"},
		brCell{"SELECT MIN(c_row) AS v FROM tm", "42883", "function min(record) does not exist"},
		brCell{"SELECT string_agg(c_ts, ',') AS v FROM tm", "42883", "function string_agg(timestamp without time zone, unknown) does not exist"},
		brCell{"SELECT percentile_disc(0.5, c_str) AS v FROM tm", "42809", "WITHIN GROUP is required for ordered-set aggregate percentile_disc"},
		brCell{"SELECT corr(c_str, c_f64) AS v FROM tm", "42883", "function corr(text, double precision) does not exist"},
		// Every position the argument can be written in.
		brCell{"SELECT SUM(DISTINCT customer) AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT id, SUM(customer) AS v FROM lat_ord GROUP BY id", "42883", "function sum(text)"},
		brCell{"SELECT id, SUM(customer) OVER () AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT id FROM lat_ord GROUP BY id HAVING SUM(customer) IS NULL", "42883", "function sum(text)"},
		brCell{"SELECT id FROM lat_ord GROUP BY id ORDER BY SUM(customer)", "42883", "function sum(text)"},
		brCell{"SELECT SUM(customer) FILTER (WHERE id > 1) AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT SUM(customer || 'x') AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT SUM(upper(customer)) AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT SUM(CAST(id AS TEXT)) AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT SUM(x.c) AS v FROM (SELECT customer AS c FROM lat_ord) x", "42883", "function sum(text)"},
		brCell{"WITH w AS (SELECT customer AS c FROM lat_ord) SELECT SUM(c) AS v FROM w", "42883", "function sum(text)"},
		brCell{"SELECT (SELECT SUM(customer) FROM lat_ord) AS v", "42883", "function sum(text)"},
		brCell{"SELECT SUM(CASE WHEN id > 1 THEN customer END) AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT COALESCE(SUM(customer), 'none') AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT SUM(total) + SUM(customer) AS v FROM lat_ord", "42883", "function sum(text)"},
		brCell{"SELECT (min(c_row)).b AS v FROM tm", "42883", "function min(record)"},
		// SQL's `unknown`, resolved against the overloads.
		brCell{"SELECT SUM('5') AS v", "42725", "function sum(unknown) is not unique"},
		brCell{"SELECT AVG(NULL) AS v FROM lat_ord", "42725", "function avg(unknown) is not unique"},
		brCell{"SELECT median('5') AS v FROM lat_ord", "42883", "function median(unknown) does not exist"},
		brCell{"SELECT bool_and('5') AS v FROM lat_ord", "22P02", `invalid input syntax for type boolean: "5"`},
		brCell{"SELECT stddev('t') AS v FROM lat_ord", "22P02", `invalid input syntax for type double precision: "t"`},
		brCell{"SELECT SUM(true) AS v FROM lat_ord", "42883", "function sum(boolean) does not exist"},
		brCell{"SELECT bool_and(1) AS v FROM lat_ord", "42883", "function bool_and(integer) does not exist"},
		brCell{"SELECT bool_or(1.5) AS v FROM lat_ord", "42883", "function bool_or(numeric) does not exist"},
		// Controls: the literal each overload DOES take.
		brCell{"SELECT SUM(1) AS v FROM lat_ord", "", ""},
		brCell{"SELECT SUM(1.5) AS v FROM lat_ord", "", ""},
		brCell{"SELECT min('5') AS v FROM lat_ord", "", ""},
		brCell{"SELECT bool_and('t') AS v FROM lat_ord", "", ""},
		brCell{"SELECT count(NULL) AS v FROM lat_ord", "", ""},
		brCell{"SELECT string_agg('5', ',') AS v FROM lat_ord", "", ""},
		brCell{"SELECT SUM(CAST(customer AS BIGINT)) AS v FROM lat_ord", "", ""},
		brCell{"SELECT SUM(CAST(total AS DECIMAL(9,2))) AS v FROM lat_ord", "", ""},
		brCell{"SELECT SUM(CAST(customer AS INTERVAL)) AS v FROM lat_ord", "", ""},
		brCell{"SELECT max_by(c_row, id) AS v FROM tm", "", ""},
		brCell{"SELECT percentile_cont(0.5, c_f64) AS v FROM tm", "", ""},
		brCell{"SELECT quantile_cont(c_str, 0.5) AS v FROM tm", "42883", "function quantile_cont(text"},
	)
	runBRCells(t, cells)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// The comparison classes, written out from the 18 x 18 measurement: a pair
// PostgreSQL 17.11 compares answers, every other pair is 42883 `operator does
// not exist` (#1073, #1216 item 2). DURATION is measured as the bigint the
// wire declares it.
func TestArcBRComparisonOperandClassesMatchPostgres(t *testing.T) {
	class := map[string]string{
		"c_i32": "n", "c_i64": "n", "c_f32": "n", "c_f64": "n", "c_dec": "n", "c_port": "n",
		"c_proto": "n", "c_dur": "n", "c_str": "text", "c_bytes": "bytea", "c_bool": "bool",
		"c_ts": "time", "c_date": "time", "c_ipv4": "inet", "c_ipv6": "inet", "c_cidr": "inet",
		"c_mac": "mac", "c_uuid": "uuid",
	}
	// The kept text pairs (ADR-0012 §5), written out from the base
	// measurement (br_codex/corpus.json text*/): {direct / JOIN / IN list,
	// IN (subquery)} answer where one conversion answered the same on every
	// arm.
	textKeep := map[string][2]bool{
		"c_i32": {true, true}, "c_i64": {true, true}, "c_f64": {true, true}, "c_dec": {true, true},
		"c_port": {true, true}, "c_proto": {true, true}, "c_dur": {true, true},
		"c_uuid": {true, true}, "c_ipv6": {true, true}, "c_cidr": {true, true},
		"c_date": {true, false}, "c_ts": {true, false}, "c_bool": {true, false},
	}
	var cells []brCell
	for x, cx := range class {
		for y, cy := range class {
			direct := fmt.Sprintf("SELECT id FROM tm WHERE %s = %s", x, y)
			member := fmt.Sprintf("SELECT id FROM tm a WHERE a.%s IN (SELECT b.%s FROM tm b)", x, y)
			keep := [2]bool{}
			if x == "c_str" {
				keep = textKeep[y]
			} else if y == "c_str" {
				keep = textKeep[x]
			}
			if cx == cy {
				keep = [2]bool{true, true}
			}
			for i, sql := range []string{direct, member} {
				if keep[i] {
					cells = append(cells, brCell{sql, "", ""})
				} else {
					cells = append(cells, brCell{sql, "42883", "operator does not exist: "})
				}
			}
		}
	}
	cells = append(cells,
		// PostgreSQL's sentence, operator and operand order, verbatim.
		// A text subquery against a number is the kept pair; with a SET
		// OPERATION in the body it was arm-dependent at base (0 rows single,
		// a cast error on the DAG) and is refused.
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id IN (SELECT product FROM lat_item)", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id NOT IN (SELECT product FROM lat_item)", "", ""},
		brCell{"SELECT o.customer FROM lat_ord o WHERE o.customer IN (SELECT id FROM lat_item)", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id IN (SELECT CAST(id AS TEXT) FROM lat_item)", "", ""},
		brCell{"SELECT a.c_date FROM tm a WHERE a.c_date IN (SELECT CAST(b.c_date AS TEXT) FROM tm b)", "42883", "operator does not exist: date = text"},
		brCell{"SELECT a.c_f32 FROM tm a WHERE a.c_f32 = CAST(a.c_f32 AS TEXT)", "42883", "operator does not exist: real = text"},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id IN (SELECT product FROM lat_item UNION ALL SELECT product FROM lat_item)", "42883", "operator does not exist: bigint = text"},
		// The kept pair in every spelling (bigint against text).
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id = ANY (SELECT product FROM lat_item)", "", ""},
		brCell{"SELECT o.id, o.id IN (SELECT product FROM lat_item) AS v FROM lat_ord o", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id <> o.customer", "", ""},
		// Two plain COLUMNS of the pair as a JOIN key are refused: that is
		// the hash-join key path, broken by arm at base (#615).
		brCell{"SELECT o.id FROM lat_ord o JOIN lat_item i ON o.id = i.product", "42883", "operator does not exist: bigint = text"},
		brCell{"SELECT a.id FROM tm a JOIN tm b ON a.c_i32 = CAST(b.c_i32 AS TEXT)", "", ""},
		brCell{"SELECT NULLIF(customer, id) AS v FROM lat_ord", "", ""},
		// PostgreSQL's sentence, operator and operand order, over a REFUSED
		// pair (integer against date) in every spelling.
		brCell{"SELECT id FROM tm WHERE c_i32 IN (SELECT c_date FROM tm)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_i32 NOT IN (SELECT c_date FROM tm)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_date IN (SELECT c_i32 FROM tm)", "42883", "operator does not exist: date = integer"},
		brCell{"SELECT id FROM tm WHERE c_i32 = ANY (SELECT c_date FROM tm)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_i32 > ALL (SELECT c_date FROM tm)", "42883", "operator does not exist: integer > date"},
		brCell{"SELECT id, c_i32 IN (SELECT c_date FROM tm) AS v FROM tm", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_i32 = (SELECT c_date FROM tm LIMIT 1)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_i32 <> c_date", "42883", "operator does not exist: integer <> date"},
		brCell{"SELECT id FROM tm WHERE c_i32 != c_date", "42883", "operator does not exist: integer <> date"},
		brCell{"SELECT id FROM tm WHERE c_i32 < c_date", "42883", "operator does not exist: integer < date"},
		brCell{"SELECT id FROM tm WHERE c_i32 BETWEEN c_date AND c_date", "42883", "operator does not exist: integer >= date"},
		brCell{"SELECT id FROM tm WHERE c_i32 BETWEEN 1 AND c_date", "42883", "operator does not exist: integer <= date"},
		brCell{"SELECT id FROM tm WHERE c_i32 IS DISTINCT FROM c_date", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_i32 IS NOT DISTINCT FROM c_date", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE c_i32 IN (c_date, 1)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT a.id FROM tm a JOIN tm b ON a.c_i32 = b.c_date", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT id FROM tm WHERE (c_i64, c_i32) IN (SELECT id, c_date FROM tm)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT CASE c_i32 WHEN c_date THEN 1 END AS v FROM tm", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT c_i32 FROM tm GROUP BY c_i32 HAVING c_i32 = MAX(c_date)", "42883", "operator does not exist: integer = date"},
		brCell{"SELECT o.id FROM lat_ord o WHERE (o.id > 1) = 1", "42883", "operator does not exist: boolean = integer"},
		brCell{"SELECT 1 = true AS v", "42883", "operator does not exist: integer = boolean"},
		brCell{"SELECT 1 IS DISTINCT FROM 1 = true AS v", "42883", "operator does not exist: integer = boolean"},
		brCell{"SELECT id FROM lat_ord WHERE id = true", "42883", "operator does not exist: bigint = boolean"},
		brCell{"SELECT c_mac = c_str AS v FROM tm", "42883", "operator does not exist: macaddr = text"},
		brCell{"SELECT c_arr = c_str AS v FROM tm", "42883", "operator does not exist: array = text"},
		// Controls: every class PostgreSQL does compare within, an unknown
		// literal on either side, and a subquery of the same class.
		brCell{"SELECT o.id FROM lat_ord o WHERE o.total = o.id", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id IN (SELECT amount FROM lat_item)", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.id IN (SELECT order_id FROM lat_item)", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.customer = 'Bob'", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.customer = NULL", "", ""},
		brCell{"SELECT c_ts = c_date AS v FROM tm", "", ""},
		brCell{"SELECT c_ipv4 = c_cidr AS v FROM tm", "", ""},
		brCell{"SELECT c_dur = c_i64 AS v FROM tm", "", ""},
		brCell{"SELECT c_arr = c_arr AS v FROM tm", "", ""},
		brCell{"SELECT c_arr[1] = c_str AS v FROM tm", "", ""},
		brCell{"SELECT c_map['k'] = c_i64 AS v FROM tm", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE o.total > 0 AND o.customer LIKE 'A%'", "", ""},
		// The recorded literal supersets (ADR-0012 §5): an unquoted number
		// against text reads the number's text, against a timestamp the epoch
		// instant; a function whose registered return is text is not a type
		// this rule reads.
		brCell{"SELECT o.id FROM lat_ord o WHERE o.customer IN (1, 2)", "", ""},
		brCell{"SELECT o.id FROM lat_ord o WHERE (o.id, o.customer) = (1, 2)", "", ""},
		brCell{"SELECT id FROM tm WHERE c_ts >= 1700000000000", "", ""},
		brCell{"SELECT id FROM tm WHERE c_str = TRUE", "", ""},
		brCell{"SELECT id FROM tm WHERE current_date = CAST(c_ts AS DATE)", "", ""},
		brCell{"SELECT id FROM tm WHERE c_ipv4 = int_to_ip(c_i64)", "", ""},
		// A CAST to a parameterized or unrecognised name is not TEXT.
		brCell{"SELECT id FROM tm a WHERE a.c_i64 IN (SELECT CAST(b.c_f64 AS DECIMAL(9,2)) FROM tm b)", "", ""},
		brCell{"SELECT id FROM tm WHERE c_f32 IN (CAST(3.1 AS DECIMAL(9,2)), 7.1)", "", ""},
		brCell{"SELECT id FROM tm WHERE c_ipv4 = CAST(c_str AS INET)", "", ""},
	)
	runBRCells(t, cells)
}

// A searched CASE's WHEN is a boolean context in every clause (#1216 item 1),
// measured on 17.11.
func TestArcBRCaseWhenIsABooleanContextInEveryClause(t *testing.T) {
	runBRCells(t, []brCell{
		{"SELECT id FROM lat_ord WHERE CASE WHEN upper(customer) THEN 1 ELSE 0 END = 1",
			"42804", "argument of CASE/WHEN must be type boolean, not type text"},
		{"SELECT id FROM lat_ord o JOIN lat_item i ON CASE WHEN i.product THEN 1 END = o.id",
			"42804", "argument of CASE/WHEN must be type boolean, not type text"},
		{"SELECT id FROM lat_ord GROUP BY id HAVING CASE WHEN MAX(customer) THEN 1 END = 1",
			"42804", "argument of CASE/WHEN must be type boolean, not type text"},
		{"SELECT id FROM lat_ord ORDER BY CASE WHEN customer THEN 1 END",
			"42804", "argument of CASE/WHEN must be type boolean, not type text"},
		{"SELECT id FROM lat_ord WHERE CASE WHEN id > 1 THEN 1 ELSE 0 END = 1", "", ""},
		{"SELECT id FROM lat_ord WHERE CASE customer WHEN 'Bob' THEN 1 ELSE 0 END = 1", "", ""},
		{"SELECT id FROM lat_ord GROUP BY id HAVING CASE WHEN MIN(id) THEN 1 END = 1",
			"42804", "argument of CASE/WHEN must be type boolean, not type bigint"},
	})
}

// A container folded with something it cannot be is refused (#1060), and two
// ROWs of different shapes are not compared (#1060/#1065). Verdicts measured
// on 17.11 over typed columns of the same PostgreSQL types.
func TestArcBRContainerFoldsAreRefused(t *testing.T) {
	runBRCells(t, []brCell{
		{"SELECT COALESCE(c_row, c_arr) AS v FROM tm", "42804", "COALESCE types record(a text, b bigint) and array cannot be matched"},
		{"SELECT COALESCE(c_row, id) AS v FROM tm", "42804", "COALESCE types record(a text, b bigint) and bigint cannot be matched"},
		{"SELECT COALESCE(c_arr, CAST(id AS TEXT)) AS v FROM tm", "42804", "COALESCE types array and text cannot be matched"},
		{"SELECT COALESCE(c_arr, id) AS v FROM tm", "42804", "COALESCE types array and bigint cannot be matched"},
		{"SELECT IFNULL(c_map, c_str) AS v FROM tm", "42804", "COALESCE types map and text cannot be matched"},
		{"SELECT GREATEST(c_vec, c_f64) AS v FROM tm", "42804", "GREATEST types vector and double precision cannot be matched"},
		{"SELECT LEAST(c_row, c_rownest) AS v FROM tm", "42846", "LEAST could not convert type record(x bigint) to record(a text, b bigint)"},
		{"SELECT COALESCE(c_row, c_rownest) AS v FROM tm", "42846", "COALESCE could not convert type record(x bigint) to record(a text, b bigint)"},
		// A CASE's ELSE is read first, as the server does.
		{"SELECT CASE WHEN id = 1 THEN c_row ELSE CAST(id AS TEXT) END AS v FROM tm", "42804", "CASE types text and record(a text, b bigint) cannot be matched"},
		{"SELECT CASE WHEN id = 1 THEN c_arr ELSE CAST(id AS TEXT) END AS v FROM tm", "42804", "CASE types text and array cannot be matched"},
		{"SELECT CASE WHEN id = 1 THEN c_row ELSE c_rownest END AS v FROM tm", "42846", "CASE/WHEN could not convert type record(a text, b bigint) to record(x bigint)"},
		{"SELECT COALESCE(c_row, 'x') AS v FROM tm", "22P02", `malformed record literal: "x"`},
		{"SELECT GREATEST(c_arr, 'x') AS v FROM tm", "22P02", `malformed array literal: "x"`},
		{"SELECT GREATEST(c_arr, '{x}') AS v FROM tm", "0A000", "a quoted ARRAY literal in GREATEST is not supported"},
		{"SELECT NULLIF(c_row, c_rownest) AS v FROM tm", "42804", "cannot compare record types with different numbers of columns"},
		{"SELECT c_row = c_rownest AS v FROM tm", "42804", "cannot compare record types with different numbers of columns"},
		{"SELECT NULLIF(c_arr, c_str) AS v FROM tm", "42883", "operator does not exist: array = text"},
		// Controls: one container kind, one shape, a NULL arm.
		{"SELECT COALESCE(c_row, c_row) AS v FROM tm", "", ""},
		{"SELECT CASE WHEN id = 1 THEN c_row ELSE NULL END AS v FROM tm", "", ""},
		{"SELECT LEAST(c_row, c_row) AS v FROM tm", "", ""},
		{"SELECT COALESCE(c_arr, c_arr) AS v FROM tm", "", ""},
		{"SELECT c_row = c_row AS v FROM tm", "", ""},
		{"SELECT COALESCE(c_str, 'x') AS v FROM tm", "", ""},
		{"SELECT COALESCE(c_i32, c_f64) AS v FROM tm", "", ""},
	})
}
