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
