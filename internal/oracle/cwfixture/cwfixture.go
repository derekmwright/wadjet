// SPDX-License-Identifier: MIT

// Package cwfixture is arc CW's container fixture: one table holding an ARRAY
// of every element class a client decodes differently (int4, text, timestamp,
// date, bool, a network type, uuid), a ROW carrying a timestamp field, a MAP,
// and an ARRAY of ROW — with the values that separate PostgreSQL's text output
// from any other rendering (an element needing quotes, the text "NULL" beside
// a real NULL, an empty string, an empty array, a NULL element, a NULL field).
//
// The gates on every door (pgwire, the CLI, the HTTP and async doors, the
// distributed arms) load the same rows, and PGDDL is the same table in
// PostgreSQL 17 — the container this arc measured its expectations on.
package cwfixture

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Table is the fixture's name.
const Table = "cw"

func elem(t parquet.TypeID) *parquet.Column {
	return &parquet.Column{Name: "element", Type: t, Nullable: true}
}

// RowFields is the ROW column's declaration: PostgreSQL's `cwr` composite.
func RowFields() []parquet.Column {
	return []parquet.Column{
		{Name: "x", Type: parquet.TypeInt32, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "t", Type: parquet.TypeTimestamp, Nullable: true},
	}
}

// Schema is the fixture's declaration.
func Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ai", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeInt32)},
		{Name: "at", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeString)},
		{Name: "ats", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeTimestamp)},
		{Name: "ad", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeDate)},
		{Name: "ab", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeBool)},
		{Name: "aip", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeIPv4)},
		{Name: "au", Type: parquet.TypeArray, Nullable: true, ElementType: elem(parquet.TypeUUID)},
		{Name: "r", Type: parquet.TypeRow, Nullable: true, Fields: RowFields()},
		{Name: "m", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
		{Name: "ar", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{
			Name: "element", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "x", Type: parquet.TypeInt32, Nullable: true},
				{Name: "s", Type: parquet.TypeString, Nullable: true},
			}}},
	}}
}

// The instants the rows carry, as the epoch milliseconds a TIMESTAMP is boxed
// as: 2024-06-15 12:30:45.5, 2024-06-15 12:30:45 and 1999-12-31 23:59:59.
const (
	TS1 = int64(1718454645500)
	TS2 = int64(1718454645000)
	TS3 = int64(946684799000)
)

// Rows is the fixture's three rows.
func Rows() []map[string]any {
	return []map[string]any{
		{
			"id":  int64(1),
			"ai":  []any{int32(1), int32(2), int32(3)},
			"at":  []any{"a b", "c", "", "NULL", nil, `x"y`, "a,b", "{}"},
			"ats": []any{TS1, nil},
			"ad":  []any{"2024-01-02"},
			"ab":  []any{true, false, nil},
			"aip": []any{"1.2.3.4", "10.0.0.1"},
			"au":  []any{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
			"r":   map[string]any{"x": int32(7), "s": "x y", "t": TS2},
			"m":   map[string]any{"a": int64(1), "b": int64(2)},
			"ar": []any{
				map[string]any{"x": int32(1), "s": "a"},
				map[string]any{"x": int32(2), "s": "b c"},
			},
		},
		{
			"id": int64(2), "ai": []any{}, "at": nil, "ats": []any{}, "ad": nil,
			"ab": []any{}, "aip": nil, "au": []any{}, "r": nil, "m": map[string]any{}, "ar": []any{},
		},
		{
			"id":  int64(3),
			"ai":  []any{nil, int32(5)},
			"at":  []any{"z"},
			"ats": []any{TS3},
			"ad":  []any{"1970-01-01", nil},
			"ab":  []any{true},
			"aip": []any{},
			"au":  nil,
			"r":   map[string]any{"x": nil, "s": "", "t": nil},
			"m":   nil,
			"ar":  nil,
		},
	}
}

// PGDDL builds the same table in PostgreSQL 17: IPv4 is `inet` there and the
// MAP column has no counterpart, so it is absent.
const PGDDL = `DROP TABLE IF EXISTS cw; DROP TYPE IF EXISTS cwr; DROP TYPE IF EXISTS cwe;
CREATE TYPE cwr AS (x int, s text, t timestamp);
CREATE TYPE cwe AS (x int, s text);
CREATE TABLE cw (id bigint, ai int[], at text[], ats timestamp[], ad date[], ab bool[], aip inet[], au uuid[], r cwr, ar cwe[]);
INSERT INTO cw VALUES
 (1, '{1,2,3}', ARRAY['a b','c','','NULL',NULL,'x"y','a,b','{}'], ARRAY[TIMESTAMP '2024-06-15 12:30:45.5', NULL], '{2024-01-02}', '{t,f,NULL}', '{1.2.3.4,10.0.0.1}', '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}', ROW(7,'x y',TIMESTAMP '2024-06-15 12:30:45')::cwr, ARRAY[ROW(1,'a')::cwe, ROW(2,'b c')::cwe]),
 (2, '{}', NULL, '{}', NULL, '{}', NULL, '{}', NULL, '{}'),
 (3, '{NULL,5}', '{z}', ARRAY[TIMESTAMP '1999-12-31 23:59:59'], ARRAY[DATE '1970-01-01', NULL], '{t}', '{}', NULL, ROW(NULL,'',NULL)::cwr, NULL);`
