// SPDX-License-Identifier: MIT

package parquet

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A NEW column spelled INTERVAL is refused by the shared declaration resolver
// every column-creating door uses (SQL CREATE TABLE, HTTP and gRPC table
// creation; #1713d), at any depth; a ROW field NAMED interval and DURATION are
// not. ResolveColumn — the CAST machinery's and pgwire's grammar — keeps the
// alias, and ParseTypeID still names DURATION for it.
func TestDeclaredColumnRefusesANewIntervalColumn(t *testing.T) {
	for _, c := range []struct {
		typ   string
		state string
	}{
		{"INTERVAL", "0A000"},
		{"interval", "0A000"},
		{" Interval ", "0A000"},
		{"ARRAY(INTERVAL)", "0A000"},
		{"ARRAY(ARRAY(INTERVAL))", "0A000"},
		{"MAP(STRING, INTERVAL)", "0A000"},
		{"MAP(INTERVAL, STRING)", "0A000"},
		{"ROW(a INT64, b INTERVAL)", "0A000"},
		{"STRUCT(a ROW(b INTERVAL))", "0A000"},
		{"DURATION", ""},
		{"ARRAY(DURATION)", ""},
		{"ROW(interval INT64)", ""},
		{"TEXT", ""},
	} {
		_, err := DeclaredColumn("c", c.typ, true)
		if got := sqlerr.StateOf(err); got != c.state || (c.state == "" && err != nil) {
			t.Errorf("DeclaredColumn(%q): %v (SQLSTATE %q), want %q", c.typ, err, got, c.state)
		}
	}
	if col, err := ResolveColumn("c", "INTERVAL"); err != nil || col.Type != TypeDuration {
		t.Errorf("ResolveColumn(INTERVAL) = %v, %v; the CAST grammar keeps the DURATION alias", col.Type, err)
	}
}

// A file whose schema carries Parquet's INTERVAL annotation is refused at
// every reader open: no decoding arm reads its months / days / milliseconds,
// so its bytes would be a string or a bytes column (#1713d).
func TestReaderRefusesTheIntervalAnnotation(t *testing.T) {
	flba := PhysicalFixedLenByteArray
	ivl := ConvertedInterval
	utf8 := ConvertedUTF8
	ba := PhysicalByteArray
	refused := []SchemaElement{{Name: "schema", NumChildren: 1}, {Name: "iv", Type: &flba, TypeLength: 12, ConvertedType: &ivl}}
	if err := refuseIntervalAnnotation(refused); sqlerr.StateOf(err) != "0A000" {
		t.Errorf("an INTERVAL-annotated column: %v, want 0A000", err)
	}
	fine := []SchemaElement{{Name: "schema", NumChildren: 1}, {Name: "s", Type: &ba, ConvertedType: &utf8}}
	if err := refuseIntervalAnnotation(fine); err != nil {
		t.Errorf("a UTF8 column: %v", err)
	}
}
