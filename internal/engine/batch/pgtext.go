// SPDX-License-Identifier: MIT

package batch

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file is PostgreSQL's TEXT OUTPUT for a value under its declared
// column — array_out's `{…}` with its quoting, record_out's `(…)`, a TIMESTAMP
// or DATE element as its text — and it is the ONE renderer every door uses:
// pgwire's text format, the CLI's table and CSV forms, and CAST(container AS
// text). It lived in pgwire until arc CW; the CLI printed Go's `[1 2 3]` /
// `map[x:1 y:q]` for the same values psql printed as `{1,2,3}` / `(1,q)`,
// because two renderers drift (#1250, #1268).
//
// It takes the DECLARATION because the boxed value alone cannot say what it
// is: a bare []any is an ARRAY or a MAP's entry list, and an int64 element is
// a bigint or a TIMESTAMP's epoch milliseconds. col may be nil — every helper
// then degrades to a schema-free rendering rather than refusing.

// FormatPGText is PostgreSQL's text output of val: every scalar the way the
// pgwire text format always sent it, plus PostgreSQL's composite/array text
// forms for ROW and ARRAY (#471), using col — the column's declared
// parquet.Column — for a ROW's field order and an ARRAY's or MAP's element.
// col may be nil (a caller with no declaration): every nested-type helper
// below degrades to a schema-free rendering instead of refusing, per each
// helper's own doc.
func FormatPGText(val any, col *parquet.Column) string {
	// A TIMESTAMP is boxed as epoch MILLISECONDS, and its rendering is the
	// caller's everywhere a top-level column is sent (sendDataRowFormatted's
	// default arm reaches for FormatTimestamp). Nothing did that for a
	// timestamp INSIDE a container, so a `timestamp[]` element came out as the
	// raw number — tolerable while the column declared text and a promise the
	// moment it declares OID 1115 (#992).
	if col != nil && col.Type == parquet.TypeTimestamp {
		if ms, ok := val.(int64); ok {
			return FormatTimestamp(ms)
		}
	}
	// A DATE is boxed as its text by a vector, but an element an expression
	// built before it reached one is still the day count — an int32 from a
	// constructor's DATE literal, an int64 from a column reference (ColRef
	// widens every integer storage) — and under its declaration it is a date
	// either way (#1268's sibling for DATE). The DECLARATION picks the arm and
	// the box may be either storage width; before arc CW round 3 only the int32
	// was taken and `CAST(ARRAY[d] AS TEXT)` printed `{19724}`.
	if col != nil && col.Type == parquet.TypeDate {
		switch days := val.(type) {
		case int32:
			return FormatDate(days)
		case int64:
			if days >= math.MinInt32 && days <= math.MaxInt32 {
				return FormatDate(int32(days))
			}
		}
	}
	switch tv := val.(type) {
	case []float32:
		// VECTOR: a wadjet extension with no PostgreSQL array/composite
		// form to match (unlike ARRAY/ROW below, which this bracket
		// convention used to also apply, incorrectly — see #471). Left as
		// is: square brackets, no space, the established display for this
		// type.
		var buf strings.Builder
		buf.WriteByte('[')
		for i, f := range tv {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("%g", f))
		}
		buf.WriteByte(']')
		return buf.String()
	case []any:
		// wadjet's boxing for BOTH ARRAY and MAP (a MAP's entries arrive as
		// one 2-field ROW per key, in the sorted-key order mapEntryRows
		// already put them in) — formatPGArrayOrMap tells them apart using
		// col.Type when it is known.
		return formatPGArrayOrMap(tv, col)
	case map[string]any:
		// wadjet's boxing for ROW.
		return formatPGComposite(tv, col)
	case bool:
		// PostgreSQL text format for bool is t/f, not true/false.
		if tv {
			return "t"
		}
		return "f"
	case []byte:
		// wadjet's boxing for BYTES, and PostgreSQL's text form for the
		// bytea it now declares (OID 17): `\x` then LOWERCASE hex, which is
		// byteaout under the default bytea_output = hex — the setting
		// expr.pgcompat already reports to a client that asks. This arm used
		// to be missing, so the value fell to the %v default and went out as
		// "[255 254 0 65]" (#570).
		//
		// The hex form also removes a hazard the raw bytes carried: a
		// non-UTF-8 value with an embedded NUL cannot appear in a
		// PostgreSQL text-format field at all, and libpq's PQgetvalue —
		// a NUL-terminated char* — truncates at it, so pgx read four bytes
		// where psql read two. Hex is pure ASCII.
		return `\x` + hex.EncodeToString(tv)
	case float64:
		return FormatPGFloat(tv, 64)
	case float32:
		return FormatPGFloat(float64(tv), 32)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatPGArrayOrMap renders wadjet's []any boxing for ARRAY and MAP.
// col.Type tells them apart when it is known; without it (col is nil, or
// declares neither) this renders as an ARRAY, which is right far more often
// — MAP is the wadjet extension here, ARRAY is the PostgreSQL type.
//
// Review note (N2, adversarial review of #464/#471, not fixed here): this
// default is also a genuine DISPLAY divergence, not just a best-effort
// guess. The exact same MAP value renders two different text shapes
// depending on whether nestedSchema happened to resolve col for this
// position — formatPGMap's "{k1: v1, k2: v2}" when it did, this function's
// ARRAY-shaped "{elem1,elem2}" (each entry's own 2-field ROW rendered as a
// composite, comma-joined) when it did not. A client could see either
// shape for the same query depending on the coord vs. legacy path, or a
// renamed/computed MAP column landing outside both nestedSchemaByName and
// nestedColumnSchemas' resolution. Fixing it needs a way to tell "this is
// unambiguously a MAP, just with no known field names" from "this could be
// either" — which the boxed value alone (a bare []any, identical for both
// types) does not carry, and no col to consult.
func formatPGArrayOrMap(elems []any, col *parquet.Column) string {
	if col != nil && col.Type == parquet.TypeMap {
		return formatPGMap(elems, col)
	}
	var elemCol *parquet.Column
	if col != nil && col.ElementType != nil {
		elemCol = col.ElementType
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range elems {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(formatPGArrayElement(e, elemCol))
	}
	b.WriteByte('}')
	return b.String()
}

// formatPGArrayElement renders one ARRAY element in PostgreSQL's array text
// form: a genuine NULL is the bare, unquoted keyword (never the four
// letters as text — QuotePGArrayElement is what quotes THAT case, below); anything
// else is rendered by its own type, recursively, and then quoted if
// QuotePGArrayElement says the result needs it.
func formatPGArrayElement(val any, col *parquet.Column) string {
	if val == nil {
		return "NULL"
	}
	// An element that is itself an ARRAY is a further DIMENSION, and
	// array_out writes it bare — `{{1,2},{3,4}}`, never `{"{1,2}","{3,4}"}`.
	// Only a MAP (an entry list rendered `{k: v}`) or a composite needs the
	// quoting below.
	if inner, ok := val.([]any); ok && (col == nil || col.Type == parquet.TypeArray) {
		return formatPGArrayOrMap(inner, col)
	}
	return QuotePGArrayElement(FormatPGText(val, col))
}

// QuotePGArrayElement applies PostgreSQL's array-element quoting: wrap in double
// quotes — backslash-escaping an embedded double quote or backslash — when
// the text is empty, reads case-insensitively as the NULL keyword, or
// contains a character the array parser would otherwise treat as
// structural.
//
// Verified against live PostgreSQL 17 (array_out). This input (a plain
// value, a value needing quoting for each trigger character below, an
// empty string, and the literal text "NULL"):
//
//	ARRAY['plain','has,comma','has"quote','has\backslash','has space',
//	      '','NULL','has{brace}','has(paren)']
//
// renders as:
//
//	{plain,"has,comma","has\"quote","has\\backslash","has space","","NULL",
//	 "has{brace}",has(paren)}
//
// Note parentheses do NOT trigger array quoting (last element, unquoted) —
// that is a COMPOSITE rule, QuotePGCompositeField below, not an array one.
func QuotePGArrayElement(s string) string {
	if !pgArrayNeedsQuoting(s) {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		if r == '"' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}

func pgArrayNeedsQuoting(s string) bool {
	if s == "" || strings.EqualFold(s, "NULL") {
		return true
	}
	for _, r := range s {
		switch r {
		// PostgreSQL's array_isspace (arrayfuncs.c) treats seven bytes as
		// whitespace: space, tab, newline, CR, vertical tab, and form feed
		// — the last two (\v, \f) were missing here, so a leading or
		// trailing VT/FF silently dropped its quoting on a round trip
		// instead of coming back with it, same as any other array-breaking
		// character would.
		case ',', '{', '}', '"', '\\', ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

// formatPGMap renders a MAP's entries — already in the sorted-key order
// mapEntryRows (internal/engine/batch) put them in when the row was
// written, which this function relies on rather than re-deriving: ranging
// a Go map here would reintroduce the exact randomization mapEntryRows
// exists to avoid — as "{k1: v1, k2: v2}". MAP is a wadjet extension with no
// PostgreSQL form to match, so this keeps the bracket convention already in
// use rather than inventing a new one; the fix is the two real defects
// map-range-random order (gone: this walks the incoming SLICE, never a Go
// map) and a NULL value printing as Go's "<nil>" (renders as the unquoted
// NULL keyword now, matching QuotePGArrayElement's convention above it).
//
// col, when known, is a TypeMap column: like ARRAY, its per-entry structure
// lives under ElementType — not Fields directly (a MAP column's Fields is
// unused; ElementType points at a TypeRow column carrying the two entry
// fields, the same declaration shape internal/engine/batch and every other
// nested-type schema in this codebase uses) — whose two Fields name which
// of each entry's two keys is "key" and which is "value". mapEntryRows
// writes those same two names, defaulting to "key"/"value" when the schema
// does not override them, which this falls back to as well when col is nil
// or its structure does not resolve.
func formatPGMap(entries []any, col *parquet.Column) string {
	keyName, valName := "key", "value"
	var keyCol, valCol *parquet.Column
	if col != nil && col.ElementType != nil && len(col.ElementType.Fields) == 2 {
		entryFields := col.ElementType.Fields
		keyName, valName = entryFields[0].Name, entryFields[1].Name
		keyCol, valCol = &entryFields[0], &entryFields[1]
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range entries {
		if i > 0 {
			b.WriteString(", ")
		}
		entry, ok := e.(map[string]any)
		if !ok {
			// Not the 2-field entry shape mapEntryRows produces — render
			// whatever it is rather than panic on a type assertion.
			b.WriteString(FormatPGText(e, nil))
			continue
		}
		b.WriteString(formatPGMapEntryField(entry[keyName], keyCol))
		b.WriteString(": ")
		b.WriteString(formatPGMapEntryField(entry[valName], valCol))
	}
	b.WriteByte('}')
	return b.String()
}

func formatPGMapEntryField(val any, col *parquet.Column) string {
	if val == nil {
		return "NULL"
	}
	return FormatPGText(val, col)
}

// formatPGComposite renders a ROW value in PostgreSQL's composite text
// form: parenthesized, comma-separated, in the column's DECLARED field
// order (col.Fields, when known) — with a NULL field as an EMPTY slot
// between commas, never the word NULL (that is an array/MAP convention,
// not a composite one).
//
// Without a declared order (col is nil, or names a different number of
// fields than the value has keys — a computed ROW expression on the legacy
// query path, or a catalog name collision nestedColumnSchemas already
// distrusts for a different reason), the keys are sorted instead: not
// PostgreSQL's answer, but deterministic, which random Go map iteration was
// not.
//
// Verified against live PostgreSQL 17 (record_out) on a 3-field composite:
// ROW(1,NULL,3) renders "(1,,3)"; the NULL middle field is nothing between
// the two commas, not the word NULL.
func formatPGComposite(row map[string]any, col *parquet.Column) string {
	names, fieldCols := compositeFieldOrder(row, col)
	var b strings.Builder
	b.WriteByte('(')
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v, ok := row[name]
		if !ok || v == nil {
			continue // NULL: an empty slot, not text.
		}
		b.WriteString(QuotePGCompositeField(FormatPGText(v, fieldCols[i])))
	}
	b.WriteByte(')')
	return b.String()
}

// compositeFieldOrder returns row's field names in DISPLAY order, and the
// declared Column for each (nil where col did not cover it) for recursive
// rendering.
func compositeFieldOrder(row map[string]any, col *parquet.Column) ([]string, []*parquet.Column) {
	if col != nil && len(col.Fields) == len(row) {
		names := make([]string, len(col.Fields))
		cols := make([]*parquet.Column, len(col.Fields))
		for i := range col.Fields {
			names[i] = col.Fields[i].Name
			cols[i] = &col.Fields[i]
		}
		return names, cols
	}
	names := make([]string, 0, len(row))
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	return names, make([]*parquet.Column, len(names))
}

// QuotePGCompositeField applies PostgreSQL's composite-field quoting: wrap in
// double quotes — doubling an embedded double quote, backslash-escaping an
// embedded backslash — when the text is empty or contains a character the
// composite parser would otherwise treat as structural.
//
// Verified against live PostgreSQL 17 (record_out) field by field:
// braces do NOT trigger composite quoting ("has{brace}" prints unquoted,
// unlike an array element with the same content — QuotePGArrayElement above) and,
// a genuine PostgreSQL quirk this mirrors rather than improves on, the
// literal text "NULL" is not quoted either — ROW(1,'NULL',3) prints
// "(1,NULL,3)", indistinguishable in the composite's OWN text from what a
// true NULL field renders as ONE LEVEL UP, in an array or a bare column:
// an empty slot here, not this string.
func QuotePGCompositeField(s string) string {
	if !pgCompositeNeedsQuoting(s) {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`""`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func pgCompositeNeedsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for _, r := range s {
		switch r {
		// record_in/record_out's isspace (rowtypes.c, same set as the C
		// library isspace it defers to for this) treats \v and \f as
		// whitespace alongside space/tab/newline/CR — the same gap as
		// pgArrayNeedsQuoting above, and the same fix.
		case ',', '(', ')', '"', '\\', ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

// FormatPGFloat is FormatFloat8Text, the ONE renderer for a float's text
// form (main's arc VL, #1252): a container's float element renders exactly as
// the scalar does on the wire and in a TEXT column.
func FormatPGFloat(v float64, bits int) string { return FormatFloat8Text(v, bits) }

// DeclaredValue is val as a vector of col's declared type holds and reads it
// back: every leaf in the box its type's vector gives (a DATE, an address, a
// UUID, a DECIMAL as its text; a TIMESTAMP as its epoch milliseconds, which
// FormatPGText renders under the declaration). It is how a site holding an
// EXPRESSION's box — not a vector's — renders it under the declaration the
// planner gave the expression (arc CW round 3): the box of an element an
// expression built is whatever its kernel produced (an int64 day count, an
// int64 address), and the vector's own writer is the one place that accepts
// every storage width a type has. A box the declared type cannot hold fails
// the write loudly (TypeMismatchError), exactly as the same value projected.
func DeclaredValue(val any, col *parquet.Column) any {
	if val == nil || col == nil {
		return val
	}
	v := NewColumnVector(*col, 1)
	v.SetValue(0, val)
	v.Len = 1
	return v.GetValue(0)
}

// VectorDecl reconstructs a declaration from a VECTOR, for a caller whose
// batch schema lost it (exec.Project's field-path fallback) or that holds only
// the vector (a CAST of a container column to text, which renders it through
// FormatPGText and needs its element's type — arc CW). It recurses into
// ARRAY/MAP elements and ROW children: an earlier version copied
// Type/Scale/Dimension only, so the pooled output vector for a nested field
// came back with nil Child / Children and every value inside it was silently
// dropped.
func VectorDecl(name string, v *Vector) parquet.Column {
	col := parquet.Column{
		Name:      name,
		Type:      v.Type,
		Nullable:  true,
		Scale:     v.DecimalData.Scale,
		Dimension: v.VectorDim,
	}
	switch v.Type {
	case TypeArray, TypeMap:
		if v.Child != nil {
			el := VectorDecl("element", v.Child)
			col.ElementType = &el
		}
	case TypeRow:
		for i, ch := range v.Children {
			fn := fmt.Sprintf("f%d", i)
			if i < len(v.FieldNames) {
				fn = v.FieldNames[i]
			}
			col.Fields = append(col.Fields, VectorDecl(fn, ch))
		}
	}
	return col
}
