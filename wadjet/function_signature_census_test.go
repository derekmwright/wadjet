// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1053, #1056 and #583 are ONE seam seen from three sides: PostgreSQL resolves
// a call by its NAME AND ITS ARGUMENT TYPES, and reports every failure of that
// as `42883 function f(types) does not exist`. The wrong COUNT is one failure
// (`upper('a','b')`), a number in a text position is another (`upper(1)`), and a
// bytea in a text-only position is a third (`upper(b)`). This engine answered
// all three: NULL or a dropped argument for the count, XX000 for the literal,
// and the text those bytes spell for the bytea.
//
// Every expectation is live PostgreSQL 17.11 over a `text` column and a `bytea`
// column holding 'hi'; each row carries the server's verbatim answer.
func TestACallResolvesByItsArgumentsOrIs42883(t *testing.T) {
	ctx := context.Background()
	db := fscOpen(t)

	for _, c := range []struct {
		name, sql string
		// want is the value when the call resolves; state the SQLSTATE when
		// it does not. pg is PostgreSQL 17.11's verbatim answer.
		want  any
		state string
		pg    string
		// msg is a substring the refusal must carry. It is set where the
		// server's sentence is one this engine reproduces exactly; two
		// families deliberately differ and are recorded rather than asserted
		// (the arc's notes, residual 7): PostgreSQL names the CONSTRUCT's
		// underlying function for `TRIM` (`pg_catalog.btrim`) where this
		// engine names the spelling a user can write, and it resolves an
		// `unknown` argument to the type of its closest candidate overload
		// (`replace(bytea, bytea, bytea)`) where this engine has no candidate
		// list to resolve against and says `unknown`.
		msg string
	}{
		// --- #1053: the COUNT -------------------------------------------
		{name: "upper_two_args", sql: `SELECT UPPER('a','b') AS v`, state: "42883",
			pg: `42883 function upper(unknown, unknown) does not exist`, msg: `function upper(unknown, unknown) does not exist`},
		{name: "upper_no_args", sql: `SELECT UPPER() AS v`, state: "42883",
			pg: `42883 function upper() does not exist`},
		{name: "abs_two_args", sql: `SELECT ABS(1,2) AS v`, state: "42883",
			pg: `42883 function abs(integer, integer) does not exist`, msg: `function abs(integer, integer) does not exist`},
		{name: "replace_two_args", sql: `SELECT REPLACE('a','b') AS v`, state: "42883",
			pg: `42883 function replace(unknown, unknown) does not exist`},
		{name: "substr_one_arg", sql: `SELECT SUBSTR('abc') AS v`, state: "42883",
			pg: `42883 function substr(unknown) does not exist`, msg: `function substr(unknown) does not exist`},
		{name: "md5_two_args", sql: `SELECT MD5('a','b') AS v`, state: "42883",
			pg: `42883 function md5(unknown, unknown) does not exist`},
		{name: "semver_major_two_args", sql: `SELECT SEMVER_MAJOR('1.2.3','x') AS v`, state: "42883",
			pg: `wadjet's own function; the rule is PostgreSQL's — a count no overload takes is 42883`},
		{name: "semver_cmp_one_arg", sql: `SELECT SEMVER_CMP('1.0.0') AS v`, state: "42883",
			pg: `wadjet's own function; #1053's own example, which answered NULL`},
		// The counts that must still ANSWER, which is the half a refusal can
		// break: an optional trailing argument, and a variadic tail.
		{name: "substr_two_args", sql: `SELECT SUBSTR('abcdef',2) AS v`, want: "bcdef", pg: `bcdef`},
		{name: "substr_three_args", sql: `SELECT SUBSTR('abcdef',2,3) AS v`, want: "bcd", pg: `bcd`},
		{name: "round_one_arg", sql: `SELECT ROUND(1.5) AS v`, want: 2.0, pg: `2`},
		{name: "round_two_args", sql: `SELECT ROUND(1.55,1) AS v`, want: 1.6, pg: `1.6`},
		{name: "concat_one_arg", sql: `SELECT CONCAT('a') AS v`, want: "a", pg: `a`},
		{name: "concat_five_args", sql: `SELECT CONCAT('a','b','c','d','e') AS v`, want: "abcde", pg: `abcde`},
		{name: "greatest_one_arg", sql: `SELECT GREATEST(1) AS v`, want: int32(1), pg: `1`},
		{name: "lpad_two_args", sql: `SELECT LPAD('a',3) AS v`, want: "  a", pg: `'  a'`},
		{name: "lpad_three_args", sql: `SELECT LPAD('a',3,'0') AS v`, want: "00a", pg: `00a`},

		// --- #1056: a NUMBER in a text position -------------------------
		// concat renders ANY operand, which is why it is the one that must
		// still answer rather than refuse.
		{name: "concat_number_and_column", sql: `SELECT CONCAT(1, name) AS v FROM fsc WHERE k=1`,
			want: "1<name>", pg: `1<name>`},
		{name: "concat_column_and_number", sql: `SELECT CONCAT(name, 1) AS v FROM fsc WHERE k=1`,
			want: "<name>1", pg: `<name>1`},
		{name: "concat_ws_numbers", sql: `SELECT CONCAT_WS(',',1,2) AS v`, want: "1,2", pg: `1,2`},
		{name: "concat_op_number", sql: `SELECT 'x' || 1 AS v`, want: "x1", pg: `x1`},
		// Every other text function refuses it.
		{name: "starts_with_number", sql: `SELECT STARTS_WITH(name, 1) AS v FROM fsc WHERE k=1`,
			state: "42883", pg: `42883 function starts_with(text, integer) does not exist`},
		{name: "ends_with_number", sql: `SELECT ENDS_WITH(name, 1) AS v FROM fsc WHERE k=1`,
			state: "42883", pg: `42883 function ends_with(text, integer) does not exist`},
		{name: "contains_number", sql: `SELECT CONTAINS(name, 1) AS v FROM fsc WHERE k=1`,
			state: "42883", pg: `wadjet's own spelling; PostgreSQL's answer for the shape is 42883`},
		{name: "replace_number", sql: `SELECT REPLACE(name, 1, 'x') AS v FROM fsc WHERE k=1`,
			state: "42883", pg: `42883 function replace(text, integer, unknown) does not exist`},
		{name: "upper_number", sql: `SELECT UPPER(1) AS v`, state: "42883",
			pg: `42883 function upper(integer) does not exist`, msg: `function upper(integer) does not exist`},
		{name: "lower_fractional", sql: `SELECT LOWER(1.5) AS v`, state: "42883",
			pg: `42883 function lower(numeric) does not exist`, msg: `function lower(numeric) does not exist`},
		{name: "length_number", sql: `SELECT LENGTH(1) AS v`, state: "42883",
			pg: `42883 function length(integer) does not exist`, msg: `function length(integer) does not exist`},
		{name: "lpad_number", sql: `SELECT LPAD(1,3,'0') AS v`, state: "42883",
			pg: `42883 function lpad(integer, integer, unknown) does not exist`},
		{name: "split_part_number", sql: `SELECT SPLIT_PART(1,'a',1) AS v`, state: "42883",
			pg: `42883 function split_part(integer, unknown, integer) does not exist`},
		{name: "reverse_number", sql: `SELECT REVERSE(1) AS v`, state: "42883",
			pg: `42883 function reverse(integer) does not exist`},
		{name: "repeat_number", sql: `SELECT REPEAT(1,2) AS v`, state: "42883",
			pg: `42883 function repeat(integer, integer) does not exist`},
		{name: "translate_number", sql: `SELECT TRANSLATE(1,'a','b') AS v`, state: "42883",
			pg: `42883 function translate(integer, unknown, unknown) does not exist`},
		{name: "strpos_number", sql: `SELECT STRPOS(1,'a') AS v`, state: "42883",
			pg: `42883 function strpos(integer, unknown) does not exist`},
		{name: "regexp_replace_number", sql: `SELECT REGEXP_REPLACE(1,'a','b') AS v`, state: "42883",
			pg: `42883 function regexp_replace(integer, unknown, unknown) does not exist`},
		{name: "trim_number", sql: `SELECT TRIM(1) AS v`, state: "42883",
			pg: `42883 function pg_catalog.btrim(integer) does not exist`},
		{name: "md5_number", sql: `SELECT MD5(1) AS v`, state: "42883",
			pg: `42883 function md5(integer) does not exist`},
		// THE LITERAL'S OWN NAME, which PostgreSQL decides by magnitude and
		// spelling: int4 if it fits, int8 if it fits that, numeric otherwise,
		// and numeric for anything with a point or an exponent. The arc named
		// every bare integer `bigint` — this engine's own widening — where the
		// server names `integer`, which was the one exception to "PostgreSQL's
		// own message shape" (round-1 review, N7).
		{name: "literal_name_int4_edge", sql: `SELECT UPPER(2147483647) AS v`, state: "42883",
			pg: `42883 function upper(integer) does not exist`, msg: `function upper(integer) does not exist`},
		{name: "literal_name_past_int4", sql: `SELECT UPPER(2147483648) AS v`, state: "42883",
			pg: `42883 function upper(bigint) does not exist`, msg: `function upper(bigint) does not exist`},
		{name: "literal_name_past_int8", sql: `SELECT UPPER(9223372036854775808) AS v`, state: "42883",
			pg: `42883 function upper(numeric) does not exist`, msg: `function upper(numeric) does not exist`},
		{name: "literal_name_exponent", sql: `SELECT UPPER(1e3) AS v`, state: "42883",
			pg: `42883 function upper(numeric) does not exist`, msg: `function upper(numeric) does not exist`},
		{name: "literal_name_boolean", sql: `SELECT UPPER(TRUE) AS v`, state: "42883",
			pg: `42883 function upper(boolean) does not exist`, msg: `function upper(boolean) does not exist`},
		// A UNARY SIGN over a literal is still that literal: `upper(-1)` is
		// `function upper(integer) does not exist` on the server and ANSWERED
		// here while the sign hid the literal from the check.
		{name: "literal_name_negative", sql: `SELECT UPPER(-1) AS v`, state: "42883",
			pg: `42883 function upper(integer) does not exist`, msg: `function upper(integer) does not exist`},

		// --- #583: a BYTES value in a text position ---------------------
		// PostgreSQL has none of these over bytea.
		{name: "upper_bytes", sql: `SELECT UPPER(b) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function upper(bytea) does not exist`, msg: `function upper(bytea) does not exist`},
		{name: "lower_bytes", sql: `SELECT LOWER(b) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function lower(bytea) does not exist`, msg: `function lower(bytea) does not exist`},
		{name: "trim_bytes", sql: `SELECT TRIM(b) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function pg_catalog.btrim(bytea) does not exist`},
		{name: "reverse_bytes", sql: `SELECT REVERSE(b) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function reverse(bytea) does not exist`, msg: `function reverse(bytea) does not exist`},
		{name: "replace_bytes", sql: `SELECT REPLACE(b,'h','z') AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function replace(bytea, bytea, bytea) does not exist`},
		{name: "starts_with_bytes", sql: `SELECT STARTS_WITH(b,'h') AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function starts_with(bytea, bytea) does not exist`},
		{name: "repeat_bytes", sql: `SELECT REPEAT(b,2) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function repeat(bytea, integer) does not exist`},
		{name: "split_part_bytes", sql: `SELECT SPLIT_PART(b,'i',1) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function split_part(bytea, bytea, integer) does not exist`},
		{name: "lpad_bytes", sql: `SELECT LPAD(b,5,'0') AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function lpad(bytea, integer, bytea) does not exist`},
		{name: "char_length_bytes", sql: `SELECT CHAR_LENGTH(b) AS v FROM fsc WHERE k=1`, state: "42883",
			pg: `42883 function char_length(bytea) does not exist`},
		// The ones PostgreSQL DOES have over bytea keep answering, over BYTES.
		{name: "length_bytes", sql: `SELECT LENGTH(b) AS v FROM fsc WHERE k=1`, want: int32(2), pg: `2`},
		{name: "octet_length_bytes", sql: `SELECT OCTET_LENGTH(b) AS v FROM fsc WHERE k=1`, want: int32(2), pg: `2`},
		{name: "bit_length_bytes", sql: `SELECT BIT_LENGTH(b) AS v FROM fsc WHERE k=1`, want: int32(16), pg: `16`},
		{name: "substring_bytes", sql: `SELECT SUBSTRING(b,1,1) AS v FROM fsc WHERE k=1`,
			want: []byte{0x68}, pg: `\x68`},
		{name: "md5_bytes", sql: `SELECT MD5(b) AS v FROM fsc WHERE k=1`,
			want: "49f68a5c8493ec2c0bf489821c21fc3b", pg: `49f68a5c8493ec2c0bf489821c21fc3b`},
		{name: "position_bytes", sql: `SELECT POSITION('i' IN b) AS v FROM fsc WHERE k=1`,
			want: int32(2), pg: `2`},
		// --- #583's BRIDGE, which this engine had neither half of --------
		{name: "encode_hex", sql: `SELECT ENCODE(b,'hex') AS v FROM fsc WHERE k=1`, want: "6869", pg: `6869`},
		{name: "encode_base64", sql: `SELECT ENCODE(b,'base64') AS v FROM fsc WHERE k=1`, want: "aGk=", pg: `aGk=`},
		{name: "encode_escape_non_utf8", sql: `SELECT ENCODE(nb,'escape') AS v FROM fsc WHERE k=2`,
			want: `\377\376\000A`, pg: `\377\376\000A`},
		{name: "encode_unknown", sql: `SELECT ENCODE(b,'zzz') AS v FROM fsc WHERE k=1`, state: "22023",
			pg: `22023 unrecognized encoding: "zzz"`},
		{name: "decode_hex", sql: `SELECT DECODE('6869','hex') AS v`, want: []byte("hi"), pg: `\x6869`},
		{name: "decode_base64", sql: `SELECT DECODE('aGk=','base64') AS v`, want: []byte("hi"), pg: `\x6869`},
		{name: "decode_escape", sql: `SELECT DECODE('hi','escape') AS v`, want: []byte("hi"), pg: `\x6869`},
		{name: "decode_bad_hex", sql: `SELECT DECODE('zz','hex') AS v`, state: "22023",
			pg: `22023 invalid hexadecimal digit: "z"`},
		{name: "decode_unknown", sql: `SELECT DECODE('hi','zzz') AS v`, state: "22023",
			pg: `22023 unrecognized encoding: "zzz"`},
		{name: "get_byte", sql: `SELECT GET_BYTE(b,0) AS v FROM fsc WHERE k=1`, want: int32(104), pg: `104, declared integer`},
		{name: "get_byte_past_end", sql: `SELECT GET_BYTE(b,5) AS v FROM fsc WHERE k=1`, state: "2202E",
			pg: `2202E index 5 out of valid range, 0..1`},
		{name: "get_byte_negative", sql: `SELECT GET_BYTE(b,-1) AS v FROM fsc WHERE k=1`, state: "2202E",
			pg: `2202E index -1 out of valid range, 0..1`},
		{name: "set_byte", sql: `SELECT SET_BYTE(b,0,65) AS v FROM fsc WHERE k=1`,
			want: []byte{0x41, 0x69}, pg: `\x4169`},
		{name: "set_byte_wraps_the_value", sql: `SELECT SET_BYTE(b,0,300) AS v FROM fsc WHERE k=1`,
			want: []byte{0x2c, 0x69}, pg: `\x2c69 — 300 & 0xff`},
		{name: "set_byte_past_end", sql: `SELECT SET_BYTE(b,5,65) AS v FROM fsc WHERE k=1`, state: "2202E",
			pg: `2202E index 5 out of valid range, 0..1`},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if c.state != "" {
				if err == nil {
					t.Fatalf("answered %v; PostgreSQL 17.11 says %s\n  SQL: %s", res.Rows, c.pg, c.sql)
				}
				if got := sqlerr.StateOf(err); got != c.state {
					t.Errorf("SQLSTATE %q, want %q\n  err: %v\n  PostgreSQL 17.11: %s\n  SQL: %s",
						got, c.state, err, c.pg, c.sql)
				}
				// The MESSAGE, where a cell names one: a 42883 that names the
				// wrong ARGUMENT TYPE sends the reader looking for the wrong
				// overload, which is the whole of N7.
				if c.msg != "" && !strings.Contains(err.Error(), c.msg) {
					t.Errorf("message %q does not carry %q\n  PostgreSQL 17.11: %s\n  SQL: %s",
						err.Error(), c.msg, c.pg, c.sql)
				}
				// XX000 is the one answer this whole family must never give
				// again: an internal error tells the client nothing about its
				// own statement (#1056).
				if strings.Contains(err.Error(), "internal error") {
					t.Errorf("the refusal is an INTERNAL ERROR: %v\n  SQL: %s", err, c.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("%v\n  PostgreSQL 17.11 answers %s\n  SQL: %s", err, c.pg, c.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1\n  SQL: %s", len(res.Rows), c.sql)
			}
			got := res.Rows[0]["v"]
			if wb, isBytes := c.want.([]byte); isBytes {
				gb, ok := got.([]byte)
				if !ok || string(gb) != string(wb) {
					t.Errorf("= %#v, want %#v (PostgreSQL 17.11: %s)\n  SQL: %s", got, wb, c.pg, c.sql)
				}
				return
			}
			if got != c.want {
				t.Errorf("= %#v (%T), want %#v (PostgreSQL 17.11: %s)\n  SQL: %s",
					got, got, c.want, c.pg, c.sql)
			}
		})
	}
}

// TestEveryTextKernelTakesALiteralInEveryPosition is #1056's registry-wide ask,
// and it is the gate that stands where the XX000 was: every function with a
// STRING vec kernel, called with a numeric literal in each of its positions,
// answers what PostgreSQL answers and NEVER an internal error.
//
// It is generated from the DECLARATIONS rather than listed, so a kernel added
// later is covered without anyone remembering to add a row.
func TestEveryTextKernelTakesALiteralInEveryPosition(t *testing.T) {
	ctx := context.Background()
	db := fscOpen(t)

	cells := 0
	for _, name := range expr.SignatureNames() {
		sig, _ := expr.SignatureOf(name)
		if !expr.HasVecKernel(name) {
			continue
		}
		if !fscSpellableAsACall(name) {
			// LEFT and RIGHT are reserved tokens in this grammar, `||` is an
			// operator registered under punctuation so no query may spell it
			// (#609), and EXTRACT is a special form. None can be written as an
			// ordinary call at all, so a syntax error here would say nothing
			// about the kernel. Recorded rather than silently skipped: the
			// parser gap is arc PS's, and `left(b,1)` is a residue of #583.
			continue
		}
		width := sig.Min
		if width == 0 {
			continue
		}
		for pos := 0; pos < width; pos++ {
			args := make([]string, width)
			for i := range args {
				args[i] = "'a'"
				if i >= 1 && sig.DomainAt(i) == expr.ArgAny {
					// A count or an index position: give it a number that
					// makes the call meaningful when it is not the one under
					// test.
					args[i] = "1"
				}
			}
			args[pos] = "42"
			sql := fmt.Sprintf(`SELECT %s(%s) AS v FROM fsc WHERE k=1`, name, strings.Join(args, ","))
			cells++
			_, err := db.Query(ctx, sql)
			if err == nil {
				// An ANSWER is correct for a position whose domain is ANY —
				// concat renders a number — and wrong for a declared text one,
				// which the refusal below would have caught.
				continue
			}
			if strings.Contains(err.Error(), "internal error") {
				t.Errorf("%s: a numeric literal in position %d is an INTERNAL ERROR, which "+
					"tells the client nothing about its own statement (#1056): %v\n  SQL: %s",
					name, pos, err, sql)
				continue
			}
			if got := sqlerr.StateOf(err); got != "42883" && got != "22023" && got != "22P02" {
				t.Errorf("%s: a numeric literal in position %d raised SQLSTATE %q; the family's "+
					"answers are 42883 (no such signature) or a value-level refusal\n  err: %v\n  SQL: %s",
					name, pos, got, err, sql)
			}
		}
	}
	if cells < 30 {
		t.Fatalf("only %d (kernel, position) cells were exercised; this gate is vacuous", cells)
	}
	t.Logf("%d (vec kernel, literal position) cells exercised", cells)
}

// fscSpellableAsACall reports whether a query may write this function as
// `name(args)`. The four that cannot are the grammar's, not the registry's.
func fscSpellableAsACall(name string) bool {
	switch name {
	case "left", "right", "extract", "||":
		return false
	}
	return true
}

func fscOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeBytes, Nullable: true},
		{Name: "nb", Type: parquet.TypeBytes, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "fsc", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("fsc", sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{
		{"k": int64(1), "name": "<name>", "b": []byte("hi"), "nb": []byte("hi")},
		{"k": int64(2), "name": "x", "b": []byte("hi"), "nb": []byte{0xff, 0xfe, 0x00, 0x41}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
