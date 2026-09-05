package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #583: the functions PostgreSQL HAS over bytea answer in bytea, and they index
// BYTES.
//
// Every scalar function read its operand through expr.toString, so a byte slice
// became whatever Go string its bytes spell: `length` counted CHARACTERS (1 for
// the two bytes of an encoded 'é'), `substring` cut a non-UTF-8 value into the
// replacement character U+FFFD — a value the column does not hold — and `||`
// handed back TEXT under OID 25, which is #570's own embedded-NUL hazard coming
// back through a derived value.
//
// Every expectation is live PostgreSQL 17.11 over the same bytes, EXCEPT the
// cells named `residual_*`: those record what THIS engine answers where it
// does not agree with the server, each with the server's own answer beside it
// in the comment, and each fails the day the two start agreeing. A gate whose
// header claims live PostgreSQL for a value the server does not produce is a
// false record, which is what round 2 of this arc shipped for the three
// `text || bytea` cells.
func TestByteaFunctionsAnswerInBytes(t *testing.T) {
	ctx := context.Background()
	db := f3ByteaOpen(t)

	for _, c := range []struct {
		name, expr string
		k          int64
		want       any
		decl       parquet.TypeID
	}{
		// length(bytea) is the BYTE count — the same number octet_length
		// gives, which is what "bytea has no characters" means.
		{"length_ascii", `LENGTH(b)`, 1, int32(2), parquet.TypeInt32},
		{"length_non_utf8", `LENGTH(b)`, 2, int32(4), parquet.TypeInt32},
		{"length_backslash", `LENGTH(b)`, 5, int32(3), parquet.TypeInt32},
		// The cells that separate BYTES from characters. A bare column takes
		// the VECTORIZED kernel and a derived value takes the scalar one, so
		// both spellings are here: #583's first pass gave the scalar arm the
		// byte count and left the kernel counting runes, which is the
		// evaluator a projection over a bare column reaches (round 2, B4).
		{"length_multibyte_column", `LENGTH(b)`, 6, int32(2), parquet.TypeInt32},
		{"length_multibyte_derived", `LENGTH(b || CAST('' AS BYTES))`, 6, int32(2), parquet.TypeInt32},
		{"octet_length_multibyte", `OCTET_LENGTH(b)`, 6, int32(2), parquet.TypeInt32},
		// RESIDUAL: the server has no `char_length(bytea)` at all —
		// `function char_length(bytea) does not exist`, 42883, measured — so
		// this cell records an ANSWER where PostgreSQL raises. It answers the
		// BYTE count because `length` and `char_length` share one kernel and
		// a rune count here would make one expression give two numbers; the
		// refusal is the text-only-function family's, deferred with it in
		// ADR-0012. Delete this cell when that plan-time check lands.
		{"residual_char_length_answers_where_pg_raises", `CHAR_LENGTH(b)`, 6, int32(2), parquet.TypeInt32},
		{"length_multibyte_word", `LENGTH(b)`, 7, int32(6), parquet.TypeInt32},
		{"octet_length_multibyte_word", `OCTET_LENGTH(b)`, 7, int32(6), parquet.TypeInt32},
		// The TEXT family keeps CHARACTERS over the same bytes, which is the
		// boundary from the other side.
		{"string_length_multibyte", `LENGTH('é')`, 6, int32(1), parquet.TypeInt32},
		{"string_char_length_multibyte", `CHAR_LENGTH('héllo')`, 6, int32(5), parquet.TypeInt32},
		{"string_octet_length_multibyte", `OCTET_LENGTH('héllo')`, 6, int32(6), parquet.TypeInt32},
		{"octet_length", `OCTET_LENGTH(b)`, 2, int32(4), parquet.TypeInt32},
		// substring(bytea, …) is bytea, indexed by bytes. The second cell is
		// the one that was U+FFFD.
		{"substring_ascii", `SUBSTRING(b, 1, 1)`, 1, []byte{0x68}, parquet.TypeBytes},
		{"substring_non_utf8", `SUBSTRING(b, 1, 1)`, 2, []byte{0xff}, parquet.TypeBytes},
		{"substring_tail", `SUBSTRING(b, 2)`, 2, []byte{0xfe, 0x00, 0x41}, parquet.TypeBytes},
		// bytea || bytea, and bytea || an unknown-typed literal, are bytea.
		{"concat_literal", `b || 'x'`, 1, []byte{0x68, 0x69, 0x78}, parquet.TypeBytes},
		{"concat_column", `b || b`, 2, []byte{0xff, 0xfe, 0x00, 0x41, 0xff, 0xfe, 0x00, 0x41},
			parquet.TypeBytes},
		// RESIDUAL, pinned: an unknown-typed LITERAL beside a bytea operand
		// of `||` still contributes its own SPELLING, so `b || '\x41'`
		// appends the four characters and PostgreSQL appends the one byte
		// 0x41. It is #582's rule one layer up — at a function ARGUMENT
		// rather than at a comparison — and closing it means resolving a
		// literal argument from its sibling's declared type in the compile
		// layer, which is where the literal is still a *Lit. The COLUMN and
		// the ordinary-spelling literal cells above are the ones #583 was
		// filed for. Delete this pin when the argument rule lands.
		{"residual_concat_hex_spelled_literal", `b || '\x41'`, 2,
			[]byte{0xff, 0xfe, 0x00, 0x41, 0x5c, 0x78, 0x34, 0x31}, parquet.TypeBytes},
		// `text || bytea` is TEXT on the server — it resolves through
		// `text || anynonarray`, which RENDERS the bytea and concatenates as
		// text — while `bytea || bytea` and `bytea || <unknown literal>` are
		// bytea. Declaring bytea for the text pair moved a right class to a
		// wrong one (round 2, B3), and the CLASS these three assert is the
		// server's.
		//
		// The VALUE is not, and these are RESIDUAL pins for it. Measured on
		// 17.11 with b = '\x6869':
		//
		//	'AB'::text || b   ->  AB\x6869   (8 chars)   here: ABhi
		//	b || 'AB'::text   ->  \x6869AB              here: hiAB
		//	upper('ab') || b  ->  AB\x6869              here: ABhi
		//
		// The server renders the bytea through bytea_out; this engine splices
		// the raw bytes, which on the wire puts non-UTF-8 (and, for '\x00',
		// a NUL) inside an OID-25 value — #570's hazard through a derived
		// value. Recorded in ADR-0012's #583 entry with the mechanism: the
		// value needs the operand's DECLARED type at the evaluator, and
		// deciding it from the Go box cannot separate a text COLUMN from an
		// unknown LITERAL, which is the `b || 'x'` cell above. Delete these
		// three when the plan-time argument-type check lands; they FAIL when
		// they start agreeing with the server.
		{"residual_text_concat_bytea_value_left", `CAST('AB' AS STRING) || b`, 1, "ABhi", parquet.TypeString},
		{"residual_text_concat_bytea_value_right", `b || CAST('AB' AS STRING)`, 1, "hiAB", parquet.TypeString},
		{"residual_text_concat_bytea_value_fn", `UPPER(CAST('ab' AS STRING)) || b`, 1, "ABhi", parquet.TypeString},
		// The two that were already right, as the controls: md5(bytea) is
		// text on the server too, and a CAST renders the \x form.
		{"md5", `MD5(b)`, 1, "49f68a5c8493ec2c0bf489821c21fc3b", parquet.TypeString},
		{"cast_text", `CAST(b AS STRING)`, 2, `\xfffe0041`, parquet.TypeString},
		// A STRING column keeps character semantics: nothing here leaks into
		// the text family, which is the boundary from the other side.
		{"string_length_is_characters", `LENGTH('éàü')`, 1, int32(3), parquet.TypeInt32},
		{"string_substring_is_characters", `SUBSTRING('éàü', 2, 2)`, 1, "àü", parquet.TypeString},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT %s AS v FROM byteapr WHERE k = %d`, c.expr, c.k)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows\n  SQL: %s", len(res.Rows), sql)
			}
			got := res.Rows[0]["v"]
			if wb, ok := c.want.([]byte); ok {
				gb, isBytes := got.([]byte)
				if !isBytes || !bytes.Equal(gb, wb) {
					t.Errorf("= %#v, want %#v (live PostgreSQL 17.11)\n  SQL: %s", got, wb, sql)
				}
			} else if got != c.want {
				t.Errorf("= %#v, want %#v (live PostgreSQL 17.11)\n  SQL: %s", got, c.want, sql)
			}
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d column metas\n  SQL: %s", len(res.ColumnMetas), sql)
			}
			if d := res.ColumnMetas[0].TypeID; d != c.decl {
				t.Errorf("declares %v, want %v — a bytea result under a text OID is what "+
					"libpq truncates at the first NUL\n  SQL: %s", d, c.decl, sql)
			}
		})
	}
}
