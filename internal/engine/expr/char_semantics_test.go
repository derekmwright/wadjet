package expr

import (
	"testing"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #856: the string family indexes CHARACTERS, as PostgreSQL does.
//
// LENGTH and CHARACTER_LENGTH are synonyms on the server and disagreed here —
// 6 against 3 for 'éàü' — and the same byte indexing was in SUBSTRING/SUBSTR,
// POSITION/STRPOS, LPAD, RPAD, LEFT, RIGHT and the vectorized REVERSE. Four of
// those produced INVALID UTF-8 by cutting a multi-byte character in half, which
// is a wrong VALUE and not merely a wrong count.
//
// Every expectation is live postgres:17.11 (ROUND0.md's table). OCTET_LENGTH
// and BIT_LENGTH are the byte-counting spellings and are unchanged; they are
// asserted here as controls, because a fix that made everything count
// characters would be as wrong as the one that made everything count bytes.
//
// The COMBINING-MARK cell is the one that says which unit "character" means:
// "é" is one grapheme and TWO code points, and PostgreSQL counts CODE
// POINTS — `length('é')` written that way is 2 on the server, measured. Go's
// rune count is a code-point count, so the two agree; the cell exists so that
// remains a decision rather than a coincidence.

const (
	// Three two-byte characters: six octets, three code points.
	multiByte = "éàü"
	// One base letter plus a combining acute: three octets, TWO code points,
	// ONE grapheme. PostgreSQL 17.11: length 2, character_length 2.
	combining = "é"
)

func charSemanticsBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "s", Type: parquet.TypeString},
	}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, multiByte)
	return b
}

func TestStringFunctionsCountCharacters(t *testing.T) {
	b := charSemanticsBatch(t)
	lit := func(v any) Expr { return &Lit{Val: v} }
	for _, c := range []struct {
		name string
		expr Expr
		want any
	}{
		// --- the counting family ------------------------------------------
		{"length_multibyte", &FuncCall{Name: "length", Args: []Expr{lit(multiByte)}}, int32(3)},
		{"len_multibyte", &FuncCall{Name: "len", Args: []Expr{lit(multiByte)}}, int32(3)},
		{"character_length_multibyte",
			&FuncCall{Name: "character_length", Args: []Expr{lit(multiByte)}}, int32(3)},
		{"char_length_multibyte",
			&FuncCall{Name: "char_length", Args: []Expr{lit(multiByte)}}, int32(3)},
		// The controls: these are BYTE counts on the server too.
		{"octet_length_multibyte",
			&FuncCall{Name: "octet_length", Args: []Expr{lit(multiByte)}}, int32(6)},
		{"bit_length_multibyte",
			&FuncCall{Name: "bit_length", Args: []Expr{lit(multiByte)}}, int32(48)},
		// A code point that is not a grapheme. PostgreSQL counts CODE POINTS.
		{"length_combining_mark", &FuncCall{Name: "length", Args: []Expr{lit(combining)}}, int32(2)},
		{"character_length_combining_mark",
			&FuncCall{Name: "character_length", Args: []Expr{lit(combining)}}, int32(2)},
		{"octet_length_combining_mark",
			&FuncCall{Name: "octet_length", Args: []Expr{lit(combining)}}, int32(3)},
		// The empty string and NULL, unchanged.
		{"length_empty", &FuncCall{Name: "length", Args: []Expr{lit("")}}, int32(0)},
		{"length_null", &FuncCall{Name: "length", Args: []Expr{lit(nil)}}, nil},
		{"length_ascii", &FuncCall{Name: "length", Args: []Expr{lit("abc")}}, int32(3)},

		// --- the indexing family ------------------------------------------
		{"substring_from_the_start",
			&FuncCall{Name: "substring", Args: []Expr{lit(multiByte), lit(int64(1)), lit(int64(2))}}, "éà"},
		{"substr_from_the_middle",
			&FuncCall{Name: "substr", Args: []Expr{lit(multiByte), lit(int64(2)), lit(int64(2))}}, "àü"},
		{"substr_without_a_length",
			&FuncCall{Name: "substr", Args: []Expr{lit(multiByte), lit(int64(2))}}, "àü"},
		// PostgreSQL's window rule: a start below position 1 consumes part of
		// the length before the string begins (#373), in CHARACTERS now.
		{"substr_start_below_one",
			&FuncCall{Name: "substr", Args: []Expr{lit(multiByte), lit(int64(0)), lit(int64(2))}}, "é"},
		{"substr_past_the_end",
			&FuncCall{Name: "substr", Args: []Expr{lit(multiByte), lit(int64(9))}}, ""},
		{"substr_of_a_combining_mark",
			&FuncCall{Name: "substr", Args: []Expr{lit(combining), lit(int64(1)), lit(int64(1))}}, "e"},

		// --- position -------------------------------------------------------
		{"strpos_multibyte",
			&FuncCall{Name: "strpos", Args: []Expr{lit(multiByte), lit("à")}}, int32(2)},
		{"position_multibyte",
			&FuncCall{Name: "position", Args: []Expr{lit(multiByte), lit("ü")}}, int32(3)},
		{"strpos_absent", &FuncCall{Name: "strpos", Args: []Expr{lit(multiByte), lit("z")}}, int32(0)},
		{"strpos_at_the_start",
			&FuncCall{Name: "strpos", Args: []Expr{lit(multiByte), lit("é")}}, int32(1)},

		// --- padding --------------------------------------------------------
		{"lpad_multibyte",
			&FuncCall{Name: "lpad", Args: []Expr{lit(multiByte), lit(int64(5)), lit("x")}}, "xxéàü"},
		{"rpad_multibyte",
			&FuncCall{Name: "rpad", Args: []Expr{lit(multiByte), lit(int64(5)), lit("x")}}, "éàüxx"},
		// A multi-byte FILL, which the byte version could also cut in half.
		{"lpad_multibyte_fill",
			&FuncCall{Name: "lpad", Args: []Expr{lit("ab"), lit(int64(5)), lit("é")}}, "éééab"},
		{"rpad_multibyte_fill",
			&FuncCall{Name: "rpad", Args: []Expr{lit("ab"), lit(int64(5)), lit("é")}}, "abééé"},
		// Truncation when n is SHORTER than the input: characters again.
		{"lpad_truncates_to_characters",
			&FuncCall{Name: "lpad", Args: []Expr{lit(multiByte), lit(int64(2)), lit("x")}}, "éà"},
		{"rpad_truncates_to_characters",
			&FuncCall{Name: "rpad", Args: []Expr{lit(multiByte), lit(int64(2)), lit("x")}}, "éà"},
		{"lpad_no_padding_needed",
			&FuncCall{Name: "lpad", Args: []Expr{lit(multiByte), lit(int64(3)), lit("x")}}, "éàü"},

		// --- reverse / left / right -----------------------------------------
		{"reverse_multibyte", &FuncCall{Name: "reverse", Args: []Expr{lit(multiByte)}}, "üàé"},
		{"left_multibyte", &FuncCall{Name: "left", Args: []Expr{lit(multiByte), lit(int64(2))}}, "éà"},
		{"right_multibyte", &FuncCall{Name: "right", Args: []Expr{lit(multiByte), lit(int64(2))}}, "àü"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := c.expr.Eval(b, 0)
			if got != c.want {
				t.Errorf("= %#v, want %#v (live PostgreSQL 17.11)", got, c.want)
			}
			// Nothing in this family may produce invalid UTF-8. Four of them
			// did, by slicing a multi-byte character in half.
			if s, ok := got.(string); ok && !utf8.ValidString(s) {
				t.Errorf("produced invalid UTF-8 (% x) — a cut character is a wrong VALUE, "+
					"not a wrong count (#856)", s)
			}
		})
	}
}

// SUBSTRING with a NEGATIVE length is SQLSTATE 22011 on the server, not the
// empty string this answered. substrWindow's own doc recorded it as a refusal
// this layer could not make.
func TestSubstringRefusesANegativeLength(t *testing.T) {
	b := charSemanticsBatch(t)
	e := &FuncCall{Name: "substring", Args: []Expr{
		&Lit{Val: "abc"}, &Lit{Val: int64(2)}, &Lit{Val: int64(-1)}}}
	state, msg := recoverFatalEvalForTest(t, func() { e.Eval(b, 0) })
	if state != "22011" || msg != "negative substring length not allowed" {
		t.Errorf("raised [%s] %s, want [22011] negative substring length not allowed", state, msg)
	}
	// A zero length is NOT an error: PostgreSQL answers the empty string.
	ok := &FuncCall{Name: "substring", Args: []Expr{
		&Lit{Val: "abc"}, &Lit{Val: int64(2)}, &Lit{Val: int64(0)}}}
	if got := ok.Eval(b, 0); got != "" {
		t.Errorf("zero length = %#v, want \"\"", got)
	}
}

// The two evaluators must agree. Every function here has a boxed definition
// and a vectorized kernel, and #856 found REVERSE disagreeing already —
// fnReverse reversed runes and vecReverse reversed bytes, so one query
// answered two things depending on which path the plan reached.
func TestCharSemanticsAgreeBetweenTheBoxedAndVectorizedPaths(t *testing.T) {
	vals := []string{multiByte, combining, "abc", "", "日本語テキスト", "😀🎉"}
	b := batch.NewRecordBatch([]parquet.Column{{Name: "s", Type: parquet.TypeString}}, len(vals))
	b.Len = len(vals)
	for i, v := range vals {
		b.Columns[0].SetValue(i, v)
	}
	for _, c := range []struct {
		name string
		args []Expr
		fn   VecScalarFunc
		out  batch.TypeID
	}{
		{"length", []Expr{&ColRef{Name: "s"}}, vecCharLength, batch.TypeInt32},
		{"substr", []Expr{&ColRef{Name: "s"}, &Lit{Val: int64(2)}, &Lit{Val: int64(2)}},
			vecSubstr, batch.TypeString},
		{"reverse", []Expr{&ColRef{Name: "s"}}, vecReverse, batch.TypeString},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := batch.NewVector(c.out, len(vals))
			vecArgs := make([]*batch.Vector, len(c.args))
			vecArgs[0] = b.Columns[0]
			for i := 1; i < len(c.args); i++ {
				lv := batch.NewVector(batch.TypeInt64, len(vals))
				for r := range vals {
					lv.SetValue(r, c.args[i].(*Lit).Val)
				}
				vecArgs[i] = lv
			}
			c.fn(vecArgs, out, len(vals))
			boxed := &FuncCall{Name: c.name, Args: c.args}
			for i := range vals {
				want := boxed.Eval(b, i)
				got := out.GetValue(i)
				if c.out == batch.TypeString {
					// An empty result boxes as nil through the vector and as
					// "" through the boxed call; both mean the empty string.
					if got == nil {
						got = ""
					}
					if s, ok := got.(string); ok && !utf8.ValidString(s) {
						t.Errorf("row %d (%q): the kernel produced invalid UTF-8 (% x)",
							i, vals[i], s)
					}
				}
				if got != want {
					t.Errorf("row %d (%q): kernel %#v, boxed %#v — one function, two answers",
						i, vals[i], got, want)
				}
			}
		})
	}
}
