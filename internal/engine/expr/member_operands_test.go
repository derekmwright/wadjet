// SPDX-License-Identifier: MIT

package expr

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A NUMERIC LITERAL ON A MEMBERSHIP'S OUTER SIDE NEVER BOXES AS A DOUBLE
// (#1372). Every spelling of PostgreSQL's numeric input — in every outer
// shape the rule types: a quoted literal, a quoted literal under a bare
// CAST AS NUMERIC / DECIMAL or ::numeric, an unquoted constant with or
// without a sign — is typed NUMERIC(38, its value's own scale), and the
// typed node evaluates to a DECIMAL box (its rendered text), which
// InSubquery keys on its decSet and memberDecimalEqual compares by
// canonical value. There is no bare-NUMERIC outcome to fall back to: a bare
// NUMERIC of a literal boxes as a float64, and a float64 probe is the one
// that reaches the float rung. A number no DECIMAL(38,s) holds is 22003.
func TestMemberNumericProbeIsNeverADouble(t *testing.T) {
	z := strings.Repeat
	typed := []struct{ text, typ string }{
		{"14", "NUMERIC(38,0)"}, {"14.0000000000000000001", "NUMERIC(38,19)"}, {"+12.5", "NUMERIC(38,1)"},
		{"-12.5", "NUMERIC(38,1)"}, {"00014", "NUMERIC(38,0)"}, {"14.000", "NUMERIC(38,3)"}, {"14.", "NUMERIC(38,0)"},
		{".5", "NUMERIC(38,1)"}, {".140000000000000000001e2", "NUMERIC(38,19)"}, {"1.4e1", "NUMERIC(38,0)"},
		{"1.4E1", "NUMERIC(38,0)"}, {"0.125e+2", "NUMERIC(38,1)"}, {"1400e-2", "NUMERIC(38,2)"},
		{"1.25000000000000001e13", "NUMERIC(38,4)"}, {" 14 ", "NUMERIC(38,0)"},
		{"14." + z("0", 40), "NUMERIC(38,0)"}, {z("0", 40) + "12.5", "NUMERIC(38,1)"},
		{"14" + z("0", 40) + "e-40", "NUMERIC(38,0)"}, {"0." + z("0", 45), "NUMERIC(38,0)"},
		{"14." + z("0", 35) + "1", "NUMERIC(38,36)"},
	}
	overflow := []string{
		"14." + z("0", 39) + "1", "0." + z("0", 38) + "1", "1e-40", "1" + z("0", 40), "1e40", "1e1000",
		"NaN", "Infinity", "-Infinity", "inf",
	}
	shapes := func(text string) []string {
		out := []string{"'" + text + "'", "CAST('" + text + "' AS NUMERIC)", "CAST('" + text + "' AS decimal)", "'" + text + "'::numeric"}
		if strings.TrimSpace(text) == text && !strings.ContainsAny(text, "NIi") {
			out = append(out, text)
		}
		return out
	}
	if _, ok := MemberLiteralCast(parquet.TypeDecimal); ok {
		t.Fatalf("MemberLiteralCast(DECIMAL) names a type: a bare NUMERIC of a literal is a double")
	}
	for _, c := range typed {
		for _, src := range shapes(c.text) {
			node, err := plansql.ParseExpression(src)
			if err != nil {
				t.Fatalf("parse %s: %v", src, err)
			}
			probe, ok, err := MemberProbe(node, parquet.TypeDecimal)
			if err != nil || !ok {
				t.Errorf("MemberProbe(%s) = ok %v, err %v; want a typed literal", src, ok, err)
				continue
			}
			cast, isCast := probe.(*plansql.CastNode)
			if !isCast || cast.TypeName != c.typ {
				t.Errorf("MemberProbe(%s) = %s; want cast(… as %s)", src, probe, c.typ)
				continue
			}
			compiled, err := Compile(probe)
			if err != nil {
				t.Fatalf("compile %s: %v", probe, err)
			}
			if v := compiled.Eval(nil, 0); func() bool { _, isStr := v.(string); return !isStr }() {
				t.Errorf("%s evaluates to %T %v; want a DECIMAL box (decimal text), never a double", probe, v, v)
			}
			// Idempotent: a typed literal is no longer an operand the rule types.
			if again, ok, _ := MemberProbe(probe, parquet.TypeDecimal); ok {
				t.Errorf("MemberProbe typed %s a second time: %s", probe, again)
			}
		}
	}
	for _, text := range overflow {
		for _, src := range shapes(text) {
			node, err := plansql.ParseExpression(src)
			if err != nil {
				t.Fatalf("parse %s: %v", src, err)
			}
			_, _, err = MemberProbe(node, parquet.TypeDecimal)
			if st := sqlerr.StateOf(err); st != "22003" || !strings.Contains(err.Error(), strings.TrimSpace(text)) {
				t.Errorf("MemberProbe(%s) err = %v; want 22003 naming the literal", src, err)
			}
			if err := CheckMemberProbe(node, parquet.TypeDecimal); sqlerr.StateOf(err) != "22003" {
				t.Errorf("CheckMemberProbe(%s) = %v; want 22003", src, err)
			}
		}
	}
	// Text that is no number keeps the CAST, whose input function refuses it.
	for _, text := range []string{"zz", "1e", ".", "", "+NaN", "14\u00a0", "0x0E", "1_4"} {
		node, _ := plansql.ParseExpression("'" + text + "'")
		if err := CheckMemberProbe(node, parquet.TypeDecimal); sqlerr.StateOf(err) != "22P02" {
			t.Errorf("CheckMemberProbe(%q) = %v; want 22P02", text, err)
		}
	}
	// An operand that is no literal is left alone, whatever its declaration.
	for _, src := range []string{"a.v", "a.v + 1", "CAST(a.v AS NUMERIC)", "CAST('14' AS NUMERIC(18,4))"} {
		node, _ := plansql.ParseExpression(src)
		if _, ok, err := MemberProbe(node, parquet.TypeDecimal); ok || err != nil {
			t.Errorf("MemberProbe(%s) typed it (ok %v, err %v)", src, ok, err)
		}
	}
	// numeric = integer is numeric: against a bigint set a numeric literal
	// that is not a plain integer takes the same NUMERIC(38,s); a plain
	// integer constant is already exact on the integer rung, and a QUOTED
	// literal alone takes bigint (the input function refuses '14.5').
	for _, c := range []struct{ src, want string }{
		{"14.0000000000000000001", "cast('14.0000000000000000001' as NUMERIC(38,19))"},
		{"CAST('14.0000000000000000001' AS NUMERIC)", "cast('14.0000000000000000001' as NUMERIC(38,19))"},
		{"1.4e1", "cast('1.4e1' as NUMERIC(38,0))"},
		{"14", "14"},
		{"'14.5'", "cast('14.5' as BIGINT)"},
	} {
		node, _ := plansql.ParseExpression(c.src)
		probe, _, err := MemberProbe(node, parquet.TypeInt64)
		if err != nil || probe.String() != c.want {
			t.Errorf("MemberProbe(%s, bigint) = %s, %v; want %s", c.src, probe, err, c.want)
		}
	}
	// The per-row rung meets an integer member and a decimal probe by value.
	set := &parquet.Column{Type: parquet.TypeInt64}
	for _, c := range []struct {
		probe  any
		member any
		eq     bool
	}{
		{"14.0000000000000000000", int64(14), true},
		{"14.0000000000000000001", int64(14), false},
		{"-14", int64(-14), true},
	} {
		if eq, decided := memberDecimalEqual(c.probe, c.member, set); !decided || eq != c.eq {
			t.Errorf("memberDecimalEqual(%v, %v, bigint) = %v, %v; want %v, decided", c.probe, c.member, eq, decided, c.eq)
		}
	}
}
