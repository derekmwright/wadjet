package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file is the missing gate feedback_gate_coverage_of_type_system.md
// asks for: a type-matrix sweep over the network-native types × the
// function-argument / CAST / comparison-against-a-literal consumer
// classes, at the internal/engine/expr layer where the bug actually lived
// (ColRef.Eval, FuncCall.Eval/EvalVec, Cast.Eval, Cmp/compileCmp).
//
// Root cause: ColRef.Eval boxed TypeIPv4 and TypeMAC columns as their raw
// encoded int64 in every path — including the ones that hand the value to
// a function body expecting text, a string-family CAST, or a comparison
// against a string literal — instead of falling through to
// Vector.GetValue's default case the way TypeIPv6/TypeCIDR/TypeUUID
// already do. There is no vectorized kernel for any network function
// (DefaultRegistry.LookupVec returns nil for all of them), so
// FuncCall.EvalVec's fallback IS FuncCall.Eval; netTypeMatrixBatch is built
// with 2 rows and every case below is checked through EvalVec too, to pin
// that the fallback wiring stays intact rather than assuming it from the
// source.

// netTypeMatrixBatch returns a 2-row batch carrying one column of each of
// the six network-native types, values chosen so that a IPv4 comparison
// against a string literal or against the other row's value only comes out
// right when the comparator is doing NUMERIC (address) ordering rather than
// lexical string ordering: "9.255.255.255" sorts after "10.1.2.3" as text.
func netTypeMatrixBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "c_ipv4", Type: parquet.TypeIPv4},
		{Name: "c_ipv6", Type: parquet.TypeIPv6},
		{Name: "c_cidr", Type: parquet.TypeCIDR},
		{Name: "c_mac", Type: parquet.TypeMAC},
		{Name: "c_port", Type: parquet.TypePort},
		{Name: "c_proto", Type: parquet.TypeProtocol},
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, "10.1.2.3")
	b.Columns[1].SetValue(0, "2001:db8::1")
	b.Columns[2].SetValue(0, "192.168.1.0/24")
	b.Columns[3].SetValue(0, "aa:bb:cc:dd:ee:ff")
	b.Columns[4].SetValue(0, int32(443))
	b.Columns[5].SetValue(0, int32(6))
	b.Columns[0].SetValue(1, "9.255.255.255")
	b.Columns[1].SetValue(1, "2001:db8::2")
	b.Columns[2].SetValue(1, "10.0.0.0/8")
	b.Columns[3].SetValue(1, "11:22:33:44:55:66")
	b.Columns[4].SetValue(1, int32(53))
	b.Columns[5].SetValue(1, int32(17))
	return b
}

// TestNetworkTypeFunctionArgumentMatrix sweeps a representative network
// function argument per network-native type over both engine paths.
func TestNetworkTypeFunctionArgumentMatrix(t *testing.T) {
	tests := []struct {
		name string
		call *FuncCall
		want any
	}{
		{"ipv4: ip_to_string", &FuncCall{Name: "ip_to_string", Args: []Expr{&ColRef{Name: "c_ipv4"}}}, "10.1.2.3"},
		{"ipv4: cidr_contains", &FuncCall{Name: "cidr_contains", Args: []Expr{&Lit{Val: "10.0.0.0/8"}, &ColRef{Name: "c_ipv4"}}}, true},
		{"mac: mac_vendor_oui", &FuncCall{Name: "mac_vendor_oui", Args: []Expr{&ColRef{Name: "c_mac"}}}, "AA:BB:CC"},
		{"mac: mac_to_string", &FuncCall{Name: "mac_to_string", Args: []Expr{&ColRef{Name: "c_mac"}}}, "aa:bb:cc:dd:ee:ff"},
		// Regression guards: types that already worked before the fix.
		{"ipv6 (guard): ip_to_string", &FuncCall{Name: "ip_to_string", Args: []Expr{&ColRef{Name: "c_ipv6"}}}, "2001:db8::1"},
		{"cidr (guard): network_address", &FuncCall{Name: "network_address", Args: []Expr{&ColRef{Name: "c_cidr"}}}, "192.168.1.0"},
		{"port (guard): port_name", &FuncCall{Name: "port_name", Args: []Expr{&ColRef{Name: "c_port"}}}, "https"},
		{"protocol (guard): protocol_name", &FuncCall{Name: "protocol_name", Args: []Expr{&ColRef{Name: "c_proto"}}}, "tcp"},
	}

	for _, tt := range tests {
		t.Run(tt.name+"/Eval", func(t *testing.T) {
			b := netTypeMatrixBatch(t)
			if got := tt.call.Eval(b, 0); got != tt.want {
				t.Errorf("Eval row0 = %#v, want %#v", got, tt.want)
			}
		})
		t.Run(tt.name+"/EvalVec", func(t *testing.T) {
			b := netTypeMatrixBatch(t)
			out := batch.NewVector(batch.TypeString, b.Len)
			if tt.want == true || tt.want == false {
				out = batch.NewVector(batch.TypeBool, b.Len)
			}
			tt.call.EvalVec(b, out, b.Len)
			got := out.GetValue(0)
			if got != tt.want {
				t.Errorf("EvalVec row0 = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestNetworkTypeCastToStringMatrix sweeps CAST(col AS STRING) over every
// network-native type.
func TestNetworkTypeCastToStringMatrix(t *testing.T) {
	tests := []struct {
		name string
		col  string
		want string
	}{
		{"ipv4", "c_ipv4", "10.1.2.3"},
		{"mac", "c_mac", "aa:bb:cc:dd:ee:ff"},
		{"ipv6 (guard)", "c_ipv6", "2001:db8::1"},
		{"cidr (guard)", "c_cidr", "192.168.1.0/24"},
		{"port (guard)", "c_port", "443"},
		{"protocol (guard)", "c_proto", "6"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := netTypeMatrixBatch(t)
			c := &Cast{Operand: &ColRef{Name: tt.col}, DestType: "string"}
			if got := c.Eval(b, 0); got != tt.want {
				t.Errorf("CAST(%s AS STRING) row0 = %#v, want %#v", tt.col, got, tt.want)
			}
		})
	}
}

// TestNetworkTypeComparisonLiteralMatrix sweeps a comparison against a
// string literal over every network-native type, including an ordering
// (<, >) check that only passes under NUMERIC address comparison — the
// regression this fix must not introduce by switching IPv4/MAC's general
// boxing to a formatted string (which would make compare() take the
// lexical-string fast path for a column-vs-column or column-vs-literal
// ordering test).
func TestNetworkTypeComparisonLiteralMatrix(t *testing.T) {
	tests := []struct {
		name string
		cmp  Expr
		want bool
	}{
		{"ipv4 = literal", compileCmp(&ColRef{Name: "c_ipv4"}, &Lit{Val: "10.1.2.3"}, CmpEq), true},
		{"ipv4 <> literal", compileCmp(&ColRef{Name: "c_ipv4"}, &Lit{Val: "9.9.9.9"}, CmpEq), false},
		{"ipv4 > literal is numeric order", compileCmp(&ColRef{Name: "c_ipv4"}, &Lit{Val: "9.255.255.255"}, CmpGt), true},
		{"mac = literal", compileCmp(&ColRef{Name: "c_mac"}, &Lit{Val: "aa:bb:cc:dd:ee:ff"}, CmpEq), true},
		{"ipv6 = literal (guard)", compileCmp(&ColRef{Name: "c_ipv6"}, &Lit{Val: "2001:db8::1"}, CmpEq), true},
		{"cidr = literal (guard)", compileCmp(&ColRef{Name: "c_cidr"}, &Lit{Val: "192.168.1.0/24"}, CmpEq), true},
		{"port = literal (guard)", compileCmp(&ColRef{Name: "c_port"}, &Lit{Val: "443"}, CmpEq), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := netTypeMatrixBatch(t)
			be, ok := tt.cmp.(BoolExpr)
			if !ok {
				t.Fatalf("%T does not implement BoolExpr", tt.cmp)
			}
			if got := be.EvalBool(b, 0); got != tt.want {
				t.Errorf("row0 = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIPv4ColumnToColumnOrderingUsesNumericEncoding pins that a
// column-to-column ordering comparison (which never goes through
// CmpNetworkLit/CmpTemporalLit — both operands are ColRefs) still compares
// the raw int64 address encoding, not a formatted string: row 0's c_ipv4
// (10.1.2.3) is numerically GREATER than row 1's (9.255.255.255) but
// lexically SMALLER as text, so this only passes under numeric comparison.
func TestIPv4ColumnToColumnOrderingUsesNumericEncoding(t *testing.T) {
	// A dedicated two-column, one-row batch so a plain Cmp of two ColRefs is
	// exercised (no literal on either side).
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeIPv4},
		{Name: "z", Type: parquet.TypeIPv4},
	}
	one := batch.NewRecordBatch(schema, 1)
	one.Columns[0].SetValue(0, "10.1.2.3")
	one.Columns[1].SetValue(0, "9.255.255.255")

	cmp := compileCmp(&ColRef{Name: "a"}, &ColRef{Name: "z"}, CmpGt)
	be, ok := cmp.(BoolExpr)
	if !ok {
		t.Fatalf("%T does not implement BoolExpr", cmp)
	}
	if got := be.EvalBool(one, 0); !got {
		t.Errorf("a(10.1.2.3) > z(9.255.255.255) = %v, want true (numeric ordering)", got)
	}
}
