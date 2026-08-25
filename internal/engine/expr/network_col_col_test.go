package expr

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ccOps is the six comparison operators in both spellings, so one table drives
// the row-at-a-time evaluator and the vectorized kernel over the same pair.
var ccOps = []struct {
	name string
	expr CmpOp
	kern kernel.CompareOp
}{
	{"=", CmpEq, kernel.OpEq},
	{"<>", CmpNe, kernel.OpNe},
	{"<", CmpLt, kernel.OpLt},
	{"<=", CmpLe, kernel.OpLe},
	{">", CmpGt, kernel.OpGt},
	{">=", CmpGe, kernel.OpGe},
}

// ccRowPath evaluates `a OP z` at row 0 through the row-at-a-time evaluator —
// two ColRefs, so no literal binding (CmpNetworkLit) applies and the pair
// takes the generic comparison the DAG's re-parsed filter and every projection
// also take.
func ccRowPath(t *testing.T, b *batch.RecordBatch, op CmpOp) bool {
	t.Helper()
	e := compileCmp(&ColRef{Name: "a"}, &ColRef{Name: "z"}, op)
	be, ok := e.(BoolExpr)
	if !ok {
		t.Fatalf("%T does not implement BoolExpr", e)
	}
	return be.EvalBool(b, 0)
}

// ccKernelPath evaluates the same comparison through the vectorized col-col
// kernel: the row survives iff the kernel keeps index 0.
func ccKernelPath(t *testing.T, b *batch.RecordBatch, typ batch.TypeID, op kernel.CompareOp) bool {
	t.Helper()
	k := kernel.ResolveColColFilterKernel(typ, op)
	if k == nil {
		t.Fatalf("no col-col kernel for %v %v", typ, op)
	}
	return len(k(b.Columns[0], b.Columns[1], nil, 1, make([]uint32, 0, 1))) == 1
}

// TestColColNetworkComparisonAgreesWithTheKernel is the two-SITE half of
// #565: for a column-to-column comparison over each network type, the
// vectorized kernel and the row-at-a-time evaluator must answer the same
// thing, because a query reaches one or the other for reasons that have
// nothing to do with its meaning — a WHERE clause on a scanned column takes
// the kernel, a projected value or a later DAG stage's re-parsed filter takes
// the row path.
//
// Each pair below is chosen so LEXICAL text order and the ADDRESS's own order
// DISAGREE, which is what makes the two sites distinguishable at all:
//
//   - IPv4 / MAC: ColRef.Eval boxes the raw encoded int64, which sorts as the
//     address does, and the kernel reads the same int64. They agreed before
//     this test existed and it pins that.
//   - IPv6: the kernel compares the RAW 16 BYTES; the row path boxes the
//     RENDERED text, whose lexical order is not the address's ("2001:db8::9"
//     sorts ABOVE "2001:db8::10" as text and BELOW it as an address).
//   - UUID: the row path boxes the zero-padded, fixed-width 32-hex-digit
//     text, whose lexical order IS the byte order — the accident
//     TestUUIDOrderingIsCorrectByHexAccident already pins, checked here for
//     the col-col shape too.
//   - CIDR: both must use PostgreSQL's inet order (kernel.CidrOrderKey).
func TestColColNetworkComparisonAgreesWithTheKernel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     parquet.TypeID
		btyp    batch.TypeID
		a, z    string
		wantLt  bool // is a strictly BELOW z in the type's own order?
		wantEq  bool
		comment string
	}{
		{
			name: "ipv4", typ: parquet.TypeIPv4, btyp: batch.TypeIPv4,
			a: "10.1.2.3", z: "9.255.255.255", wantLt: false,
			comment: "10.x is the larger address and the smaller text",
		},
		{
			name: "mac", typ: parquet.TypeMAC, btyp: batch.TypeMAC,
			a: "aa:bb:cc:dd:ee:09", z: "aa:bb:cc:dd:ee:10", wantLt: true,
			comment: "hex 09 < hex 10 both numerically and as this fixed-width text",
		},
		{
			name: "ipv6", typ: parquet.TypeIPv6, btyp: batch.TypeIPv6,
			a: "2001:db8::9", z: "2001:db8::10", wantLt: true,
			comment: "::9 is BELOW ::10 as an address and ABOVE it as text",
		},
		{
			name: "uuid", typ: parquet.TypeUUID, btyp: batch.TypeUUID,
			a: "0fffffff-0000-0000-0000-000000000000",
			z: "10000000-0000-0000-0000-000000000000", wantLt: true,
			comment: "fixed-width hex: lexical order is the byte order",
		},
		{
			name: "cidr_host_vs_slash32", typ: parquet.TypeCIDR, btyp: batch.TypeCIDR,
			a: "10.0.0.1", z: "10.0.0.1/32", wantEq: true,
			comment: "PostgreSQL inet: a bare address IS its own /32 host route",
		},
		{
			name: "cidr_mask_order", typ: parquet.TypeCIDR, btyp: batch.TypeCIDR,
			a: "10.0.0.0/24", z: "9.0.0.0/8", wantLt: false,
			comment: "10.x is the larger address; as text 10.0.0.0/24 sorts below 9.0.0.0/8",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{{Name: "a", Type: tc.typ}, {Name: "z", Type: tc.typ}}
			b := batch.NewRecordBatch(schema, 1)
			b.Columns[0].SetValue(0, tc.a)
			b.Columns[1].SetValue(0, tc.z)

			for _, op := range ccOps {
				want := map[string]bool{
					"=":  tc.wantEq,
					"<>": !tc.wantEq,
					"<":  tc.wantLt,
					"<=": tc.wantLt || tc.wantEq,
					">":  !tc.wantLt && !tc.wantEq,
					">=": !tc.wantLt,
				}[op.name]

				row := ccRowPath(t, b, op.expr)
				ker := ccKernelPath(t, b, tc.btyp, op.kern)
				if row != ker {
					t.Errorf("%q %s %q: the two sites disagree — row-at-a-time %v, vectorized kernel %v\n"+
						"  a WHERE clause and a projection of the same comparison answer differently (%s)",
						tc.a, op.name, tc.z, row, ker, tc.comment)
				}
				if row != want {
					t.Errorf("%q %s %q: row-at-a-time answered %v, want %v (%s)",
						tc.a, op.name, tc.z, row, want, tc.comment)
				}
				if ker != want {
					t.Errorf("%q %s %q: vectorized kernel answered %v, want %v (%s)",
						tc.a, op.name, tc.z, ker, want, tc.comment)
				}
			}
		})
	}
}

// TestColColCidrMalformedIsUnknown pins ADR-0012 item 10's malformed-STORED-
// value rule for a column-to-column comparison: a value that names no address
// has no place in the order, so it matches NOTHING for every operator, `<>`
// included — the answer a NULL row gets — and never falls back to comparing
// the two texts.
//
// Both sites must answer that. The kernel used to key such a row through
// kernel.CidrOrderKey, whose whole purpose is to give an unparseable value a
// definite position for ORDER BY and GROUP BY — right for a KEY and wrong for
// a PREDICATE, and the row path's new arm would then have disagreed with it
// for exactly the rows the column is unvalidated for.
func TestColColCidrMalformedIsUnknown(t *testing.T) {
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeCIDR}, {Name: "z", Type: parquet.TypeCIDR}}
	for _, tc := range []struct{ name, a, z string }{
		{"left malformed", "not-an-address", "10.0.0.1/32"},
		{"right malformed", "10.0.0.1/32", "not-an-address"},
		{"both malformed, identical text", "not-an-address", "not-an-address"},
		{"both malformed, different text", "not-an-address", "other-garbage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := batch.NewRecordBatch(schema, 1)
			b.Columns[0].SetValue(0, tc.a)
			b.Columns[1].SetValue(0, tc.z)
			for _, op := range ccOps {
				if got := ccRowPath(t, b, op.expr); got {
					t.Errorf("row-at-a-time %q %s %q = true; a malformed stored value is UNKNOWN "+
						"and matches nothing, `<>` included (ADR-0012 item 10)", tc.a, op.name, tc.z)
				}
				if got := ccKernelPath(t, b, batch.TypeCIDR, op.kern); got {
					t.Errorf("vectorized kernel %q %s %q = true; a malformed stored value is UNKNOWN "+
						"and matches nothing, `<>` included (ADR-0012 item 10)", tc.a, op.name, tc.z)
				}
			}
		})
	}
}

// TestColColCidrNullIsUnknown pins that the CIDR arm's NULL handling is the
// one every other comparison already has: a comparison against NULL is
// UNKNOWN, so the row never passes and `<>` does not rescue it.
func TestColColCidrNullIsUnknown(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "z", Type: parquet.TypeCIDR, Nullable: true},
	}
	for _, tc := range []struct {
		name         string
		aNull, zNull bool
	}{
		{"left null", true, false},
		{"right null", false, true},
		{"both null", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := batch.NewRecordBatch(schema, 1)
			if tc.aNull {
				b.Columns[0].Nulls.SetNull(0)
			} else {
				b.Columns[0].SetValue(0, "10.0.0.1")
			}
			if tc.zNull {
				b.Columns[1].Nulls.SetNull(0)
			} else {
				b.Columns[1].SetValue(0, "10.0.0.1/32")
			}
			for _, op := range ccOps {
				if got := ccRowPath(t, b, op.expr); got {
					t.Errorf("row-at-a-time a %s z = true with a NULL operand; want UNKNOWN", op.name)
				}
				if got := ccKernelPath(t, b, batch.TypeCIDR, op.kern); got {
					t.Errorf("vectorized kernel a %s z = true with a NULL operand; want UNKNOWN", op.name)
				}
			}
		})
	}
}

// TestBoxedCidrSiteUsesInetOrder is the boxed-SITE half of #565: the three
// sites #506 had to bind from the operands' declarations for DECIMAL —
// a simple CASE's operand against one WHEN, IS DISTINCT FROM, and
// GREATEST/LEAST — reach compare() with two plain Go strings and no way to
// tell a CIDR value from a STRING one, so they compared the stored texts.
func TestBoxedCidrSiteUsesInetOrder(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeCIDR},
		{Name: "z", Type: parquet.TypeCIDR},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, "10.0.0.1")
	b.Columns[1].SetValue(0, "10.0.0.1/32")

	col := func(n string) Expr { return &ColRef{Name: n} }

	t.Run("is distinct from", func(t *testing.T) {
		e := &IsDistinctFrom{Left: col("a"), Right: col("z")}
		if got := e.EvalBool(b, 0); got {
			t.Errorf("'10.0.0.1' IS DISTINCT FROM '10.0.0.1/32' = %v, want false "+
				"(one PostgreSQL inet value)", got)
		}
	})

	t.Run("simple case", func(t *testing.T) {
		e := &Case{
			Operand: col("a"),
			Whens:   []CaseWhen{{Cond: col("z"), Result: &Lit{Val: int64(1)}}},
			Else:    &Lit{Val: int64(0)},
		}
		if got := fmt.Sprint(e.Eval(b, 0)); got != "1" {
			t.Errorf("CASE a WHEN z THEN 1 ELSE 0 END = %s, want 1 (the two spellings are one value)", got)
		}
	})

	t.Run("greatest picks by inet order", func(t *testing.T) {
		// 9.0.0.0/8 is the LARGER text and the SMALLER address, so a lexical
		// GREATEST answers it and an inet one answers 10.0.0.0/24.
		two := batch.NewRecordBatch(schema, 1)
		two.Columns[0].SetValue(0, "10.0.0.0/24")
		two.Columns[1].SetValue(0, "9.0.0.0/8")
		e := &FuncCall{Name: "greatest", Args: []Expr{col("a"), col("z")}}
		if got := fmt.Sprint(e.Eval(two, 0)); got != "10.0.0.0/24" {
			t.Errorf("GREATEST('10.0.0.0/24', '9.0.0.0/8') = %s, want 10.0.0.0/24 "+
				"(the larger ADDRESS, not the larger text)", got)
		}
	})
}
