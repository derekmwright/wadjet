package exec

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
)

// Predicate evaluates a row and returns true if it passes the filter.
type Predicate func(b *batch.RecordBatch, row int) bool

// CompareOp represents a comparison operation.
type CompareOp int

const (
	OpEq CompareOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIsNull
	OpIsNotNull
)

// Filter is a UnaryOperator that filters rows using a selection vector.
type Filter struct {
	Pred   Predicate
	selBuf []uint16 // reusable selection vector to avoid per-batch allocation
}

func NewFilter(pred Predicate) *Filter {
	return &Filter{Pred: pred}
}

func (f *Filter) Init(_ context.Context) error { return nil }

func (f *Filter) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if cap(f.selBuf) < in.Len {
		f.selBuf = make([]uint16, 0, in.Len)
	}
	sel := f.selBuf[:0]

	if in.Sel != nil {
		for _, idx := range in.Sel {
			if f.Pred(in, int(idx)) {
				sel = append(sel, idx)
			}
		}
	} else {
		for i := 0; i < in.Len; i++ {
			if f.Pred(in, i) {
				sel = append(sel, uint16(i))
			}
		}
	}

	f.selBuf = sel // save grown slice for next call

	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (f *Filter) Close() error { return nil }

// Clone returns a new Filter that shares the same predicate closure but has
// its own scratch buffer, allowing concurrent Execute calls.
func (f *Filter) Clone() UnaryOperator {
	return &Filter{Pred: f.Pred}
}

// ColumnCompare creates a predicate that compares a column against a constant value.
func ColumnCompare(colName string, op CompareOp, value any) Predicate {
	cachedIdx := -2 // -2 = unresolved, -1 = not found
	var cachedNetInt int64
	var cachedNetStr string
	netResolved := false
	return func(b *batch.RecordBatch, row int) bool {
		if cachedIdx == -2 {
			cachedIdx = b.ColumnIndex(colName)
		}
		idx := cachedIdx
		if idx < 0 {
			return false
		}
		v := b.Columns[idx]

		if op == OpIsNull {
			return v.Nulls.IsNull(row)
		}
		if op == OpIsNotNull {
			return !v.Nulls.IsNull(row)
		}

		if v.Nulls.IsNull(row) {
			return false
		}

		switch v.Type {
		case batch.TypeInt64, batch.TypeTimestamp:
			return compareInt64(v.Int64Data[row], toInt64(value), op)
		case batch.TypeInt32:
			return compareInt64(int64(v.Int32Data[row]), toInt64(value), op)
		case batch.TypeFloat64:
			return compareFloat64(v.Float64Data[row], toFloat64(value), op)
		case batch.TypeFloat32:
			return compareFloat64(float64(v.Float32Data[row]), toFloat64(value), op)
		case batch.TypeString:
			return compareString(v.BytesData.UnsafeStringValue(row), fmt.Sprint(value), op)
		case batch.TypeBool:
			if op == OpEq {
				return v.BoolData[row] == value.(bool)
			}
			if op == OpNe {
				return v.BoolData[row] != value.(bool)
			}
		case batch.TypeIPv4:
			if !netResolved {
				cachedNetInt = parseIPv4FilterVal(value)
				netResolved = true
			}
			return compareInt64(v.Int64Data[row], cachedNetInt, op)
		case batch.TypeMAC:
			if !netResolved {
				cachedNetInt = parseMACFilterVal(value)
				netResolved = true
			}
			return compareInt64(v.Int64Data[row], cachedNetInt, op)
		case batch.TypeIPv6:
			if !netResolved {
				cachedNetStr = parseIPv6FilterVal(value)
				netResolved = true
			}
			return compareString(v.BytesData.UnsafeStringValue(row), cachedNetStr, op)
		case batch.TypeCIDR:
			return compareString(v.BytesData.UnsafeStringValue(row), fmt.Sprint(value), op)
		case batch.TypePort, batch.TypeProtocol:
			return compareInt64(int64(v.Int32Data[row]), toInt64(value), op)
		case batch.TypeDuration:
			return compareInt64(v.Int64Data[row], toInt64(value), op)
		case batch.TypeUUID:
			return compareString(v.BytesData.UnsafeStringValue(row), fmt.Sprint(value), op)
		case batch.TypeDate:
			return compareInt64(int64(v.Int32Data[row]), toInt64(value), op)
		}
		return false
	}
}

func parseIPv4FilterVal(value any) int64 {
	s := fmt.Sprint(value)
	ip := net.ParseIP(s)
	if ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			return int64(binary.BigEndian.Uint32(ip4))
		}
	}
	return 0
}

func parseMACFilterVal(value any) int64 {
	s := fmt.Sprint(value)
	hw, err := net.ParseMAC(s)
	if err != nil || len(hw) != 6 {
		return 0
	}
	var n uint64
	for _, b := range hw {
		n = (n << 8) | uint64(b)
	}
	return int64(n)
}

func parseIPv6FilterVal(value any) string {
	s := fmt.Sprint(value)
	ip := net.ParseIP(s)
	if ip != nil {
		return string(ip.To16())
	}
	return ""
}

// ColumnLike creates a predicate that evaluates col LIKE pattern using SQL LIKE semantics.
// The pattern uses % for any sequence of characters and _ for any single character.
func ColumnLike(colName, pattern string, not bool) Predicate {
	cachedIdx := -2
	return func(b *batch.RecordBatch, row int) bool {
		if cachedIdx == -2 {
			cachedIdx = b.ColumnIndex(colName)
		}
		if cachedIdx < 0 {
			return false
		}
		v := b.Columns[cachedIdx]
		if v.Nulls.IsNull(row) {
			return false
		}
		s := v.BytesData.UnsafeStringValue(row)
		result := matchLike(s, pattern)
		if not {
			return !result
		}
		return result
	}
}

// matchLike implements SQL LIKE pattern matching.
// % matches any sequence of characters, _ matches any single character.
func matchLike(s, pattern string) bool {
	return matchLikeRecur(s, pattern, 0, 0)
}

func matchLikeRecur(s, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		if pattern[pi] == '%' {
			pi++
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true
			}
			for i := si; i <= len(s); i++ {
				if matchLikeRecur(s, pattern, i, pi) {
					return true
				}
			}
			return false
		}
		if si >= len(s) {
			return false
		}
		if pattern[pi] == '_' || pattern[pi] == s[si] {
			si++
			pi++
		} else {
			return false
		}
	}
	return si == len(s)
}

// And combines predicates with logical AND.
func And(preds ...Predicate) Predicate {
	return func(b *batch.RecordBatch, row int) bool {
		for _, p := range preds {
			if !p(b, row) {
				return false
			}
		}
		return true
	}
}

// Or combines predicates with logical OR.
func Or(preds ...Predicate) Predicate {
	return func(b *batch.RecordBatch, row int) bool {
		for _, p := range preds {
			if p(b, row) {
				return true
			}
		}
		return false
	}
}

func compareInt64(a, b int64, op CompareOp) bool {
	switch op {
	case OpEq:
		return a == b
	case OpNe:
		return a != b
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	default:
		return false
	}
}

func compareFloat64(a, b float64, op CompareOp) bool {
	switch op {
	case OpEq:
		return a == b
	case OpNe:
		return a != b
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	default:
		return false
	}
}

func compareString(a, b string, op CompareOp) bool {
	switch op {
	case OpEq:
		return a == b
	case OpNe:
		return a != b
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	default:
		return false
	}
}

// KernelFilter is a UnaryOperator that uses a pre-resolved typed filter kernel.
// The type dispatch happens once on first Execute; the inner loop has no type switches.
type KernelFilter struct {
	ColName  string
	Op       CompareOp
	Value    any
	colIdx   int
	kern     kernel.FilterKernel
	outSel   []uint16
	resolved bool
}

// NewKernelFilter creates a filter that uses typed kernels for comparison.
func NewKernelFilter(colName string, op CompareOp, value any) *KernelFilter {
	return &KernelFilter{ColName: colName, Op: op, Value: value}
}

func (f *KernelFilter) Init(_ context.Context) error { return nil }

func (f *KernelFilter) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			// Strip table alias qualifier (e.g. "n1.n_name" → "n_name")
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
		}
		if f.colIdx >= 0 {
			typ := in.Columns[f.colIdx].Type
			f.kern = kernel.ResolveFilterKernel(typ, toKernelOp(f.Op), f.Value)
		}
		f.outSel = make([]uint16, 0, in.Len)
		f.resolved = true
	}

	if f.colIdx < 0 || f.kern == nil {
		return nil, nil
	}

	sel := f.kern(in.Columns[f.colIdx], in.Sel, in.Len, f.outSel)
	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (f *KernelFilter) Close() error { return nil }

// Clone returns a new KernelFilter with the same parameters but fresh
// resolution state and scratch buffers for concurrent Execute calls.
func (f *KernelFilter) Clone() UnaryOperator {
	return &KernelFilter{ColName: f.ColName, Op: f.Op, Value: f.Value}
}

// InFilter uses a vectorized kernel for set membership testing (IN / NOT IN).
type InFilter struct {
	ColName  string
	Values   []any
	Negate   bool
	colIdx   int
	kern     kernel.FilterKernel
	outSel   []uint16
	resolved bool
}

func NewInFilter(colName string, values []any, negate bool) *InFilter {
	return &InFilter{ColName: colName, Values: values, Negate: negate}
}

func (f *InFilter) Init(_ context.Context) error { return nil }

func (f *InFilter) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
		}
		if f.colIdx >= 0 {
			typ := in.Columns[f.colIdx].Type
			f.kern = kernel.ResolveInFilterKernel(typ, f.Values, f.Negate)
		}
		f.outSel = make([]uint16, 0, in.Len)
		f.resolved = true
	}
	if f.colIdx < 0 || f.kern == nil {
		return nil, nil
	}
	sel := f.kern(in.Columns[f.colIdx], in.Sel, in.Len, f.outSel)
	if len(sel) == 0 {
		return nil, nil
	}
	in.Sel = sel
	return in, nil
}

func (f *InFilter) Close() error { return nil }

func (f *InFilter) Clone() UnaryOperator {
	return &InFilter{ColName: f.ColName, Values: f.Values, Negate: f.Negate}
}

// LikeFilter uses a vectorized kernel for SQL LIKE pattern matching.
type LikeFilter struct {
	ColName  string
	Pattern  string
	Negate   bool
	colIdx   int
	kern     kernel.FilterKernel
	outSel   []uint16
	resolved bool
}

func NewLikeFilter(colName, pattern string, negate bool) *LikeFilter {
	return &LikeFilter{ColName: colName, Pattern: pattern, Negate: negate}
}

func (f *LikeFilter) Init(_ context.Context) error { return nil }

func (f *LikeFilter) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
		}
		if f.colIdx >= 0 {
			f.kern = kernel.ResolveLikeFilterKernel(f.Pattern, f.Negate)
		}
		f.outSel = make([]uint16, 0, in.Len)
		f.resolved = true
	}
	if f.colIdx < 0 || f.kern == nil {
		return nil, nil
	}
	sel := f.kern(in.Columns[f.colIdx], in.Sel, in.Len, f.outSel)
	if len(sel) == 0 {
		return nil, nil
	}
	in.Sel = sel
	return in, nil
}

func (f *LikeFilter) Close() error { return nil }

func (f *LikeFilter) Clone() UnaryOperator {
	return &LikeFilter{ColName: f.ColName, Pattern: f.Pattern, Negate: f.Negate}
}

func toKernelOp(op CompareOp) kernel.CompareOp {
	switch op {
	case OpEq:
		return kernel.OpEq
	case OpNe:
		return kernel.OpNe
	case OpLt:
		return kernel.OpLt
	case OpLe:
		return kernel.OpLe
	case OpGt:
		return kernel.OpGt
	case OpGe:
		return kernel.OpGe
	default:
		return kernel.OpEq
	}
}

func toInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int:
		return int64(tv)
	case int32:
		return int64(tv)
	case float64:
		return int64(tv)
	case string:
		return parseTimestampString(tv)
	default:
		return 0
	}
}

func parseTimestampString(s string) int64 {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

// ChainFilter applies a sequence of unary filter operators in order.
// Each operator narrows the selection vector before passing to the next.
type ChainFilter struct {
	Ops []UnaryOperator
}

func NewChainFilter(ops []UnaryOperator) *ChainFilter {
	return &ChainFilter{Ops: ops}
}

func (f *ChainFilter) Init(ctx context.Context) error {
	for _, op := range f.Ops {
		if err := op.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (f *ChainFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	for _, op := range f.Ops {
		var err error
		in, err = op.Execute(ctx, in)
		if err != nil {
			return nil, err
		}
		if in == nil {
			return nil, nil
		}
	}
	return in, nil
}

func (f *ChainFilter) Close() error {
	for _, op := range f.Ops {
		op.Close()
	}
	return nil
}

// Clone returns a new ChainFilter with cloned sub-operators.
// Sub-operators that implement Cloneable are cloned; others are shared.
func (f *ChainFilter) Clone() UnaryOperator {
	cloned := make([]UnaryOperator, len(f.Ops))
	for i, op := range f.Ops {
		if c, ok := op.(Cloneable); ok {
			cloned[i] = c.Clone()
		} else {
			cloned[i] = op
		}
	}
	return &ChainFilter{Ops: cloned}
}

func toFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	default:
		return 0
	}
}

// ColColFilter compares two columns element-wise using a vectorized kernel.
// Resolves column indices and kernel on first Execute; inner loop has no type switches.
type ColColFilter struct {
	LeftCol  string
	RightCol string
	Op       CompareOp
	leftIdx  int
	rightIdx int
	kern     kernel.ColColFilterKernel
	outSel   []uint16
	resolved bool
}

func NewColColFilter(leftCol, rightCol string, op CompareOp) *ColColFilter {
	return &ColColFilter{LeftCol: leftCol, RightCol: rightCol, Op: op}
}

func (f *ColColFilter) Init(_ context.Context) error { return nil }

func (f *ColColFilter) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.leftIdx = in.ColumnIndex(f.LeftCol)
		if f.leftIdx < 0 && strings.Contains(f.LeftCol, ".") {
			parts := strings.SplitN(f.LeftCol, ".", 2)
			f.leftIdx = in.ColumnIndex(parts[1])
		}
		f.rightIdx = in.ColumnIndex(f.RightCol)
		if f.rightIdx < 0 && strings.Contains(f.RightCol, ".") {
			parts := strings.SplitN(f.RightCol, ".", 2)
			f.rightIdx = in.ColumnIndex(parts[1])
		}
		if f.leftIdx >= 0 && f.rightIdx >= 0 {
			typ := in.Columns[f.leftIdx].Type
			f.kern = kernel.ResolveColColFilterKernel(typ, toKernelOp(f.Op))
		}
		f.outSel = make([]uint16, 0, in.Len)
		f.resolved = true
	}

	if f.kern == nil {
		return nil, fmt.Errorf("ColColFilter: could not resolve kernel for %s %v %s (leftIdx=%d, rightIdx=%d)",
			f.LeftCol, f.Op, f.RightCol, f.leftIdx, f.rightIdx)
	}

	sel := f.kern(in.Columns[f.leftIdx], in.Columns[f.rightIdx], in.Sel, in.Len, f.outSel)
	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (f *ColColFilter) Close() error { return nil }

// Clone returns a new ColColFilter with the same parameters but fresh
// resolution state and scratch buffers for concurrent Execute calls.
func (f *ColColFilter) Clone() UnaryOperator {
	return &ColColFilter{LeftCol: f.LeftCol, RightCol: f.RightCol, Op: f.Op}
}
