// SPDX-License-Identifier: MIT

// This file holds the recursive CTE's column-type resolution for the physical
// planner, governed by ADR-0021 §1o-b and ADR-0012.
package physical

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// recursiveArmLiterals marks, per output position of an arm, what PostgreSQL
// types from CONTEXT rather than from the value: an UNKNOWN literal (a quoted
// string or a bare NULL) and a numeric literal with a fraction or exponent,
// which PostgreSQL reads as numeric and this engine's declared-type layer as
// double precision.
type recursiveArmLiterals struct {
	unknown []bool
	numeric []*setOpLitDecimal
}

func recursiveArmLiteralsOf(plan *logical.Node, cols int) recursiveArmLiterals {
	var out recursiveArmLiterals
	if plan == nil || cols == 0 {
		return out
	}
	out.unknown = setOpUnknownLiteralArms(plan, cols)
	if lits := setOpArmLiterals(plan); len(lits) == cols {
		out.numeric = lits
	}
	return out
}

func (l recursiveArmLiterals) isUnknown(i int) bool { return i < len(l.unknown) && l.unknown[i] }

func (l recursiveArmLiterals) numericText(i int) (string, bool) {
	if i < len(l.numeric) && l.numeric[i] != nil {
		return l.numeric[i].text, true
	}
	return "", false
}

// coerceRecursiveTerm converts produced batches to the seed declaration.
// Unsupported pairs raise 42804; integer narrowing is range-checked (22003).
// Unknown literals use the seed input rules. Integer widths and text/varchar
// share carriers beyond PostgreSQL matching, as recorded in ADR-0021 §1o-b.
// A finer unconstrained numeric value restarts the closure at a wider scale,
// up to 38; no earlier row is kept at the narrower scale. Empty terms have
// no batch type to check. See the seed/term table in docs/sql-reference.md.
func coerceRecursiveTerm(name string, anchor []parquet.Column, batches []*batch.RecordBatch,
	lits recursiveArmLiterals) ([]*batch.RecordBatch, error) {
	out := make([]*batch.RecordBatch, 0, len(batches))
	for _, b := range batches {
		if b == nil || b.ActiveLen() == 0 {
			continue
		}
		if len(b.Columns) != len(anchor) || len(b.Schema) != len(anchor) {
			return nil, sqlerr.New("42601",
				"each UNION query in recursive query %q must have the same number of columns", name)
		}
		converted := false
		for i := range anchor {
			term := b.Schema[i]
			if term.Type == parquet.TypeDecimal && b.Columns[i] != nil && b.Columns[i].Base == nil {
				// The VECTOR's scale is the one its unscaled integers are at;
				// a declaration that says otherwise must not move a value.
				term.Scale = b.Columns[i].DecimalData.Scale
			}
			if sameRecursiveCarrier(anchor[i], term) {
				continue
			}
			conv := recursiveTermConversion(name, i, anchor[i], term, lits)
			if conv.err != nil {
				return nil, conv.err
			}
			if !converted {
				if b.HasViews() {
					b.FlattenViews()
				}
				b = b.Compact()
				b = &batch.RecordBatch{Schema: b.Schema, Columns: append([]*batch.Vector(nil), b.Columns...), Len: b.Len}
				converted = true
			}
			v, err := conv.run(b.Columns[i], b.Len)
			if err != nil {
				return nil, err
			}
			b.Columns[i] = v
		}
		out = append(out, b)
	}
	return out, nil
}

// errWidenRecursiveScale asks iterateRecursiveCTE to restart the fixed point
// with column col declared at scale: an unconstrained numeric column met a
// finer value.
type errWidenRecursiveScale struct{ col, scale int }

func (e *errWidenRecursiveScale) Error() string {
	return fmt.Sprintf("recursive CTE column %d widens to scale %d", e.col+1, e.scale)
}

// unconstrainedNumeric: this engine declares an unconstrained numeric at the
// full 38-digit precision and the value's own scale (`1::numeric` is
// DECIMAL(38,0)); a declared numeric(p,s) keeps its p.
func unconstrainedNumeric(c parquet.Column) bool {
	return c.Type == parquet.TypeDecimal && c.Precision >= 38
}

type recursiveConv struct {
	err error
	// value converts one non-NULL cell of the source column.
	value func(src *batch.Vector, row int, dst *batch.Vector, at int) error
	to    parquet.Column
}

func (c recursiveConv) run(src *batch.Vector, n int) (*batch.Vector, error) {
	dst := batch.NewRecordBatch([]parquet.Column{c.to}, n).Columns[0]
	for i := 0; i < n; i++ {
		if src.Nulls.IsNull(i) {
			dst.WriteNullAt(i)
			continue
		}
		if err := c.value(src, i, dst, i); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func recursiveTermConversion(name string, pos int, seed, term parquet.Column, lits recursiveArmLiterals) recursiveConv {
	refuse := recursiveConv{err: sqlerr.New("42804",
		"recursive query %q column %d has type %s in non-recursive term but type %s overall",
		name, pos+1, pgTypeName(seed.Type), pgTypeName(term.Type))}
	conv := func(f func(src *batch.Vector, row int, dst *batch.Vector, at int) error) recursiveConv {
		return recursiveConv{value: f, to: seed}
	}
	isInt := func(t parquet.TypeID) bool { return t == parquet.TypeInt32 || t == parquet.TypeInt64 }

	// An UNKNOWN literal takes the seed's type: a NULL is a NULL of it, and a
	// quoted string is read by the seed type's input function.
	if lits.isUnknown(pos) && term.Type == parquet.TypeString {
		if seed.Type == parquet.TypeString {
			return recursiveConv{value: copyCell, to: seed}
		}
		// Against a CONSTRAINED numeric seed an unknown literal — a NULL
		// included — is PostgreSQL's 42804: it resolves to numeric without the
		// seed's typmod.
		if seed.Type == parquet.TypeDecimal && !unconstrainedNumeric(seed) {
			return refuse
		}
		read, ok := recursiveTextReader(seed)
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			if !ok {
				// Reached only for a non-NULL value: a bare NULL is a NULL of
				// any type, and this seed type has no text input here.
				return refuse.err
			}
			s, _ := src.GetString(row)
			if err := read(s, dst, at); err != nil {
				var w *errWidenRecursiveScale
				if errors.As(err, &w) {
					w.col = pos
				}
				return err
			}
			return nil
		})
	}
	switch {
	case isInt(seed.Type) && isInt(term.Type):
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			return putInt(dst, at, readInt(src, row))
		})
	case seed.Type == parquet.TypeDecimal && !unconstrainedNumeric(seed):
		// A CONSTRAINED numeric seed accepts exactly its own typmod: every
		// other numeric, an integer and a numeric literal resolve to numeric
		// without it, which PostgreSQL refuses (measured for numeric(10,2)
		// against integer, bigint, numeric, a literal and `n + 1`).
		if term.Type == parquet.TypeDecimal && term.Precision == seed.Precision && term.Scale == seed.Scale {
			return recursiveConv{value: copyCell, to: seed}
		}
		return refuse
	case seed.Type == parquet.TypeDecimal && isInt(term.Type):
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			return putDecimal(dst, at, batch.Int128From(readInt(src, row)), 0, seed)
		})
	case seed.Type == parquet.TypeDecimal && term.Type == parquet.TypeDecimal:
		if term.Scale > seed.Scale {
			// A value that is exact at the seed's scale is stored there (a
			// product's scale is the sum of its operands' — `1.0 * 0.5` is 0.50
			// — so the declared scale alone would widen without end); a finer
			// one widens the column to the scale that value needs.
			return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
				v := src.DecimalData.Data[row]
				for need := seed.Scale; need < term.Scale; need++ {
					if r, ok := batch.Rescale(v, term.Scale, need); ok {
						if back, ok := batch.Rescale(r, need, term.Scale); ok && back.Equal(v) {
							if need == seed.Scale {
								return putDecimal(dst, at, r, need, seed)
							}
							return &errWidenRecursiveScale{col: pos, scale: need}
						}
					}
				}
				return &errWidenRecursiveScale{col: pos, scale: term.Scale}
			})
		}
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			return putDecimal(dst, at, src.DecimalData.Data[row], term.Scale, seed)
		})
	case seed.Type == parquet.TypeDecimal && term.Type == parquet.TypeFloat64:
		text, ok := lits.numericText(pos)
		if !ok {
			return refuse
		}
		if frac := decimalTextScale(strings.TrimSpace(text)); frac > seed.Scale {
			return recursiveConv{err: &errWidenRecursiveScale{col: pos, scale: frac}}
		}
		return conv(func(_ *batch.Vector, _ int, dst *batch.Vector, at int) error {
			return readDecimalText(text, dst, at, seed)
		})
	case seed.Type == parquet.TypeFloat64 &&
		(isInt(term.Type) || term.Type == parquet.TypeFloat32 || term.Type == parquet.TypeDecimal):
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			dst.Float64Data[at] = readFloat(src, row)
			return nil
		})
	case seed.Type == parquet.TypeFloat32 && (isInt(term.Type) || term.Type == parquet.TypeDecimal ||
		(term.Type == parquet.TypeFloat64 && lits.isNumericLiteral(pos))):
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			dst.Float32Data[at] = float32(readFloat(src, row))
			return nil
		})
	case seed.Type == parquet.TypeTimestamp && term.Type == parquet.TypeDate:
		return conv(func(src *batch.Vector, row int, dst *batch.Vector, at int) error {
			dst.Int64Data[at] = int64(src.Int32Data[row]) * 86_400_000
			return nil
		})
	}
	return refuse
}

func (l recursiveArmLiterals) isNumericLiteral(i int) bool {
	_, ok := l.numericText(i)
	return ok
}

func copyCell(src *batch.Vector, row int, dst *batch.Vector, at int) error {
	dst.CopyValueFrom(at, src, row)
	return nil
}

func readInt(v *batch.Vector, row int) int64 {
	if v.Type == parquet.TypeInt32 {
		return int64(v.Int32Data[row])
	}
	return v.Int64Data[row]
}

func readFloat(v *batch.Vector, row int) float64 {
	switch v.Type {
	case parquet.TypeInt32:
		return float64(v.Int32Data[row])
	case parquet.TypeInt64:
		return float64(v.Int64Data[row])
	case parquet.TypeFloat32:
		return float64(v.Float32Data[row])
	case parquet.TypeDecimal:
		return v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale)
	}
	return v.Float64Data[row]
}

func putInt(dst *batch.Vector, at int, v int64) error {
	if dst.Type == parquet.TypeInt32 {
		if v < math.MinInt32 || v > math.MaxInt32 {
			return sqlerr.New("22003", "integer out of range")
		}
		dst.Int32Data[at] = int32(v)
		return nil
	}
	dst.Int64Data[at] = v
	return nil
}

// putDecimal stores v (at fromScale) into a DECIMAL column at the seed's
// scale, which is never smaller than fromScale here (a finer value is
// rescaled exactly by its caller or widens the column), and refuses a value past
// the seed's precision with PostgreSQL's 22003.
func putDecimal(dst *batch.Vector, at int, v batch.Int128, fromScale int, seed parquet.Column) error {
	r, ok := batch.Rescale(v, fromScale, seed.Scale)
	if !ok || (seed.Precision > 0 && !batch.DecimalFitsPrecision(r, seed.Precision)) {
		return sqlerr.New("22003", "numeric field overflow")
	}
	dst.DecimalData.Data[at] = r
	return nil
}

func readDecimalText(text string, dst *batch.Vector, at int, seed parquet.Column) error {
	t := strings.TrimSpace(text)
	if frac := decimalTextScale(t); frac > seed.Scale {
		// A finer value than the column carries: the column widens (the
		// caller names the position).
		return &errWidenRecursiveScale{scale: frac}
	}
	v, err := batch.ParseDecimalStringChecked(t, seed.Scale)
	if err != nil {
		return err
	}
	return putDecimal(dst, at, v, seed.Scale, seed)
}

// decimalTextScale is the number of digits after the point a plain decimal
// literal spells; an exponent form answers 0 and is left to the parser.
func decimalTextScale(s string) int {
	if strings.ContainsAny(s, "eE") {
		return 0
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return len(s) - i - 1
	}
	return 0
}

// recursiveTextReader is the seed type's input function for an unknown
// literal, with PostgreSQL's class for text it does not read.
func recursiveTextReader(seed parquet.Column) (func(s string, dst *batch.Vector, at int) error, bool) {
	bad := func(typ, s string) error {
		code := "22P02"
		if typ == "date" || typ == "timestamp" {
			code = "22007"
		}
		return sqlerr.New(code, "invalid input syntax for type %s: %q", typ, s)
	}
	switch seed.Type {
	case parquet.TypeInt32, parquet.TypeInt64:
		typ := "integer"
		if seed.Type == parquet.TypeInt64 {
			typ = "bigint"
		}
		return func(s string, dst *batch.Vector, at int) error {
			v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if err != nil {
				return bad(typ, s)
			}
			return putInt(dst, at, v)
		}, true
	case parquet.TypeDecimal:
		return func(s string, dst *batch.Vector, at int) error { return readDecimalText(s, dst, at, seed) }, true
	case parquet.TypeFloat64, parquet.TypeFloat32:
		typ := "double precision"
		if seed.Type == parquet.TypeFloat32 {
			typ = "real"
		}
		return func(s string, dst *batch.Vector, at int) error {
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return bad(typ, s)
			}
			if seed.Type == parquet.TypeFloat32 {
				dst.Float32Data[at] = float32(f)
			} else {
				dst.Float64Data[at] = f
			}
			return nil
		}, true
	case parquet.TypeBool:
		return func(s string, dst *batch.Vector, at int) error {
			v, ok := kernel.ParseBoolText(s)
			if !ok {
				return bad("boolean", s)
			}
			dst.BoolData[at] = v
			return nil
		}, true
	case parquet.TypeDate:
		return func(s string, dst *batch.Vector, at int) error {
			d, err := time.Parse("2006-01-02", strings.TrimSpace(s))
			if err != nil {
				return bad("date", s)
			}
			dst.Int32Data[at] = int32(d.Unix() / 86400)
			return nil
		}, true
	case parquet.TypeTimestamp:
		return func(s string, dst *batch.Vector, at int) error {
			s2 := strings.TrimSpace(s)
			for _, layout := range []string{"2006-01-02 15:04:05.999999", "2006-01-02T15:04:05.999999", "2006-01-02"} {
				if ts, err := time.Parse(layout, s2); err == nil {
					dst.Int64Data[at] = ts.UnixMilli()
					return nil
				}
			}
			return bad("timestamp", s)
		}, true
	}
	return nil, false
}
