// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ArrayValueLiteral spells an ARRAY value as the typed literal the parser
// compiles back to the same array (arc CW round 4 for a DAG scalar subquery;
// round 5 for the correlated re-run's outer literal, outerLiteral). The value
// used to be substituted as Go's text of the box (`a.v = [2024-01-10]`), which
// no stage can parse, or — through the coordinator's raw read — as `null`, so
// every comparison with a subquery that returns an array refused or answered
// no row on the DAG while the single-process path answered (the round-3
// review's N5). A one-dimensional value is its PostgreSQL text cast to the
// element's array type, `CAST('{2024-01-10}' AS DATE[])`; a multi-dimensional
// one is the constructor of its inner arrays, each leaf a typed literal. ok is
// false for any other value and for a leaf with no castable name here (a ROW,
// a MAP): those keep a loud refusal.
func ArrayValueLiteral(v any, col parquet.Column) (plansql.Node, bool) {
	if col.Type != parquet.TypeArray || col.ElementType == nil {
		return nil, false
	}
	if _, ok := v.([]any); !ok {
		return nil, false
	}
	if col.ElementType.Type != parquet.TypeArray {
		el, ok := arrayElementCastName(col.ElementType)
		if !ok {
			return nil, false
		}
		return &plansql.CastNode{
			Inner:    &plansql.Lit{Value: batch.FormatPGText(v, &col), Kind: plansql.LitString},
			TypeName: el + "[]",
		}, true
	}
	return arrayConstructorLiteral(v, &col)
}

// arrayConstructorLiteral is a multi-dimensional value as ARRAY[...] of its
// inner arrays, down to typed leaves: `ARRAY[ARRAY[CAST('1' AS BIGINT), …]]`.
func arrayConstructorLiteral(v any, col *parquet.Column) (plansql.Node, bool) {
	if col.Type != parquet.TypeArray {
		name, ok := arrayElementCastName(col)
		if !ok {
			return nil, false
		}
		if v == nil {
			return &plansql.CastNode{Inner: &plansql.Lit{Kind: plansql.LitNull}, TypeName: name}, true
		}
		leaf := *col
		return &plansql.CastNode{
			Inner:    &plansql.Lit{Value: batch.FormatPGText(v, &leaf), Kind: plansql.LitString},
			TypeName: name,
		}, true
	}
	if col.ElementType == nil {
		return nil, false
	}
	typeName := func() (string, bool) {
		leaf, dims := col, 0
		for leaf.Type == parquet.TypeArray && leaf.ElementType != nil {
			leaf, dims = leaf.ElementType, dims+1
		}
		name, ok := arrayElementCastName(leaf)
		return name + strings.Repeat("[]", dims), ok
	}
	elems, _ := v.([]any)
	if v == nil || len(elems) == 0 {
		name, ok := typeName()
		if !ok {
			return nil, false
		}
		var inner plansql.Node = &plansql.Lit{Kind: plansql.LitNull}
		if v != nil {
			inner = &plansql.ArrayLitNode{}
		}
		return &plansql.CastNode{Inner: inner, TypeName: name}, true
	}
	out := &plansql.ArrayLitNode{Elements: make([]plansql.Node, len(elems))}
	for i, e := range elems {
		n, ok := arrayConstructorLiteral(e, col.ElementType)
		if !ok {
			return nil, false
		}
		out.Elements[i] = n
	}
	return out, true
}

// arrayElementCastName is the cast spelling of an array ELEMENT's declared
// type, for the literal ArrayValueLiteral writes: false for an element this
// cannot spell exactly (a container, an address type the cast would widen).
func arrayElementCastName(el *parquet.Column) (string, bool) {
	switch el.Type {
	case parquet.TypeBool:
		return "BOOLEAN", true
	case parquet.TypeInt32:
		return "INT", true
	case parquet.TypeInt64:
		return "BIGINT", true
	case parquet.TypeFloat32:
		return "REAL", true
	case parquet.TypeFloat64:
		return "DOUBLE", true
	case parquet.TypeString:
		return "TEXT", true
	case parquet.TypeDate:
		return "DATE", true
	case parquet.TypeTimestamp:
		return "TIMESTAMP", true
	case parquet.TypeUUID:
		return "UUID", true
	case parquet.TypeIPv4:
		return "IPV4", true
	case parquet.TypeDecimal:
		p := el.Precision
		if p <= 0 {
			p = batch.MaxDecimalPrecision
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", p, el.Scale), true
	}
	return "", false
}
