package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Equi-join keys must encode one common comparison type (#615, #650, #663;
// ADR-0023). Use operator resolution, not setOpWiden/select_common_type:
// int4/int8 → int8; integer/numeric → numeric; any unequal numeric pair
// involving float4/float8 → float8. float4 is not an intermediate rung.
// Numeric/numeric stays exact across scales: batch.AppendDecimalKey normalizes
// scale, including integer scale 0 (#474); no (p,s) or column-range overflow.
// See docs/internals/equi-join-key-common-types.md for the design.

// joinKeyCommonType is the ladder above for one pair of DECLARED types.
//
// ok=false means "leave this pair alone", which is the answer for every pair
// that already agrees and for every pair the ladder does not describe — a
// STRING key, a DATE against a TIMESTAMP, an IPv4 against a BIGINT. Those
// keep exactly the encoding they had; widening them is a different question
// with a different authority, and guessing here would move rows under a rule
// nobody stated.
func joinKeyCommonType(a, b parquet.TypeID) (parquet.TypeID, bool) {
	if a == b || !joinKeyNumeric(a) || !joinKeyNumeric(b) {
		return 0, false
	}
	switch {
	case a == parquet.TypeFloat64 || b == parquet.TypeFloat64,
		a == parquet.TypeFloat32 || b == parquet.TypeFloat32:
		// float8 beats everything; float4 meeting anything that is not
		// another float4 (caught by a == b above) is also float8.
		return parquet.TypeFloat64, true
	case a == parquet.TypeDecimal || b == parquet.TypeDecimal:
		// numeric ⊕ integer. Two DECIMALs are a == b as far as the KEY is
		// concerned — the key normalizes scale — so only the integer arm
		// reaches here.
		return parquet.TypeDecimal, true
	default:
		// int4 ⊕ int8.
		return parquet.TypeInt64, true
	}
}

func joinKeyNumeric(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32,
		parquet.TypeFloat64, parquet.TypeDecimal:
		return true
	}
	return false
}

// resolveJoinKeyTypes returns one entry per key PAIR: the type both sides'
// key bytes must be built at, or exec.KeyTypeUnresolved where no widening
// applies. A nil result means "no pair needs widening", which is every
// same-type join and the whole of TPC-H — the caller then sets nothing and
// the operator takes the path it took before, byte for byte.
//
// leftKeys name columns of node.Children[0] and rightKeys of
// node.Children[1]; the caller has already run assignJoinKeySides, so the
// sides are final. A key either side cannot be typed is unresolved: declining
// leaves the pre-existing behaviour, and the pre-existing behaviour is
// correct for every pair whose two sides agree.
func resolveJoinKeyTypes(node *logical.Node, leftKeys, rightKeys []string) []parquet.TypeID {
	if node == nil || len(node.Children) < 2 ||
		len(leftKeys) == 0 || len(leftKeys) != len(rightKeys) {
		return nil
	}
	left := joinSideColTypes(node.Children[0])
	right := joinSideColTypes(node.Children[1])
	if left == nil || right == nil {
		return nil
	}
	out := make([]parquet.TypeID, len(leftKeys))
	any := false
	for i := range leftKeys {
		out[i] = exec.KeyTypeUnresolved
		lt, lok := left[joinKeyLookupName(leftKeys[i])]
		rt, rok := right[joinKeyLookupName(rightKeys[i])]
		if !lok || !rok {
			continue
		}
		if common, ok := joinKeyCommonType(lt, rt); ok {
			out[i], any = common, true
		}
	}
	if !any {
		return nil
	}
	return out
}

// joinKeyLookupName strips a qualifier and lower-cases, the same reading
// declaredJoinSchema gives a wanted column: "a.w_i32" and "w_i32" name one
// column of one side.
func joinKeyLookupName(key string) string {
	k := strings.ToLower(strings.TrimSpace(key))
	if dot := strings.LastIndexByte(k, '.'); dot >= 0 {
		k = k[dot+1:]
	}
	return k
}

// joinSideColTypes merges shared declared types under both emitted names and
// source names visible below renames (#615). Use emittedColTypes for
// aggregate/window/projection/DISTINCT, and setOpDeclaredOutputSchema with
// setOpWiden for set operations. Computed projections bind only their alias;
// their inputs retain their own types under source names. Delete conflicting
// names rather than choosing: exec.joinKeyEncodingMismatch remains the
// runtime backstop. See docs/internals/join-side-declared-types.md for the design.
func joinSideColTypes(n *logical.Node) map[string]parquet.TypeID {
	if n == nil {
		return nil
	}
	out := make(map[string]parquet.TypeID)
	conflict := make(map[string]bool)
	put := func(name string, t parquet.TypeID, overwrite bool) {
		lc := strings.ToLower(strings.TrimSpace(name))
		if lc == "" || conflict[lc] {
			return
		}
		prev, dup := out[lc]
		if !dup {
			out[lc] = t
			return
		}
		if prev == t {
			return
		}
		if !overwrite {
			// The emitted answer already bound this name; a source name
			// below it does not get to move it.
			return
		}
		delete(out, lc)
		conflict[lc] = true
	}
	for name, t := range joinSideEmittedTypes(n) {
		put(name, t, true)
	}
	for name, t := range joinSideSourceTypes(n) {
		put(name, t, false)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// joinSideEmittedTypes is question 1 above: what the side's OUTPUT columns
// are called and what they carry.
//
// The Project arm is spelled out rather than delegated because
// emittedColTypes' own Project arm asks emittedColTypes for its child, and
// that one answers nil for a set operation — so `SELECT k FROM (A UNION ALL
// B)` would type every projection from an empty map. Recursing through THIS
// function instead closes that hole; everything else defers.
func joinSideEmittedTypes(n *logical.Node) map[string]parquet.TypeID {
	if n == nil {
		return nil
	}
	if cols, ok := setOpDeclaredOutputSchema(n); ok {
		out := make(map[string]parquet.TypeID, len(cols))
		for _, c := range cols {
			out[strings.ToLower(c.Name)] = c.Type
		}
		return out
	}
	if n.Type == logical.NodeProject && len(n.Children) == 1 {
		in := joinSideEmittedTypes(n.Children[0])
		if in == nil {
			return emittedColTypes(n)
		}
		decls := colDecls{types: in, dec: emittedColDecimal(n.Children[0])}
		strictInt := strictIntArithCols(n.Children[0])
		out := make(map[string]parquet.TypeID, len(n.Projections))
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			out[strings.ToLower(name)] = declaredProjectionType(proj, decls, strictInt)
		}
		return out
	}
	return emittedColTypes(n)
}

// joinSideSourceTypes is question 2: the catalog types of the scan columns
// beneath this side, under their own names. A name two scans carry at two
// types is dropped, exactly as inputColTypes drops one two join sides
// disagree about.
func joinSideSourceTypes(n *logical.Node) map[string]parquet.TypeID {
	out := make(map[string]parquet.TypeID)
	conflict := make(map[string]bool)
	var walk func(*logical.Node)
	walk = func(cur *logical.Node) {
		if cur == nil {
			return
		}
		if cur.Type == logical.NodeScan {
			for _, name := range cur.ScanColumns {
				lc := strings.ToLower(name)
				t, ok := cur.ScanColTypes[lc]
				if !ok || conflict[lc] {
					continue
				}
				if prev, dup := out[lc]; dup && prev != t {
					delete(out, lc)
					conflict[lc] = true
					continue
				}
				out[lc] = t
			}
			return
		}
		if cur.Type == logical.NodeJoin && len(cur.Children) == 2 {
			walk(cur.Children[0])
			// A semi/anti join exposes only its probe side, exactly as
			// declaredJoinSchema reads it.
			if jt := strings.ToLower(cur.JoinType); jt == "semi" || jt == "anti" {
				return
			}
			walk(cur.Children[1])
			return
		}
		for _, child := range cur.Children {
			walk(child)
		}
	}
	walk(n)
	if len(out) == 0 {
		return nil
	}
	return out
}
