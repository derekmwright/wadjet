package parquet

import "fmt"

// Record assembly for nested columns: ONE recursive descent over the FILE's
// own schema tree, driving every leaf under a column from that leaf's own
// definition and repetition levels.
//
// What it replaces (#409): three hand-written assemblers — one per container
// kind — that resolved their leaves by a FIXED-DEPTH path and gave up when
// the path did not land on a leaf. assembleRowColumn looked up
// {col, field} and skipped the field when the lookup missed, so a field that
// was itself a container (always a GROUP, never a leaf) was dropped from the
// struct. assembleMapColumn looked up {col, "key_value", value} and abandoned
// the WHOLE column on a miss, so a MAP of ARRAY or of ROW read back absent.
// assembleArrayColumn had a prefix fallback, which is worse than a miss: an
// ARRAY of MAP resolved to the MAP's FIRST leaf and answered with the array
// of KEYS. Even with the right leaf, assembleMapColumn read the value at the
// KEY leaf's entry index, an alignment that only holds while the value is a
// single leaf with one entry per map entry.
//
// The depth assumption is the whole defect, so the replacement carries no
// depth at all. Every shape — MAP inside ROW, ARRAY of MAP, MAP of ARRAY,
// MAP of MAP, ARRAY of ARRAY, to any depth — is the same three cases applied
// recursively.
//
// The plan is built from the SchemaNode tree rather than from the catalog's
// Column, because the file is what the levels describe: nodeToColumn
// (file_reader.go) derives the reported column TYPE from exactly the same
// three patterns, so the assembled value's shape and the declared type
// cannot drift apart. Level arithmetic is not recomputed here either —
// BuildSchemaTree's computeLevels already stamped MaxDefLevel/MaxRepLevel on
// every node from the footer's repetition types, and those are the numbers
// the page levels are written against.

type nestedKind int

const (
	kindLeaf nestedKind = iota
	kindStruct
	kindList
	kindMap
)

// nestedNode is one node of a column's assembly plan.
type nestedNode struct {
	kind nestedKind
	name string

	// leafIdx is the file's leaf-column index, for a leaf node only. The
	// VALUES those leaves decode to are produced by readLeafColumn, which is
	// where the leaf's own type is resolved; assembly is purely structural.
	leafIdx int

	// def is the definition level at which THIS node is present. A level
	// below it means the node is NULL.
	def int32
	// elemDef is the definition level at which a list's or map's REPEATED
	// group is present, i.e. the container has at least one entry. Exactly
	// def+1; a level equal to def is the EMPTY container, which is not the
	// same value as a NULL one.
	elemDef int32
	// rep is the repetition level a continuation entry of this container
	// carries. A following entry at this level is another element of THIS
	// container; anything lower closes it.
	rep int32

	children []*nestedNode

	// driver is the leaf whose level stream this node is read from: the
	// first leaf of the subtree, which every shape guarantees advances by at
	// least one entry per record. leaves is every leaf below the node, which
	// is what a NULL or empty container has to consume one placeholder entry
	// from.
	driver int
	leaves []int
}

// buildAssemblyPlan turns a top-level column's schema subtree into an
// assembly plan. It returns nil for a leaf column — those never need record
// assembly and go through readColumnToAny, which is also where the catalog's
// declared type is honoured.
func buildAssemblyPlan(n *SchemaNode) *nestedNode {
	if n == nil || n.IsLeaf() {
		return nil
	}
	return buildAssemblyNode(n)
}

func buildAssemblyNode(n *SchemaNode) *nestedNode {
	node := &nestedNode{name: n.Name, def: int32(n.MaxDefLevel)}

	switch {
	case n.IsLeaf() && n.IsRepeated():
		// A REPEATED leaf is the legacy two-level list encoding: the leaf
		// itself carries the repetition, with no wrapper group. Reading one
		// entry per record would leave the rest of the row's entries in the
		// stream and slide every later row's values onto the wrong row, so
		// it is assembled as the list it is. (nodeToColumn reports such a
		// column as a ROW of one field; the shapes disagree, which is loud,
		// where a desynchronised stream is silent.)
		elem := &nestedNode{
			kind: kindLeaf, name: n.Name,
			leafIdx: n.LeafIndex, def: int32(n.MaxDefLevel),
		}
		collectLeaves(elem)
		node.kind = kindList
		node.def = int32(n.MaxDefLevel) - 1
		node.elemDef = int32(n.MaxDefLevel)
		node.rep = int32(n.MaxRepLevel)
		node.children = []*nestedNode{elem}

	case n.IsLeaf():
		node.kind = kindLeaf
		node.leafIdx = n.LeafIndex

	case len(n.Children) == 1 && n.Children[0].IsRepeated() && len(n.Children[0].Children) == 1:
		// LIST: optional group X (LIST) { repeated group list { element } }
		rep := n.Children[0]
		node.kind = kindList
		node.elemDef = int32(rep.MaxDefLevel)
		node.rep = int32(rep.MaxRepLevel)
		node.children = []*nestedNode{buildAssemblyNode(rep.Children[0])}

	case len(n.Children) == 1 && n.Children[0].IsRepeated() && len(n.Children[0].Children) == 2:
		// MAP: optional group X (MAP) { repeated group key_value { key, value } }
		rep := n.Children[0]
		node.kind = kindMap
		node.elemDef = int32(rep.MaxDefLevel)
		node.rep = int32(rep.MaxRepLevel)
		node.children = []*nestedNode{
			buildAssemblyNode(rep.Children[0]),
			buildAssemblyNode(rep.Children[1]),
		}

	default:
		node.kind = kindStruct
		node.children = make([]*nestedNode, 0, len(n.Children))
		for _, c := range n.Children {
			node.children = append(node.children, buildAssemblyNode(c))
		}
	}

	collectLeaves(node)
	return node
}

// collectLeaves fills driver and leaves for node and, recursively, for every
// group below it.
func collectLeaves(node *nestedNode) {
	if node.kind == kindLeaf {
		node.driver = node.leafIdx
		node.leaves = []int{node.leafIdx}
		return
	}
	for _, c := range node.children {
		if c.leaves == nil {
			collectLeaves(c)
		}
		node.leaves = append(node.leaves, c.leaves...)
	}
	node.driver = -1
	if len(node.leaves) > 0 {
		node.driver = node.leaves[0]
	}
}

// leafCursor is one leaf's position in its own entry stream: pos indexes the
// def/rep levels (one entry per record slot), valPos indexes the decoded
// values (one per PRESENT entry only).
type leafCursor struct {
	pos    int
	valPos int
}

// recordAssembler holds the per-leaf cursors for one row group's read.
type recordAssembler struct {
	pages []leafColumnData
	cur   []leafCursor
}

func newRecordAssembler(pages []leafColumnData) *recordAssembler {
	return &recordAssembler{pages: pages, cur: make([]leafCursor, len(pages))}
}

// peekDef reports the definition level of the node's driving leaf at its
// current position, or -1 when that leaf's stream is exhausted.
func (a *recordAssembler) peekDef(node *nestedNode) int32 {
	if node.driver < 0 || node.driver >= len(a.pages) {
		return -1
	}
	c := a.cur[node.driver]
	lv := a.pages[node.driver].defLevels
	if c.pos >= len(lv) {
		return -1
	}
	return lv[c.pos]
}

// peekRep is peekDef for the repetition level. -1 on exhaustion, which is
// below every real level and therefore closes any open container.
func (a *recordAssembler) peekRep(node *nestedNode) int32 {
	if node.driver < 0 || node.driver >= len(a.pages) {
		return -1
	}
	c := a.cur[node.driver]
	lv := a.pages[node.driver].repLevels
	if c.pos >= len(lv) {
		return -1
	}
	return lv[c.pos]
}

// skipOne consumes the single placeholder entry a NULL or empty container
// leaves in every leaf beneath it.
func (a *recordAssembler) skipOne(node *nestedNode) {
	for _, li := range node.leaves {
		if li < 0 || li >= len(a.pages) {
			continue
		}
		lcd := &a.pages[li]
		c := &a.cur[li]
		if c.pos >= len(lcd.defLevels) {
			continue
		}
		if lcd.defLevels[c.pos] == lcd.maxDef {
			c.valPos++
		}
		c.pos++
	}
}

// read assembles exactly one record for node, advancing every cursor beneath
// it past that record. A nil result means the node is NULL at this record.
func (a *recordAssembler) read(node *nestedNode) any {
	switch node.kind {
	case kindLeaf:
		li := node.leafIdx
		if li < 0 || li >= len(a.pages) {
			return nil
		}
		lcd := &a.pages[li]
		c := &a.cur[li]
		if c.pos >= len(lcd.defLevels) {
			return nil
		}
		d := lcd.defLevels[c.pos]
		c.pos++
		if d != lcd.maxDef {
			return nil
		}
		var v any
		if c.valPos < len(lcd.values) {
			v = lcd.values[c.valPos]
		}
		c.valPos++
		return v

	case kindStruct:
		if d := a.peekDef(node); d < node.def {
			a.skipOne(node)
			return nil
		}
		m := make(map[string]any, len(node.children))
		for _, c := range node.children {
			// A NULL field is left OUT of the map rather than stored as a
			// nil entry: that is the convention every other row-shape in
			// this reader uses for absence (readRowsFlat omits a nil
			// column), and batch.FromRows reads a missing key as NULL.
			if v := a.read(c); v != nil {
				m[c.name] = v
			}
		}
		return m

	case kindList:
		d := a.peekDef(node)
		if d < node.def {
			a.skipOne(node)
			return nil
		}
		if d < node.elemDef {
			a.skipOne(node)
			return []any{}
		}
		arr := []any{}
		for {
			arr = append(arr, a.read(node.children[0]))
			if a.peekRep(node) != node.rep {
				return arr
			}
		}

	case kindMap:
		d := a.peekDef(node)
		if d < node.def {
			a.skipOne(node)
			return nil
		}
		if d < node.elemDef {
			a.skipOne(node)
			return map[string]any{}
		}
		m := map[string]any{}
		for {
			k := a.read(node.children[0])
			// A NULL VALUE is kept, unlike a null struct field: a map entry
			// whose value is NULL is a key that IS in the map.
			v := a.read(node.children[1])
			m[fmt.Sprint(k)] = v
			if a.peekRep(node) != node.rep {
				return m
			}
		}
	}
	return nil
}

// checkDrained reports a leaf whose level stream still holds entries after
// every record has been assembled.
//
// The count is an exact bound, not a policy one (ADR-0018 §1): assembly
// consumes one entry from a leaf per value the record holds at that leaf,
// and a NULL or empty container consumes exactly one placeholder from each
// leaf beneath it, so after the row group's numRows records EVERY leaf that
// was paged in must sit exactly at the end of its own stream. A residual
// means the levels and the row count describe different data — the file's
// own numbers contradicting each other, which is the one thing a reader can
// see without a second opinion.
//
// It is the assembler's only cross-check. The level walk itself cannot
// notice a wrong repetition level: it reads what the levels say, and levels
// that close a container early simply produce a shorter value. Where the
// mistake desynchronises SIBLING leaves — a map's key against its value, a
// struct's fields against each other — the leftover entries are what is left
// to see, and this is what sees them. Wadjet's own writer emitted exactly
// that shape for a multi-entry LIST or MAP nested inside another one, before
// #409 (see docs/adr/0018-parquet-file-numbers-are-input.md); such files are
// refused here rather than answered from.
func (a *recordAssembler) checkDrained(leaves []*SchemaNode) error {
	for i := range a.pages {
		lcd := &a.pages[i]
		left := len(lcd.defLevels) - a.cur[i].pos
		if left <= 0 {
			continue
		}
		path := any(i)
		if i < len(leaves) && leaves[i] != nil {
			path = leaves[i].Path
		}
		return fmt.Errorf("leaf %v: %d of %d level entries left over after assembling "+
			"the row group's records — the file's levels and its row count disagree",
			path, left, len(lcd.defLevels))
	}
	return nil
}

// assembleNestedColumn assembles one nested column across every row of the
// row group, writing each non-NULL value into rows[i][name].
func (a *recordAssembler) assembleNestedColumn(node *nestedNode, name string, rows []map[string]any) {
	for i := range rows {
		if v := a.read(node); v != nil {
			rows[i][name] = v
		}
	}
}
