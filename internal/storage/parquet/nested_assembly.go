package parquet

import "fmt"

// Assemble nested records by recursive descent over the FILE's schema tree,
// using each leaf's own definition/repetition levels (#409).
// ROW/LIST/MAP cases recurse to any depth; never resolve leaves by fixed-depth
// paths, take a prefix leaf, or index a map value by its key leaf's position.
// Match nodeToColumn's shape patterns and use BuildSchemaTree/computeLevels'
// stamped levels; do not substitute catalog shape or recompute level arithmetic.
// See docs/internals/parquet-recursive-record-assembly.md for the design.

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
			// EVERY declared field gets a key, a NULL one holding nil. The
			// convention is set by the other box for the same value: the
			// columnar reader's ROW vector answers GetValue with one entry
			// per child, nulls included, so omitting the key here made
			// ROW(a=>NULL, b=>-3) read back as map[b:-3] on this path and
			// map[a:<nil> b:-3] on that one — a live two-path value
			// divergence for any present ROW with a null field (#449).
			//
			// It is only the TOP level of a row where absence is spelled by
			// a missing key (readRowsFlat omits a nil column, and the
			// columnar box agrees — a batch's null column contributes no
			// entry). Inside a ROW the field set is fixed by the schema, so
			// the key is always there and nil is the value.
			//
			// Round-tripping is unaffected: decomposeRow reads m[field.Name]
			// and batch.FromRows drives (*Vector).SetValue by field name, so
			// an explicit nil and a missing key write the same NULL.
			m[c.name] = a.read(c)
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
		keyNode := node.children[0]
		m := map[string]any{}
		for {
			k := a.read(keyNode)
			// A NULL VALUE is kept, unlike a null struct field: a map entry
			// whose value is NULL is a key that IS in the map.
			v := a.read(node.children[1])
			m[a.mapKeyString(keyNode, k)] = v
			if a.peekRep(node) != node.rep {
				return m
			}
		}
	}
	return nil
}

// mapKeyString renders a decoded map-KEY leaf value into the canonical,
// parseable text a map key crosses the boundary as, so re-ingesting the
// assembled map (batch.mapKeyValue -> SetValue) reconstructs the value that
// was stored. The whole conversion is MapKeyCarrierText, which every family's
// carrier goes through; see its doc for why fmt.Sprint of a carrier corrupts a
// DECIMAL/DATE/IPv4/MAC/IPv6/UUID key (#883). A non-leaf key (the format's map
// keys are always primitive leaves) falls back to fmt.Sprint.
func (a *recordAssembler) mapKeyString(keyNode *nestedNode, k any) string {
	if keyNode.kind == kindLeaf && keyNode.leafIdx >= 0 && keyNode.leafIdx < len(a.pages) {
		lcd := &a.pages[keyNode.leafIdx]
		return MapKeyCarrierText(lcd.typeID, lcd.decScale, k)
	}
	return fmt.Sprint(k)
}

// checkDrained rejects residual leaf entries after numRows records assemble
// (ADR-0018 §1). Every value consumes one entry; NULL/empty containers consume
// one placeholder in each descendant leaf, so every paged stream must drain.
// The level walk alone cannot detect an early container close; desynchronized
// siblings expose it as leftovers. This is a cross-check, not a second oracle.
// Reject pre-#409 malformed nested files rather than answer from them
// (docs/adr/0018-parquet-file-numbers-are-input.md).
// See docs/internals/parquet-nested-assembly-drain-check.md for the design.
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
