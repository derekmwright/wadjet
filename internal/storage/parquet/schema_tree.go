package parquet

// SchemaNode is a node in the reconstructed Parquet schema tree.
// Built from the flat SchemaElement list in the file footer.
// Group nodes have children; leaf nodes have a physical type.
type SchemaNode struct {
	Name           string
	Type           *PhysicalType       // nil for group nodes
	TypeLength     int32               // for FIXED_LEN_BYTE_ARRAY
	RepetitionType FieldRepetitionType
	LogicalType    *LogicalType
	ConvertedType  *ConvertedType
	Precision      int32               // for DECIMAL
	Scale          int32               // for DECIMAL
	Children       []*SchemaNode
	Parent         *SchemaNode
	LeafIndex      int             // index among leaf columns (-1 for groups)
	Path           []string        // full path from root
	MaxDefLevel    int             // computed max definition level
	MaxRepLevel    int             // computed max repetition level
}

// IsLeaf returns true if this node is a leaf (primitive type).
func (n *SchemaNode) IsLeaf() bool { return n.Type != nil }

// IsOptional returns true if the field is OPTIONAL.
func (n *SchemaNode) IsOptional() bool { return n.RepetitionType == FieldOptional }

// IsRepeated returns true if the field is REPEATED.
func (n *SchemaNode) IsRepeated() bool { return n.RepetitionType == FieldRepeated }

// BuildSchemaTree reconstructs the schema tree from the flat SchemaElement list
// in the Parquet footer. Returns the root node and a slice of leaf nodes
// indexed by their leaf column position.
func BuildSchemaTree(elements []SchemaElement) (*SchemaNode, []*SchemaNode) {
	if len(elements) == 0 {
		return nil, nil
	}

	root := &SchemaNode{
		Name:           elements[0].Name,
		RepetitionType: elements[0].RepetitionType,
		LeafIndex:      -1,
	}

	var leaves []*SchemaNode
	leafIdx := 0

	// Stack-based tree reconstruction: the flat list is a pre-order
	// traversal where each group node declares its child count.
	type stackEntry struct {
		node      *SchemaNode
		remaining int32
	}
	stack := []stackEntry{{node: root, remaining: elements[0].NumChildren}}

	for i := 1; i < len(elements); i++ {
		elem := elements[i]

		// Pop completed parents.
		for len(stack) > 0 && stack[len(stack)-1].remaining == 0 {
			stack = stack[:len(stack)-1]
		}
		if len(stack) == 0 {
			break
		}

		parent := stack[len(stack)-1].node

		node := &SchemaNode{
			Name:           elem.Name,
			Type:           elem.Type,
			TypeLength:     elem.TypeLength,
			RepetitionType: elem.RepetitionType,
			LogicalType:    elem.LogicalType,
			ConvertedType:  elem.ConvertedType,
			Precision:      elem.Precision,
			Scale:          elem.Scale,
			Parent:         parent,
			LeafIndex:      -1,
		}

		// Build path.
		if parent == root {
			node.Path = []string{elem.Name}
		} else {
			node.Path = make([]string, len(parent.Path)+1)
			copy(node.Path, parent.Path)
			node.Path[len(parent.Path)] = elem.Name
		}

		parent.Children = append(parent.Children, node)
		stack[len(stack)-1].remaining--

		if elem.NumChildren > 0 {
			stack = append(stack, stackEntry{node: node, remaining: elem.NumChildren})
		} else {
			node.LeafIndex = leafIdx
			leafIdx++
			leaves = append(leaves, node)
		}
	}

	// Compute definition and repetition levels.
	computeLevels(root, 0, 0)

	return root, leaves
}

func computeLevels(node *SchemaNode, defLevel, repLevel int) {
	if node.RepetitionType == FieldOptional {
		defLevel++
	} else if node.RepetitionType == FieldRepeated {
		defLevel++
		repLevel++
	}
	node.MaxDefLevel = defLevel
	node.MaxRepLevel = repLevel

	for _, child := range node.Children {
		computeLevels(child, defLevel, repLevel)
	}
}

// TopLevelLeafIndex maps a TOP-LEVEL column's name to its leaf index.
//
// Node-first, by schema-tree position: a struct FIELD may carry the same
// name as a top-level column, so `x INT64, r ROW{x INT64}` has two leaves
// named "x", and a plain map[leafName]index keeps whichever came last. The
// name of a top-level column means the leaf whose PATH is that one name —
// the read paths used to answer ReadRows(["x"]) and ReadRowGroup with the
// STRUCT FIELD's values, silently, while ReadRows(nil) answered with the
// column's own.
//
// A name that is not a top-level leaf still resolves to a deeper leaf of
// that name, which is what a caller naming a nested leaf directly relies on;
// the top-level one only ever WINS a collision.
func TopLevelLeafIndex(leaves []*SchemaNode) map[string]int {
	byName := make(map[string]int, len(leaves))
	for i, l := range leaves {
		if l == nil {
			continue
		}
		if j, dup := byName[l.Name]; dup && len(leaves[j].Path) == 1 {
			continue // a top-level leaf already claims this name
		}
		byName[l.Name] = i
	}
	return byName
}

// FindLeafByName finds a leaf node by column name (last path component).
func FindLeafByName(root *SchemaNode, name string) *SchemaNode {
	if root == nil {
		return nil
	}
	var found *SchemaNode
	walkSchemaTree(root, func(n *SchemaNode) bool {
		if n.IsLeaf() && n.Name == name {
			found = n
			return false
		}
		return true
	})
	return found
}

// FindLeafByIndex finds a leaf node by its sequential leaf index.
func FindLeafByIndex(root *SchemaNode, idx int) *SchemaNode {
	if root == nil {
		return nil
	}
	var found *SchemaNode
	walkSchemaTree(root, func(n *SchemaNode) bool {
		if n.LeafIndex == idx {
			found = n
			return false
		}
		return true
	})
	return found
}

// SchemaLeafCount returns the total number of leaf columns.
func SchemaLeafCount(root *SchemaNode) int {
	count := 0
	walkSchemaTree(root, func(n *SchemaNode) bool {
		if n.IsLeaf() {
			count++
		}
		return true
	})
	return count
}

func walkSchemaTree(node *SchemaNode, fn func(*SchemaNode) bool) bool {
	if !fn(node) {
		return false
	}
	for _, child := range node.Children {
		if !walkSchemaTree(child, fn) {
			return false
		}
	}
	return true
}
