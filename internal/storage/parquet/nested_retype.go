package parquet

// nestedFileNode resolves a top-level column name to its node in the FILE's
// schema tree, under this package's one identity rule.
func nestedFileNode(root *SchemaNode, name string) *SchemaNode {
	if root == nil {
		return nil
	}
	for _, n := range root.Children {
		if n != nil && FoldName(n.Name) == FoldName(name) {
			return n
		}
	}
	return nil
}

// retypeNestedFromCatalog returns file — a CONTAINER column as the file
// describes it — with the CATALOG's type substituted at every leaf the
// catalog's matching subtree reaches and the substitution is admissible.
//
// It is the recursion retypeFromCatalog stopped short of, and it has to happen
// on the COLUMN TREE and not only per leaf index, because the retyped tree is
// what a caller carries onward: the DAG's scan source resolves its read schema
// through Reader.SchemaAs and then hands THAT to the reader, so a nested type
// the retype did not write into the returned Column is a type the DAG never
// sees. That is why a fix at the leaf array alone repaired the single-process
// arm and left the stage DAG reading `10.0.0.0` back as 167772160 — the two-path
// divergence #423's gate exists to catch, one level down (#608).
//
// The file's Column is DEEP-COPIED first: r.schema.Columns is shared with every
// other reader of the same file and a retype is per-CALLER.
func retypeNestedFromCatalog(file Column, want *Column, n *SchemaNode) Column {
	out := cloneColumn(file)
	retypeColumnTree(&out, want, n)
	return out
}

// retypeColumnTree walks one file subtree against one catalog subtree, aligned
// by the file's node tree, substituting an admissible catalog type at each leaf.
func retypeColumnTree(dst, want *Column, n *SchemaNode) {
	if dst == nil || want == nil || n == nil {
		return
	}
	if n.IsLeaf() {
		if nestedRetypeAdmissible(*dst, *want, n) {
			dst.Type = want.Type
		}
		return
	}
	dcols, nodes := containerChildren(dst, n)
	wcols, _ := containerChildren(want, n)
	if len(dcols) == 0 || len(dcols) != len(wcols) {
		// The two describe different shapes. Keep the file's answer for the
		// whole subtree, exactly as overlayDeclaredContainer does on the
		// file side.
		return
	}
	for i := range dcols {
		if dcols[i].Name != wcols[i].Name {
			return
		}
	}
	for i := range dcols {
		retypeColumnTree(dcols[i], wcols[i], nodes[i])
	}
}

// cloneColumn deep-copies a Column, so a retype of a container cannot reach
// back into the schema the FileReader hands every caller.
func cloneColumn(c Column) Column {
	out := c
	if c.ElementType != nil {
		e := cloneColumn(*c.ElementType)
		out.ElementType = &e
	}
	if len(c.Fields) > 0 {
		out.Fields = make([]Column, len(c.Fields))
		for i, f := range c.Fields {
			out.Fields[i] = cloneColumn(f)
		}
	}
	return out
}

// leafColumnsFromCatalog is FileReader.LeafColumn for every leaf, with the
// CATALOG's declared type substituted at each leaf the catalog's schema reaches
// AND the substitution is admissible.
//
// It is the catalog-side twin of the FILE-side overlay #589 built
// (overlayDeclaredColumn / containerChildren, ADR-0018 §8), and it exists
// because those two halves stopped agreeing about what a container can carry.
//
// Nine of wadjet's types have no parquet annotation (IPv4, IPv6, MAC, UUID,
// Bytes, Port, Protocol, Duration) or are spelled as plain UTF8 (CIDR), so a
// file carries them only in the `wadjet.schema` footer blob. A file written
// BEFORE that key existed (pre-v0.18.0, #396) has no blob, and the catalog is
// the only place its types survive. `retypeFromCatalog` restores them — but by
// construction it stops at the top level, so a nested IPv6 or UUID in such a
// file still read back as "" long after the file-side overlay learned to
// recurse (#608). The bytes are on disk and undamaged; only the name of their
// type was lost.
//
// Two rules, and the second is the difference from the top-level pass:
//
//   - The WALK is driven by the file's node TREE, through the same
//     containerChildren alignment collectLeafColumns and overlayDeclaredColumn
//     use, so the catalog cannot reach deeper here than the file's own schema
//     already does and a subtree the two disagree about in SHAPE keeps the
//     file's answer.
//   - A leaf pairing that is not ADMISSIBLE is declined rather than refused.
//     The top-level pass makes drift an ERROR, because there the catalog names
//     a column a user's query asked for by that name and answering from the
//     file's type instead would be a different answer arrived at without
//     saying so. Inside a container the file-side overlay already declines
//     silently on every condition it cannot meet (overlayDeclaredLeaf), and
//     making the same disagreement fatal on the catalog side would refuse
//     files that read correctly today — a behaviour change wider than the
//     repair this is for. The two halves now use ONE admissibility rule at the
//     same depths, which is what #608 is about.
//
// The result is seeded from the file's OWN declared columns, so a file WITH a
// blob is unaffected: its leaves already carry their declared types and the
// catalog agrees with them, leaf for leaf.
func leafColumnsFromCatalog(fr *FileReader, catalog []Column) []Column {
	leaves := fr.Leaves()
	out := make([]Column, len(leaves))
	for i := range leaves {
		out[i] = fr.LeafColumn(i)
	}
	if len(catalog) == 0 {
		return out
	}
	root := fr.SchemaRoot()
	if root == nil {
		return out
	}
	byName := make(map[string]*SchemaNode, len(root.Children))
	for _, n := range root.Children {
		if n != nil {
			byName[FoldName(n.Name)] = n
		}
	}
	for i := range catalog {
		c := &catalog[i]
		if !isNestedType(c.Type) {
			// The flat columns are retypeFromCatalog's, vetted and reported
			// there. Doing them again here would apply a WEAKER rule (this one
			// declines where that one refuses) to the same pairing.
			continue
		}
		if n := byName[FoldName(c.Name)]; n != nil {
			retypeLeafFromCatalog(c, n, out)
		}
	}
	return out
}

// retypeLeafFromCatalog walks one catalog column against one file subtree,
// substituting an admissible catalog type at each leaf it reaches.
func retypeLeafFromCatalog(c *Column, n *SchemaNode, out []Column) {
	if c == nil || n == nil {
		return
	}
	if n.IsLeaf() {
		if n.LeafIndex < 0 || n.LeafIndex >= len(out) {
			return
		}
		if nestedRetypeAdmissible(out[n.LeafIndex], *c, n) {
			// Type identity ONLY, exactly as overlayDeclaredLeaf copies only
			// the type: none of the nine has a precision, scale or dimension
			// to carry, and copying those fields is how a declaration reaches
			// the decode and allocation paths.
			out[n.LeafIndex].Type = c.Type
		}
		return
	}
	cols, nodes := containerChildren(c, n)
	for i := range cols {
		retypeLeafFromCatalog(cols[i], nodes[i], out)
	}
}

// nestedRetypeAdmissible decides whether the catalog's type may replace the one
// the file recovered for a leaf INSIDE a container.
//
// It asks retypeFromCatalog's question — can the values the file's own type
// decodes to be STORED as the catalog's without converting them — through the
// same two predicates, so a pairing admitted here is one admitted there. The
// width check is the same too, and it matters for the same reason: a UUID is
// sixteen bytes by definition and an IPv6 address is too, so a leaf of another
// width is not a truncated value, it is a different one.
//
// Note what it does NOT admit, and why the file's own annotation wins: if the
// file ANNOTATED this leaf then it is not a lost type, it is catalog/file
// drift, and honouring the catalog would decode the file's bytes as something
// they are not. That is the same rule overlayDeclaredLeaf applies on the
// other side.
func nestedRetypeAdmissible(have, want Column, n *SchemaNode) bool {
	if have.Type == want.Type {
		return false // nothing to substitute
	}
	if isNestedType(have.Type) || isNestedType(want.Type) {
		return false
	}
	if n.Type == nil {
		return false
	}
	// The file said what this leaf is. A CIDR is the one exception, for the
	// reason overlayDeclaredLeaf names: its storage and rendering are the same
	// UTF8 text a STRING leaf carries, so the annotation cannot distinguish
	// them and the declaration is the only thing that can.
	if n.LogicalType != nil || n.ConvertedType != nil {
		if !(leafIsUTF8String(n) && declaredOverlayUTF8Types[want.Type]) {
			return false
		}
	} else if !declaredOverlayTypes[want.Type] {
		return false
	}
	if !DecodeCompatible(have.Type, want.Type) && !CoercibleTo(have.Type, want.Type) {
		return false
	}
	if wadjetTypeToPhysical(want.Type) != *n.Type {
		return false
	}
	if w := fixedByteWidth(want); w != 0 && *n.Type == PhysicalFixedLenByteArray &&
		int(n.TypeLength) != w {
		return false
	}
	return true
}
