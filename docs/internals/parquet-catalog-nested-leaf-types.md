# Parquet catalog nested leaf types

Source: internal/storage/parquet/nested_retype.go — func leafColumnsFromCatalog(fr *FileReader, catalog []Column) []Column {, moved 2026-09-11 (#1026)

leafColumnsFromCatalog is FileReader.LeafColumn for every leaf, with the
CATALOG's declared type substituted at each leaf the catalog's schema reaches
AND the substitution is admissible.

It is the catalog-side twin of the FILE-side overlay #589 built
(overlayDeclaredColumn / containerChildren, ADR-0018 §8), and it exists
because those two halves stopped agreeing about what a container can carry.

Nine of wadjet's types have no parquet annotation (IPv4, IPv6, MAC, UUID,
Bytes, Port, Protocol, Duration) or are spelled as plain UTF8 (CIDR), so a
file carries them only in the `wadjet.schema` footer blob. A file written
BEFORE that key existed (pre-v0.18.0, #396) has no blob, and the catalog is
the only place its types survive. `retypeFromCatalog` restores them — but by
construction it stops at the top level, so a nested IPv6 or UUID in such a
file still read back as "" long after the file-side overlay learned to
recurse (#608). The bytes are on disk and undamaged; only the name of their
type was lost.

Two rules, and the second is the difference from the top-level pass:

  - The WALK is driven by the file's node TREE, through the same
    containerChildren alignment collectLeafColumns and overlayDeclaredColumn
    use, so the catalog cannot reach deeper here than the file's own schema
    already does and a subtree the two disagree about in SHAPE keeps the
    file's answer.
  - A leaf pairing that is not ADMISSIBLE is declined rather than refused.
    The top-level pass makes drift an ERROR, because there the catalog names
    a column a user's query asked for by that name and answering from the
    file's type instead would be a different answer arrived at without
    saying so. Inside a container the file-side overlay already declines
    silently on every condition it cannot meet (overlayDeclaredLeaf), and
    making the same disagreement fatal on the catalog side would refuse
    files that read correctly today — a behaviour change wider than the
    repair this is for. The two halves now use ONE admissibility rule at the
    same depths, which is what #608 is about.

The result is seeded from the file's OWN declared columns, so a file WITH a
blob is unaffected: its leaves already carry their declared types and the
catalog agrees with them, leaf for leaf.
