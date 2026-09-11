# Parquet container box normalization

Source: internal/storage/parquet/container_box.go — func arrayElements(col Column, val any) ([]any, error) {, moved 2026-09-11 (#1026)

arrayElements and rowFields resolve a caller's box for a CONTAINER column,
or refuse it.

decomposeArray, decomposeRow and decomposeMap each asserted one Go shape and
treated the failure as an ABSENT SUBTREE — the value became NULL, with no
error from WriteRows, from Close or from the read. A nullable
ARRAY(INT64) handed []int64{1,2,3} read back as NULL (#889), and
ValidateNestedLeaves could not see it either: it walks only the shapes that
assert successfully, so a box it does not recognise is a box it says nothing
about.

Two changes, and the order matters. First, a slice or map of ANY element
type is normalised — []int64, []string, []float64, map[string]int and the
rest are unambiguous spellings of the same value, and refusing them would
make an obvious call an error where it used to be silently wrong. Second,
what is left — a scalar, a string, a struct — has no reading as a container
at all and is 42804 datatype_mismatch, LATCHED, so the write fails rather
than storing a NULL nobody asked for.

reflect rather than a list of element types: the list would be the thing
that goes out of date, and this runs once per container VALUE on the write
path, not per leaf.
