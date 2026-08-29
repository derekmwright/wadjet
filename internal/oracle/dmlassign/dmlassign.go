// Package dmlassign is the SET-value matrix for DML assignment casts, and the
// PostgreSQL answers it is checked against.
//
// Why it exists. The DML doors are two copies of one executor — the embedded
// API's and the HTTP server's — and they have drifted before (#647 fixed a
// DECIMAL declaration read at one door and dropped at two others). A matrix
// written once and run by every door is the only arrangement in which "the
// doors agree" is a fact rather than an assumption.
//
// What it covers: five value CLASSES (integer literal, fractional literal,
// integer column, DECIMAL column, expression) against four target TYPES
// (INT64, DECIMAL(9,2), FLOAT64, STRING), plus the refusals at each boundary.
//
// Every Want, MergeWant and State was read off postgres:17-alpine against the
// fixture Setup describes — not remembered, and not derived from what wadjet
// happened to answer. The matrix exists because #678's first round got one of
// these cells wrong in a way every gate in the tree passed: an integer box
// assigned to a DECIMAL column was read as the already-unscaled CARRIER
// (ADR-0018 §4) rather than as a VALUE, so `SET d = n` with n = 10 stored 0.10
// and the statement returned success.
package dmlassign

// Case is one cell: a SET clause body, the column to read back, and what
// PostgreSQL stores (or the SQLSTATE it raises).
type Case struct {
	Name string

	// Set is the clause body for an UPDATE. Merge is its spelling for a MERGE,
	// where the interesting values live on the source; empty Merge reuses Set.
	Set   string
	Merge string

	// Col is the column to read back afterwards.
	Col string

	// Want is the stored value as the engine renders it; MergeWant overrides
	// it on the MERGE arm, where the source's values differ. Both are empty
	// when State is set.
	Want      string
	MergeWant string

	// State is the SQLSTATE PostgreSQL raises, for the cells that must be
	// REFUSED. A cell with a State must leave the row unchanged.
	// MergeState overrides it on the MERGE arm, where a spelling legal over
	// one relation can be ambiguous over two.
	State      string
	MergeState string
}

// Target is the fixture the expectations were read against:
//
//	mv  (id 1, n 10, d 1.50, f 2.5, s 'ab')   -- the UPDATE / MERGE target
//	mvs (id 1, k 7, dk 3.25, fk 0.5, sk 'zz') -- the MERGE source
//
// The source's VALUE column names (k, dk, fk, sk) are deliberately DISJOINT
// from the target's, so an unqualified reference in a MERGE resolves to exactly
// one relation and every cell tests what its name says it does.
//
// `id` is the deliberate exception: BOTH relations spell it, because a matrix
// in which no name is shared cannot see the one answer that is worse than a
// wrong error — silently picking a side. `SET n = id` is 1 over one relation
// and 42702 over two, and that pair is a cell (protocol item 2: a fixture whose
// values cannot distinguish the two rules passes for the wrong reason).
const (
	TargetDDL = "CREATE TABLE mv (id INT64, n INT64, d DECIMAL(9,2), f FLOAT64, s STRING)"
	SourceDDL = "CREATE TABLE mvs (id INT64, k INT64, dk DECIMAL(9,2), fk FLOAT64, sk STRING)"
	TargetRow = "INSERT INTO mv VALUES (1, 10, 1.50, 2.5, 'ab')"
	SourceRow = "INSERT INTO mvs VALUES (1, 7, 3.25, 0.5, 'zz')"
)

// Matrix returns every cell.
func Matrix() []Case {
	return []Case{
		// --- target INT64 -------------------------------------------------
		{Name: "int literal into INT64", Set: "n = 5", Col: "n", Want: "5"},
		{Name: "fractional literal into INT64 rounds", Set: "n = 2.4", Col: "n", Want: "2"},
		{Name: "fractional literal at the half rounds away from zero",
			Set: "n = 2.5", Col: "n", Want: "3"},
		{Name: "negative half rounds away from zero", Set: "n = 0 - 2.5", Col: "n", Want: "-3"},
		{Name: "integer column into INT64", Set: "n = id", Merge: "n = s.k", Col: "n",
			Want: "1", MergeWant: "7"},
		{Name: "DECIMAL column into INT64 rounds", Set: "n = d", Merge: "n = s.dk", Col: "n",
			Want: "2", MergeWant: "3"},
		{Name: "expression into INT64", Set: "n = 1 + 1", Col: "n", Want: "2"},
		// The same spelling over one relation and over two. `id` is the one
		// name the target and the source share, so the UPDATE arm answers and
		// the MERGE arm must refuse rather than quietly take a side.
		{Name: "unqualified name both relations spell", Set: "n = id", Col: "n",
			Want: "1", MergeState: "42702"},
		{Name: "the same name qualified by the target", Set: "n = id", Merge: "n = t.id",
			Col: "n", Want: "1"},
		{Name: "the same name qualified by the source", Set: "n = id", Merge: "n = s.id",
			Col: "n", Want: "1"},
		{Name: "an ambiguous name inside a function", Set: "n = ABS(id)", Col: "n",
			Want: "1", MergeState: "42702"},
		{Name: "expression over the column into INT64", Set: "n = n + 1", Merge: "n = t.n + 1",
			Col: "n", Want: "11"},
		{Name: "text naming no number into INT64", Set: "n = 'abc'", Col: "n", State: "22P02"},

		// --- target DECIMAL(9,2) ------------------------------------------
		// The R1 regression lived in this block. An INTEGER box out of the
		// expression engine reached DecimalValueFromBox, which reads an
		// integer as the already-UNSCALED carrier (ADR-0018 §4), so 10 stored
		// as 0.10 and 1 + 1 stored as 0.02 — both returning success.
		{Name: "int literal into DECIMAL", Set: "d = 5", Col: "d", Want: "5.00"},
		{Name: "fractional literal into DECIMAL rounds to scale",
			Set: "d = 2.567", Col: "d", Want: "2.57"},
		{Name: "integer column into DECIMAL is a VALUE, not a carrier",
			Set: "d = n", Merge: "d = s.k", Col: "d", Want: "10.00", MergeWant: "7.00"},
		{Name: "DECIMAL column into DECIMAL", Set: "d = d", Merge: "d = s.dk", Col: "d",
			Want: "1.50", MergeWant: "3.25"},
		{Name: "integer expression into DECIMAL is a VALUE, not a carrier",
			Set: "d = 1 + 1", Col: "d", Want: "2.00"},
		{Name: "real expression into DECIMAL", Set: "d = d * 2", Merge: "d = t.d * 2",
			Col: "d", Want: "3.00"},
		{Name: "expression mixing the column and an integer",
			Set: "d = n + 1", Merge: "d = t.n + 1", Col: "d", Want: "11.00"},
		{Name: "past the declared precision", Set: "d = 9999999.99 + 1", Col: "d", State: "22003"},
		{Name: "text naming no number into DECIMAL", Set: "d = 'abc'", Col: "d", State: "22P02"},

		// --- target FLOAT64 -----------------------------------------------
		{Name: "int literal into FLOAT64", Set: "f = 5", Col: "f", Want: "5"},
		{Name: "integer column into FLOAT64", Set: "f = n", Merge: "f = s.k", Col: "f",
			Want: "10", MergeWant: "7"},
		{Name: "DECIMAL column into FLOAT64", Set: "f = d", Merge: "f = s.dk", Col: "f",
			Want: "1.5", MergeWant: "3.25"},
		{Name: "expression into FLOAT64", Set: "f = 1 + 1", Col: "f", Want: "2"},

		// --- target STRING ------------------------------------------------
		{Name: "string literal into STRING", Set: "s = 'lit'", Col: "s", Want: "lit"},
		{Name: "integer column into STRING", Set: "s = n", Merge: "s = s.k", Col: "s",
			Want: "10", MergeWant: "7"},
		{Name: "DECIMAL column into STRING", Set: "s = d", Merge: "s = s.dk", Col: "s",
			Want: "1.50", MergeWant: "3.25"},
		{Name: "a function over STRING", Set: "s = UPPER(s)", Merge: "s = UPPER(s.sk)",
			Col: "s", Want: "AB", MergeWant: "ZZ"},
	}
}

// MergeSet is the clause body to use on the MERGE arm.
func (c Case) MergeSet() string {
	if c.Merge != "" {
		return c.Merge
	}
	return c.Set
}

// MergeValue is the expected stored value on the MERGE arm.
func (c Case) MergeValue() string {
	if c.MergeWant != "" {
		return c.MergeWant
	}
	return c.Want
}

// MergeSQLState is the SQLSTATE expected on the MERGE arm.
func (c Case) MergeSQLState() string {
	if c.MergeState != "" {
		return c.MergeState
	}
	return c.State
}
