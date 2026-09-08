package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// These two back the allow-list entries in
// logical.TestOnlyOnePathReadsAScanColumnListForAStar. Both functions read a
// scan's catalog-annotated column list, and both are safe for a POLICED
// relation only because their descent returns at the first Project — which the
// security projection over a policed scan is. That is a structural claim, so it
// is asserted here rather than argued in a comment beside the allow-list.

func starPolicyScan() *logical.Node {
	return &logical.Node{
		Type:        logical.NodeScan,
		TableName:   "emp",
		ScanColumns: []string{"id", "dept", "ssn", "salary"},
		ScanColTypes: map[string]parquet.TypeID{
			"id": parquet.TypeInt64, "dept": parquet.TypeString,
			"ssn": parquet.TypeString, "salary": parquet.TypeInt64,
		},
	}
}

func starPolicyBarrier(scan *logical.Node) *logical.Node {
	return &logical.Node{
		Type:            logical.NodeProject,
		Children:        []*logical.Node{scan},
		SecurityBarrier: true,
		Projections: []logical.Projection{
			{Column: "id", Alias: "id"},
			{Column: "dept", Alias: "dept"},
			{Alias: "ssn", Expr: "'***'"},
		},
	}
}

// TestABareStarOverAPolicedScanDeclinesToDeclareFromTheCatalog: the zero-row
// `SELECT *` declaration (#846) must not name a column the identity's policy
// DENIES. It cannot, because the walk stops at any Project and the security
// projection is one — so the declaration comes from the barrier through the
// ordinary projection walk, and this function answers "not my shape".
func TestABareStarOverAPolicedScanDeclinesToDeclareFromTheCatalog(t *testing.T) {
	scan := starPolicyScan()

	// The control: with no policy there is no barrier, and the bare star IS
	// declared from the catalog — four columns, salary among them.
	cols, ok := starOnlyDeclaredOutputSchema(scan)
	if !ok || len(cols) != 4 {
		t.Fatalf("unpoliced: got %d columns ok=%v, want the scan's 4", len(cols), ok)
	}

	// With the barrier, it declines rather than declaring the catalog's list.
	if cols, ok := starOnlyDeclaredOutputSchema(starPolicyBarrier(scan)); ok {
		names := make([]string, 0, len(cols))
		for _, c := range cols {
			names = append(names, c.Name)
		}
		t.Fatalf("a policed scan was declared from the CATALOG as %v; the declaration must come "+
			"from the security projection, so this walk has to decline", names)
	}
	// …and through a filter, which is where a policy row filter sits.
	f := &logical.Node{Type: logical.NodeFilter,
		Children: []*logical.Node{starPolicyBarrier(starPolicyScan())}}
	if _, ok := starOnlyDeclaredOutputSchema(f); ok {
		t.Fatal("a policed scan under a filter was declared from the catalog")
	}
}

// TestTheCTEOutputHelperReadsTheBarrier is the physical half of
// logical.TestTheSubtreeOutputHelpersReadTheBarrier.
func TestTheCTEOutputHelperReadsTheBarrier(t *testing.T) {
	got := cteOutputNames(starPolicyBarrier(starPolicyScan()))
	want := []string{"id", "dept", "ssn"}
	if len(got) != len(want) {
		t.Fatalf("cteOutputNames = %v, want the policed list %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cteOutputNames = %v, want the policed list %v", got, want)
		}
	}
	// The control: a bare scan still publishes its own columns.
	if n := len(cteOutputNames(starPolicyScan())); n != 4 {
		t.Fatalf("cteOutputNames over a bare scan = %d columns, want 4", n)
	}
}
