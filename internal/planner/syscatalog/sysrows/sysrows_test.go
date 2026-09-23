// SPDX-License-Identifier: MIT

package sysrows

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
)

// syscatalog cannot import the expression package (it imports the parser,
// which imports syscatalog), so it restates the one database's name; the two
// must be the same name.
func TestTheCatalogsDatabaseIsTheSessionCatalog(t *testing.T) {
	if syscatalog.Database != expr.SessionCatalog {
		t.Fatalf("syscatalog.Database = %q, expr.SessionCatalog = %q", syscatalog.Database, expr.SessionCatalog)
	}
}
