// SPDX-License-Identifier: MIT

package sysrows

import (
	"context"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// Resolver answers the catalog functions (expr.CatalogResolver) — regclass
// in both directions, pg_get_userbyid, pg_relation_is_publishable,
// pg_get_serial_sequence — from the SAME snapshot the catalog relations are
// built from, taken through the identity's view on the statement's context.
// So `c.oid::regclass` names exactly the relations pg_class lists for this
// identity, and a relation it cannot see is an OID that names nothing, as it
// is in pg_class.
//
// The snapshot is taken on first use, not at plan time: a statement that
// never evaluates a catalog function never reads the catalog for one.
type Resolver struct {
	ctx context.Context
	cat *catalog.Catalog

	once sync.Once
	snap *snapshot
	err  error
}

// NewResolver is the resolver for one statement.
func NewResolver(ctx context.Context, cat *catalog.Catalog) *Resolver {
	return &Resolver{ctx: ctx, cat: cat}
}

var _ expr.CatalogResolver = (*Resolver)(nil)

// resolverRel is the relation the snapshot error names when there is none.
var resolverRel = &syscatalog.Relation{Schema: syscatalog.SchemaPgCatalog, Name: "pg_class"}

func (r *Resolver) snapshot() *snapshot {
	r.once.Do(func() {
		r.snap, r.err = takeSnapshot(r.ctx, r.cat, resolverRel)
	})
	if r.err != nil {
		// Evaluation has no error channel; the refusal travels as the
		// fatal-evaluation panic every expression error uses.
		expr.RaiseFatal(r.err)
	}
	return r.snap
}

func (r *Resolver) rel(oid int64) *relInfo {
	return r.snapshot().byOID[oid]
}

// RelationName is regclass's output: the bare name for a relation whose
// schema is on the search path (pg_catalog and public), qualified otherwise.
func (r *Resolver) RelationName(oid int64) (string, bool) {
	ri := r.rel(oid)
	if ri == nil {
		return "", false
	}
	name := expr.QuoteIdent(ri.name)
	if ri.schema == syscatalog.SchemaInformationSchema {
		return syscatalog.SchemaInformationSchema + "." + name, true
	}
	return name, true
}

// RelationOID is regclass's input: an unqualified name resolves through the
// search path — pg_catalog, then public — and a qualified one in its schema.
func (r *Resolver) RelationOID(name string) (int64, bool) {
	parts := expr.SplitQualifiedName(name)
	switch len(parts) {
	case 1:
		for _, schema := range []string{syscatalog.SchemaPgCatalog, "public"} {
			if oid, ok := r.find(schema, parts[0]); ok {
				return oid, true
			}
		}
	case 2:
		return r.find(parts[0], parts[1])
	case 3:
		if parts[0] == syscatalog.Database {
			return r.find(parts[1], parts[2])
		}
	}
	return 0, false
}

func (r *Resolver) find(schema, name string) (int64, bool) {
	s := r.snapshot()
	// A system relation's name is exact; the storage catalog keeps a user
	// table's name as created and resolves it case-insensitively (#731).
	if ri, ok := s.byName[schema+"."+name]; ok && (!ri.user || ri.name == name) {
		return ri.oid, true
	}
	if ri, ok := s.byName[schema+"."+strings.ToLower(name)]; ok && ri.user {
		return ri.oid, true
	}
	return 0, false
}

// RelationKind is the relation's relkind and whether it is a user table.
func (r *Resolver) RelationKind(oid int64) (string, bool, bool) {
	ri := r.rel(oid)
	if ri == nil {
		return "", false, false
	}
	return ri.kind, ri.user, true
}

// ColumnExists reports whether a visible relation has a visible column.
func (r *Resolver) ColumnExists(oid int64, column string) bool {
	ri := r.rel(oid)
	if ri == nil {
		return false
	}
	for _, c := range ri.cols {
		if c.name == column {
			return true
		}
	}
	return false
}

// RoleName names the one role, this connection's identity.
func (r *Resolver) RoleName(oid int64) (string, bool) {
	if oid != syscatalog.BootstrapRoleOID {
		return "", false
	}
	return r.snapshot().user, true
}
