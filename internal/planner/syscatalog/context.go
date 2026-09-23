// SPDX-License-Identifier: MIT

package syscatalog

import "context"

// Access is the calling identity's view of the storage catalog, as the door
// that authorized the statement decided it. The catalog relations are built
// from it and from nothing else, so every one of them — pg_class,
// pg_attribute, pg_tables, information_schema.tables and .columns, and every
// join between them — follows the one decision (ADR-0034's "metadata follows
// the effective table decision").
//
// It travels on the context because the decision lives in internal/auth,
// which imports the planner; the door installs it where it authorizes the
// statement's table functions (auth.AuthorizeTableFunctions), which every
// SELECT door already calls before it plans.
type Access struct {
	// User is the identity the catalog reports as the current role — the
	// owner of every object and the one row of pg_roles.
	User string
	// VisibleTables filters the storage catalog's relations to the ones this
	// identity may read. Nil means every relation is visible.
	VisibleTables func(ctx context.Context, tables []string) []string
	// DeniedColumns names the columns of one table this identity may not
	// see, folded to lower case; a denied column does not exist for it, in
	// the catalog as in a SELECT. Nil means none are denied.
	DeniedColumns func(ctx context.Context, table string) map[string]bool
}

// Session is what a catalog relation knows about the connection a statement
// arrived on: pg_stat_ssl reports it.
type Session struct {
	SSL        bool
	SSLVersion string
	SSLCipher  string
}

type accessKey struct{}
type sessionKey struct{}

// WithAccess installs the identity's catalog view on ctx.
func WithAccess(ctx context.Context, a Access) context.Context {
	return context.WithValue(ctx, accessKey{}, a)
}

// AccessFromContext returns the installed view, and whether one was.
func AccessFromContext(ctx context.Context) (Access, bool) {
	a, ok := ctx.Value(accessKey{}).(Access)
	return a, ok
}

// WithSession installs the connection's properties on ctx.
func WithSession(ctx context.Context, s Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, s)
}

func SessionFromContext(ctx context.Context) Session {
	s, _ := ctx.Value(sessionKey{}).(Session)
	return s
}
