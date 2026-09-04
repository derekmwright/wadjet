package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// policedColumnSource is a tableColumnSource with a column-policy's DENIED
// columns removed from every table's declared schema.
//
// A denied column does not exist for this identity, and "does not exist" is
// a statement the name binder already knows how to make: `column "salary"
// does not exist`, SQLSTATE 42703, PostgreSQL's own message shape. Before
// #859 a denied column was merely dropped from the security projection, so
// `SELECT salary FROM t` came back as a phantom all-NULL column on the
// single-process path and as the whole `SELECT *` row on the DAG, and
// `WHERE salary > 0` was answered from the RAW column below the projection
// — a working oracle for the value the policy denies.
type policedColumnSource struct {
	src tableColumnSource
	// deniedFor answers, for one table, the columns this identity may not
	// see (folded). It is a FUNCTION and not a map because the binder is what
	// enumerates the relations a statement reads — CTE bodies, derived
	// tables, subquery blocks and set-operation arms included — and it does
	// so before any plan exists. Asking it per table as the binder resolves
	// them is what lets the policy be known at the FIRST name-binding pass,
	// so `SELECT nosuchcol FROM t` cannot answer with a hint that lists a
	// column the policy denies.
	deniedFor func(table string) map[string]bool
}

func (p policedColumnSource) GetTable(ctx context.Context, name string) (*catalog.TableMeta, error) {
	meta, err := p.src.GetTable(ctx, name)
	if err != nil || meta == nil {
		return meta, err
	}
	drop := p.deniedFor(resolveTableSpelling(p.src, name))
	if len(drop) == 0 {
		drop = p.deniedFor(name)
	}
	if len(drop) == 0 {
		return meta, nil
	}
	// Copy: the catalog's TableMeta is shared and must not lose columns for
	// every other identity in the process.
	cols := make([]parquet.Column, 0, len(meta.Schema.Columns))
	for _, c := range meta.Schema.Columns {
		if drop[strings.ToLower(c.Name)] {
			continue
		}
		cols = append(cols, c)
	}
	out := *meta
	out.Schema = parquet.Schema{Columns: cols}
	return &out, nil
}

// ResolveTableName / AmbiguousTableNames forward so the wrapper keeps the
// binder's relation-name case concession (#731).
func (p policedColumnSource) ResolveTableName(name string) string {
	return resolveTableSpelling(p.src, name)
}

func (p policedColumnSource) AmbiguousTableNames(name string) []string {
	if r, ok := p.src.(tableNameResolver); ok {
		return r.AmbiguousTableNames(name)
	}
	return nil
}

// ValidateColumnsUnderPolicy is ValidateColumns run against the schema THIS
// IDENTITY can see: every column deniedFor names is removed from its table
// before binding, so a reference to one resolves to nothing and comes back as
// the ordinary 42703 — the same error, byte for byte, that a column the table
// really does not have produces. That equality IS the meaning of "denied":
// nothing about the column, not even its name in an error's hint, survives
// the policy.
//
// deniedFor nil, or a nil catalog, is the plain unfiltered validation.
func ValidateColumnsUnderPolicy(ctx context.Context, cat *catalog.Catalog, info *plansql.SelectInfo, deniedFor func(table string) map[string]bool) error {
	if cat == nil || info == nil {
		return nil
	}
	if deniedFor == nil {
		return validateColumns(ctx, cat, info)
	}
	return validateColumns(ctx, policedColumnSource{src: cat, deniedFor: deniedFor}, info)
}

// applyContextColumnPolicies enforces the query's column policies on a plan
// this planner built for itself — the expression-subquery path, which never
// passes through auth.EnforcePlanPolicies.
//
// It returns an error when a policed scan could not be covered, because the
// alternative is answering that subquery from the raw column.
func (p *Planner) applyContextColumnPolicies(ctx context.Context, plan *logical.Node) (*logical.Node, error) {
	pol := logical.ColumnPoliciesFromContext(ctx)
	if len(pol) == 0 {
		return plan, nil
	}
	plan, unprotected := pol.Apply(plan, func(table string) []string {
		if p.catalog == nil {
			return nil
		}
		meta, err := p.catalog.GetTable(ctx, table)
		if err != nil || meta == nil {
			return nil
		}
		cols := make([]string, len(meta.Schema.Columns))
		for i, c := range meta.Schema.Columns {
			cols[i] = c.Name
		}
		return cols
	})
	if unprotected > 0 {
		return nil, logical.ErrColumnPolicyUnenforceable
	}
	return plan, nil
}

// applyContextColumnPoliciesToNewScans is applyContextColumnPolicies for the
// scans the OPTIMIZER minted — decorrelation re-parses a subquery and builds a
// fresh Scan, after the policy went in. A scan already under a security
// barrier is skipped.
func (p *Planner) applyContextColumnPoliciesToNewScans(ctx context.Context, plan *logical.Node) (*logical.Node, error) {
	pol := logical.ColumnPoliciesFromContext(ctx)
	if len(pol) == 0 {
		return plan, nil
	}
	plan, unprotected, err := pol.ApplyToNewScansWithLookup(plan, func(table string) []string {
		return p.policyTableColumns(ctx, table)
	}, logical.PolicyLookupFromContext(ctx))
	if err != nil {
		return nil, err
	}
	if unprotected > 0 {
		return nil, logical.ErrColumnPolicyUnenforceable
	}
	return plan, nil
}

func (p *Planner) policyTableColumns(ctx context.Context, table string) []string {
	if p.catalog == nil {
		return nil
	}
	meta, err := p.catalog.GetTable(ctx, table)
	if err != nil || meta == nil {
		return nil
	}
	cols := make([]string, len(meta.Schema.Columns))
	for i, c := range meta.Schema.Columns {
		cols[i] = c.Name
	}
	return cols
}
