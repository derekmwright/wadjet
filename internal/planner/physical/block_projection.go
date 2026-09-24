// SPDX-License-Identifier: MIT

// The published columns of a projection BLOCK, and the bare name a block
// publishes. It was called block_projection_stage.go; the stage that carries
// a block's projection is emitted in internal/coordinator/dagplan
// (ADR-0037).
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A derived block read by a star must publish its projection by position,
// under its own names, through Stage.ProjectExprs/OpProject (#984). The join
// then names the star's columns from that relation: duplicate sources, renames,
// aggregate aliases and computed items must not expose the underlying stream.
// Scope is star-only: a Project between root and block means named columns,
// whose consumers resolve their own names (the projected test shared with
// refuseLateralProjection).
// See docs/internals/star-read-block-projections.md for the design.

// blockBareName drops a qualifier: the projection spells `s.oid` where the
// stream spells `oid`, and that is not a divergence.
func blockBareName(s string) string {
	s = strings.TrimSpace(s)
	if dot := strings.LastIndexByte(s, '.'); dot >= 0 && dot < len(s)-1 {
		return s[dot+1:]
	}
	return s
}

// blockColumn is ONE column the block publishes, in the block's own order:
// the name a consumer above reads it under, the expression the stage's own
// input evaluates it from, and the type it is declared as.
//
// The three travel together because two of them are read by different passes
// and a disagreement between those passes is an ADR-0010 refusal, not a
// cosmetic one: the fragment computes the column from Expr and declares it
// from Decl, and an empty side of the same join declares it from Decl alone.
// Deriving both from one walk is what makes "one stage's files describe one
// relation" true by construction rather than by two rules agreeing.
type blockColumn struct {
	Name string
	Expr string
	Decl parquet.Column
	// DeclKnown is carried beside Decl because parquet.TypeBool IS the zero
	// value: `COUNT(*) = 0 AS n` reads as "no declaration" through a
	// `Type != 0` test, projectOpFromSpecs then drops the type off the wire,
	// and the worker's buildSelectProjection guesses STRING for a column that
	// is a bool (#445). The empty side of the same join declared BOOL, and
	// the two files disagreed under ADR-0010.
	DeclKnown bool
}

// blockPublishedColumns is the relation a derived block publishes, or ok=false
// when the plan cannot state it.
//
// ok=false is a real answer and not a shrug: a column with no plan-time type
// would be COMPUTED by the fragment and MISSING from the empty side's
// declaration, which is the width disagreement ADR-0010 refuses. Declining
// leaves the plan exactly as it was.
func blockPublishedColumns(p *logical.Node, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) ([]blockColumn, bool) {
	if p == nil || len(p.Children) != 1 || len(p.Projections) == 0 {
		return nil, false
	}
	// The STREAM's own declaration, typed by the walk that owns each
	// producer's rule — the aggregate arm types a group key and an AggSpec,
	// the scan arm reads the catalog annotation. A minted correlation slot and
	// an `__agg_N` have a type only their producer can state, and a second
	// rule for them is the disagreement ADR-0026 exists to prevent.
	stream := declaredJoinSchema(p.Children[0], nil, published, subqueryDecl)
	byName := make(map[string]parquet.Column, len(stream))
	for _, col := range stream {
		byName[strings.ToLower(blockBareName(col.Name))] = col
	}
	// A COMPUTED item is typed against what the stage's input EMITS, not
	// against what the block's child reads: above an aggregate the operands
	// are `__agg_N` and a minted slot, and typing `COUNT(*) + 1` against the
	// scan's columns answers nothing at all.
	decls := inputColDecls(p.Children[0])
	// THE SCALAR-SUBQUERY RESOLVER IS PART OF THE INFERENCE, not an extra.
	// `DeclaredOutputSchema` hands it to the same walk for the statement's own
	// SELECT list, and without it here `(SELECT MAX(amount) FROM lat_item) AS
	// sq` fell to the STRING fallback: a zero-row `SELECT *` over a block
	// holding one declared `sq` as text where the same statement under a
	// matching predicate declares int8 and PostgreSQL declares integer. One
	// inference means one set of arguments too (round-4 B1).
	decls.subqueryDecl = subqueryDecl
	if decls.Types == nil {
		decls.Types = map[string]parquet.TypeID{}
	}
	strictInt := strictIntArithCols(p.Children[0])
	if strictInt == nil {
		strictInt = map[string]bool{}
	}
	for _, col := range stream {
		lc := strings.ToLower(blockBareName(col.Name))
		if _, ok := decls.Types[lc]; !ok {
			decls.Types[lc] = col.Type
			if col.Type == parquet.TypeDecimal {
				if decls.Dec == nil {
					decls.Dec = map[string]logical.DecimalMeta{}
				}
				decls.Dec[lc] = logical.DecimalMeta{
					Precision: col.Precision, Scale: col.Scale,
				}
			}
		}
		// An INTEGER the producer emits keeps integer arithmetic exact above
		// it, the same hint absorbComputedSubqueryProjection passes when it
		// materializes a computed column into a scan fragment (#297, #445):
		// without it `COUNT(*) + 1` declares FLOAT64 where every other path
		// answers a bigint (ADR-0024).
		if intArithColumnType(col.Type) {
			strictInt[lc] = true
		}
	}
	// VISIBLE items only. A hidden `__sortkey_N` is the planner's own — a
	// materialized ORDER BY term the block's SELECT list does not carry — and
	// publishing it put a reserved name on the wire that no query can spell
	// (round-1 P3). It is the same list extractOutputRenames walks for the
	// statement's own projection, and for the same reason.
	items := logical.VisibleProjections(p.Projections)
	sortKeyFamily := plansql.ReservedSlotFamily(plansql.SlotName(plansql.SlotSortKey, 0))
	out := make([]blockColumn, 0, len(items))
	for _, pr := range items {
		if pr.IsAgg {
			// An aggregate SELECT item is computed by the aggregate stage,
			// not by a projection above it.
			return nil, false
		}
		name := pr.Alias
		if name == "" {
			name = pr.Column
		}
		if name == "" {
			name = strings.TrimSpace(pr.Expr)
		}
		bare := strings.ToLower(blockBareName(name))
		if bare == "" {
			return nil, false
		}
		// A MATERIALIZED ORDER BY TERM is the planner's own and is not part of
		// the relation the block publishes. `VisibleProjections` trims the
		// ones the builder flagged hidden; a block whose own `ORDER BY amount
		// LIMIT 3` was materialized carries one as an ordinary item, and
		// publishing it put `__sortkey_0` on the wire where no query can spell
		// it. The correlation slot is NOT trimmed here — the join keys on it
		// and drops it by position (ADR-0026 §3c).
		if plansql.ReservedSlotFamily(bare) == sortKeyFamily {
			continue
		}
		if pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr) {
			// ONE INFERENCE, and it is the SINGLE PATH'S. This is the call
			// `DeclaredJoinSchema`'s own computed-column arm makes and the
			// call `attachScanSelectProjections` makes for the statement's own
			// SELECT list: the same walk, the same `strictInt` hint, the same
			// scalar-subquery resolver, and the same STRING FALLBACK.
			//
			// A weaker second inference here is what three rounds of this arc
			// kept tripping over. It declined on `expr.Undecided` and each
			// decline became a DISPOSITION — a query the single path answers
			// with a declared type was routed off the DAG, or worse, left to
			// read the stream. There is no such thing as an item this engine
			// cannot declare: `SELECT ARRAY[c0] AS a`, an all-NULL `CASE`, a
			// bare `NULL` and `COALESCE(NULL, NULL)` all reach a client with
			// an OID today (25, 25, 25, 701), and a scalar-subquery item
			// reaches it with its own (20, through `subqueryDecl`). Whatever
			// that walk answers is what this stage publishes, so the two paths
			// describe one relation by construction.
			materialized := declTypeParts(
				inferProjectionDeclType(pr.ASTExpr, parquet.TypeString, strictInt, decls))
			t, prec, scale, fields := materialized.Type, materialized.Precision, materialized.Scale, materialized.Fields
			out = append(out, blockColumn{
				Name: name, Expr: pr.ASTExpr.String(),
				Decl: parquet.Column{
					Name: name, Type: t, Precision: prec, Scale: scale, Fields: fields, Nullable: true,
					ElementType: materialized.ElementType,
				},
				DeclKnown: true,
			})
			continue
		}
		// A BARE item is read under the spelling the STREAM carries. The
		// source first, because that is what an ordinary rename reads; then
		// the item's OWN name, because a MINTED correlation slot's item is
		// spelled `__key_0` and reads `order_id`, and the aggregate below
		// emits the slot rather than the source it was computed from.
		src := pr.Column
		if src == "" {
			src = strings.TrimSpace(pr.Expr)
		}
		spelling := strings.ToLower(blockBareName(src))
		col, ok := byName[spelling]
		if !ok {
			spelling = bare
			col, ok = byName[spelling]
		}
		if !ok {
			return nil, false
		}
		col.Name, col.Nullable = name, true
		out = append(out, blockColumn{Name: name, Expr: spelling, Decl: col, DeclKnown: true})
	}
	return out, true
}

// declaredBlockSchema is the block's published relation as a plan-time column
// list — the declaration a side of a join that delivers NO BATCH AT ALL is
// shaped by.
//
// A stage's files describe ONE relation (ADR-0010), and once a block is
// materialized that relation is the PROJECTION. DeclaredJoinSchema's ordinary
// walk descends past a Project and declares the scan's or the aggregate's own
// columns, so an empty build task wrote the stream's columns beside sibling
// files carrying the projection's: `declares 3 columns where an earlier file
// of the same stage input declared 4`, and where the widths happened to agree,
// `names column 3 "n" where an earlier file ... named it "__agg_0"`. That is
// #980's own sentence, and it is what the shape does the moment the lateral
// route stops standing in front of it.
func declaredBlockSchema(p *logical.Node, wantSet map[string]bool,
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	cols, ok := blockPublishedColumns(p, published, subqueryDecl)
	if !ok {
		return nil
	}
	out := make([]parquet.Column, 0, len(cols))
	for _, c := range cols {
		if len(wantSet) > 0 && !wantSet[strings.ToLower(blockBareName(c.Name))] {
			continue
		}
		out = append(out, c.Decl)
	}
	return out
}
