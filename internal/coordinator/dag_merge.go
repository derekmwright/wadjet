// This file holds gather output renames and positional binding across group-key and aggregate output classes.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// applyOutputRenames PROJECTS each gather batch (and gr.columns) to exactly
// the SELECT-list output schema described by renames: drops columns the
// worker emitted but the user didn't ask for (e.g., Q15's join carries
// supplier/lineitem internals), and renames each kept column to the user's
// alias. Match is case-insensitive to tolerate worker-side lowercasing of
// expression text.
//
// Only applies when renames is non-empty AND every entry's source column
// resolves in the batch — if any source is missing (wrapped aggregates not
// yet handled), falls back to a rename-only pass so the output is at least
// non-empty rather than truncated to nothing.
func applyOutputRenames(gr *gatherResult, renames []physical.OutputRename) {
	if gr == nil || len(renames) == 0 {
		return
	}
	// Decide project-vs-rename from the column list the receiver derived
	// from the first batch's schema (identical names). When the result is
	// fully in memory and empty, keep the historical rename-only behavior;
	// a spilled result always has decoded prefix batches (the budget is
	// only exceeded after at least one decode), so columns are present.
	br := newBatchRenamer(renames, gr.columns)
	if len(gr.batches) == 0 && gr.spillPath == "" && len(gr.columns) > 0 {
		// Columns but no batches (historical edge): rename-only. A fully
		// empty result (no columns either) keeps projecting the column
		// list to the SELECT aliases, as before.
		br.project = false
	}
	gr.columns = br.renameColumns(gr.columns)
	for bi, b := range gr.batches {
		gr.batches[bi] = br.apply(b)
	}
	if gr.spillPath != "" {
		// Replayed batches get the same transform lazily as they are
		// decoded from scratch (gatherReplayStream).
		gr.renamer = br
	}
}

// batchRenamer is the per-batch form of applyOutputRenames: one decision
// (project vs rename-only) made up front from the output column names, then
// applied to each batch — eagerly to the in-memory prefix, lazily to batches
// replayed from gather spill scratch. Both paths MUST use the same instance
// so a single query's batches all share one schema shape.
type batchRenamer struct {
	renames  []physical.OutputRename
	compiled map[int]expr.Expr // index → compiled expr; nil map = compilation failed
	// exprType is the output column type for each compiled expression, decided
	// ONCE at construction so it cannot flap between batches (an all-null batch
	// would otherwise pick a different type than a populated one and corrupt
	// the gathered result's schema). A boolean wrapper (BETWEEN, AND/OR, IN,
	// comparison, IS, LIKE, ANY/ALL over a __win_/__agg_ column) materializes a
	// real Bool column so the DAG returns true/false — matching the
	// single-process pipeline and the PostgreSQL bool OID — rather than a
	// float64 0/1 (#610 review). Everything else stays float64, the historical
	// wrapped-aggregate output.
	exprType map[int]parquet.TypeID
	// exprDecl carries the DECIMAL (p,s) beside exprType for a rename the plan
	// declared, because a DECIMAL is an unscaled integer plus a scale and a
	// vector built at the wrong one reads back a power of ten out.
	exprDecl map[int]parquet.Column
	project  bool
}

// newBatchRenamer compiles expression-bearing renames and decides whether a
// full projection is possible: compilation succeeded AND every non-expr
// source column resolves in columns (case-insensitive, tolerating worker-
// side lowercasing). Otherwise batches get a rename-only pass so the output
// is at least non-empty rather than truncated to nothing.
func newBatchRenamer(renames []physical.OutputRename, columns []string) *batchRenamer {
	br := &batchRenamer{renames: renames, project: true}
	br.compiled = make(map[int]expr.Expr, len(renames))
	br.exprType = make(map[int]parquet.TypeID, len(renames))
	br.exprDecl = make(map[int]parquet.Column, len(renames))
	for i, r := range renames {
		if r.Expr == nil {
			continue
		}
		e, cerr := expr.Compile(r.Expr)
		if cerr != nil {
			// Compilation failure → degrade to rename-only.
			br.compiled = nil
			br.project = false
			break
		}
		br.compiled[i] = e
		// A boolean-typed compiled expression (every SQL predicate implements
		// the three-valued protocol) gets a real Bool output column; all other
		// wrapped expressions keep the historical float64 materialization.
		br.exprType[i] = parquet.TypeFloat64
		if _, ok := e.(expr.BoolNullExpr); ok {
			br.exprType[i] = parquet.TypeBool
		} else if _, ok := e.(expr.BoolExpr); ok {
			br.exprType[i] = parquet.TypeBool
		} else if r.TypeKnown {
			// The PLAN's declaration wins over the historical float64. A
			// column the gather computes exists in no catalog, so its declared
			// type IS its runtime type (ADR-0025), and it is the same
			// inference attachScanSelectProjections makes for a SELECT item a
			// FRAGMENT computes — one rule for a computed column's type,
			// whichever operator ends up computing it. Without it every
			// non-numeric, non-boolean result was nulled: `CAST(MAX(c_ts) AS
			// STRING)`, `UPPER(MAX(c_str))`, `CASE … THEN 'a' ELSE 'b' END`
			// over a window, on both DAG arms, for every type (#831, #645).
			br.exprType[i] = r.Type
			br.exprDecl[i] = parquet.Column{
				Type: r.Type, Precision: r.Precision, Scale: r.Scale,
			}
		}
	}
	if br.project && len(columns) > 0 {
		// The decision only: apply() resolves again against each batch it is
		// handed, so nothing is cached across batches.
		srcIdx := renameSourceIndices(columns, renames)
		for i, r := range renames {
			if r.Expr != nil {
				continue // existence check uses expression evaluation
			}
			if srcIdx[i] < 0 {
				br.project = false
				break
			}
		}
	}
	return br
}

// renameSourceIndices resolves every rename to the source column it reads,
// as INDICES, so that two renames can never be handed the same column.
//
// A name is not a key here. The SELECT list may legally carry two output
// columns of one name — PostgreSQL answers `SELECT upper(a), upper(b)` with
// two columns called `upper`, and #513 made this engine agree — and when the
// producing fragment MATERIALIZES that list (attachScanSelectProjections does,
// whenever an ORDER BY term forces a projection below the sort) the gather is
// handed two columns both named `upper` and two renames both spelled
// From:"upper". Resolving each by name gave both column 0, so the second
// output carried the first one's values: 25 rows of `ALGERIA | ALGERIA` where
// PostgreSQL answers `ALGERIA | NATION ALGERIA COMMENT`.
//
// Duplicates are assigned ORDINALLY — the k-th rename spelled X takes the
// k-th column spelled X — and the correspondence is exact because the
// matching is NAME-SCOPED. The two lists are not the same list: the rename
// list is the VISIBLE select items (extractOutputRenames walks
// VisibleProjections) while the producer's projection carries the hidden ones
// too (attachScanSelectProjections walks Projections, hidden ORDER BY terms
// included). What makes the k-th ↔ k-th correspondence hold anyway is that
// only columns SPELLED LIKE THE GROUP are counted, and a hidden term is named
// __sortkey_N — a spelling no user alias can collide with — so it can neither
// join a group nor shift one. Within one name, both lists are that name's
// select items in select order.
//
// A group of N renames over M columns of that name resolves by counting, and
// the three cases are different questions rather than degrees of confidence:
//
//	M >= N  ordinal. Each output has its own column, which is what the
//	        producer emits when it MATERIALIZED the select list.
//	M == 1  every rename reads it. The producer did NOT materialize, so the
//	        streams carry SOURCE columns and N select items reading ONE
//	        source share ONE column — `SELECT DISTINCT k AS u, k AS u` really
//	        is that column twice, and PostgreSQL answers it that way.
//	else    NOTHING (-1). 1 < M < N means the producer emitted several
//	        columns of the name but fewer than the outputs asking for it, and
//	        no counting rule maps them; handing each the same first match is
//	        precisely the defect this function exists to end. -1 makes the
//	        caller degrade to a rename-only pass, which returns a WIDER
//	        result than the select list — visibly wrong rather than silently
//	        wrong, and the degradation the projection has always taken when a
//	        source does not resolve.
func renameSourceIndices(names []string, renames []physical.OutputRename) []int {
	out := make([]int, len(renames))
	groups := make(map[string][]int, len(renames))
	for i, r := range renames {
		out[i] = -1
		if r.Expr != nil {
			continue // resolved by evaluation, not by column
		}
		key := strings.ToLower(strings.TrimSpace(r.From))
		groups[key] = append(groups[key], i)
	}
	for key, group := range groups {
		if len(group) > 1 {
			// Exact matches only: a duplicate arises from one literal alias
			// written N times, so the qualified/bare fallbacks resolveRename
			// Source applies have no part in deciding WHICH of the N a rename
			// means.
			var matches []int
			for i, n := range names {
				if strings.EqualFold(strings.TrimSpace(n), key) {
					matches = append(matches, i)
				}
			}
			if len(matches) >= len(group) {
				// A group that MIXES an aggregate output with a group-key
				// reference of one name (#575) cannot be assigned by plain
				// select-order ordinal: the producer emits all group keys
				// before all aggregates, so within `matches` (ascending) the
				// key columns come first and the aggregate columns after,
				// while in select order the aggregate can appear first. Pair
				// each rename with the column of its OWN class, ordinally
				// within the class. Uniform-provenance groups (the common
				// duplicate-alias case) keep the plain ordinal below.
				aggR, keyR := 0, 0
				for _, ri := range group {
					if renames[ri].IsAgg {
						aggR++
					} else {
						keyR++
					}
				}
				if aggR > 0 && keyR > 0 && len(matches) == len(group) {
					ki, ai := 0, keyR
					for _, ri := range group {
						if renames[ri].IsAgg {
							out[ri] = matches[ai]
							ai++
						} else {
							out[ri] = matches[ki]
							ki++
						}
					}
					continue
				}
				for k, ri := range group {
					out[ri] = matches[k]
				}
				continue
			}
			if len(matches) == 1 {
				// One column for N outputs: they read one SOURCE, so they
				// read one column. Correct, and what this path has always
				// answered.
				for _, ri := range group {
					out[ri] = matches[0]
				}
				continue
			}
			if len(matches) > 1 {
				// Several, but fewer than the outputs asking. Nothing maps
				// them; falling through to resolveRenameSource would hand
				// every member the same index (it is deterministic in From,
				// and a group shares one From), which is the defect itself.
				for _, ri := range group {
					out[ri] = -1
				}
				continue
			}
			// No exact match at all: the qualified↔bare fallbacks may still
			// find the column, and they answer the same way for every member
			// of the group — which is right when it is a single shared
			// source and refused outright when it is ambiguous.
		}
		if len(group) == 1 {
			// ONE rename over SEVERAL columns of its name. The producer is an
			// aggregate that publishes its key and its output under one name
			// — `SELECT COUNT(*) AS g, MIN(id) AS m FROM t GROUP BY g` names
			// the count `g` and the key is called `g` too — and the first
			// match is the KEY, so the DAG answered the key's values under the
			// aggregate's alias (0,1,2,3,4,5 for PostgreSQL's six 1s) while
			// the single-process path answered the count (#785 family,
			// ADR-0026 §3a).
			//
			// The same CLASS rule the mixed group above uses, and it rests on
			// the same fact: the aggregate emits every group key before every
			// aggregate output, so within `matches` (ascending) an aggregate
			// output is at the END and a key reference at the front. With one
			// column of the name both answers are that column, so this changes
			// nothing outside the collision.
			if idx, ok := classScopedMatch(names, renames[group[0]]); ok {
				out[group[0]] = idx
				continue
			}
		}
		out[group[0]] = resolveRenameSource(names, renames[group[0]].From)
	}
	return out
}

// classScopedMatch resolves ONE rename whose source name several columns
// answer to, by the CLASS of the select item: an aggregate output takes the
// LAST such column, a group-key reference the FIRST. It reports false when the
// name is not duplicated, where the ordinary resolution already answers.
func classScopedMatch(names []string, r physical.OutputRename) (int, bool) {
	if r.Expr != nil {
		return 0, false
	}
	matchesOf := func(key string) []int {
		var out []int
		for i, n := range names {
			if strings.EqualFold(strings.TrimSpace(n), key) {
				out = append(out, i)
			}
		}
		return out
	}
	key := strings.ToLower(strings.TrimSpace(r.From))
	matches := matchesOf(key)
	if len(matches) == 0 {
		// The same qualified→bare fallback resolveRenameSource applies, and
		// for the same reason: a SELECT item written through a derived
		// table's alias arrives spelled `u.g` while the stream carries the
		// bare `g` twice. Counting only the exact spelling found NO duplicate
		// and handed the item the first `g`, which is the KEY (#785 round 2).
		//
		// ONLY when the exact spelling matched NOTHING. The rescan used to run
		// whenever the exact spelling matched fewer than TWO columns, so an
		// item whose QUALIFIED name resolved uniquely was thrown away in
		// favour of the first column of its BARE name — and where the producer
		// publishes a name twice, two different items then bound one column.
		// `SELECT a.order_id, a.amount, b.amount FROM lat_item a JOIN lat_item
		// b ON b.order_id = a.order_id GROUP BY a.order_id, a.amount,
		// b.amount` reaches this over the stream `[order_id amount amount
		// a.order_id a.amount b.amount]`: `a.amount` and `b.amount` each
		// matched ONE column exactly and both were re-bound to the first bare
		// `amount`, so the third output carried the second's value on both DAG
		// arms while the single-process arms and PostgreSQL 17 answered eight
		// distinct rows. Exact first, then the fallback, is
		// `exec.ColumnIndexFallback`'s own order and the rule the rest of the
		// engine binds by.
		if dot := strings.IndexByte(key, '.'); dot >= 0 && dot < len(key)-1 {
			matches = matchesOf(key[dot+1:])
		}
	}
	if len(matches) < 2 {
		return 0, false
	}
	if r.IsAgg {
		return matches[len(matches)-1], true
	}
	return matches[0], true
}

// resolveRenameSource finds the column an OutputRename's source names, with
// the same bidirectional qualified↔bare fallback exec.ColumnIndexFallback
// applies everywhere else in the engine — and case-insensitively, because a
// worker emits computed columns under lowercased expression text.
//
// The fallback is what makes self-joins resolve. A join qualifies only its
// BUILD side's colliding columns, so one alias of a self-joined table ships
// under the bare name and the other under "alias.col" ("n_name" +
// "n2.n_name"). The SELECT list names both by their aliases, so the copy on
// the probe side ("n1.n_name") matches nothing exactly; with exact matching
// alone the whole projection degraded to rename-only and that column left
// the result under its raw worker name (#314). Order mirrors
// columnIndexFallback: exact, then qualified→bare, then bare→qualified with
// ambiguity (2+ matches) refused rather than guessed.
func resolveRenameSource(names []string, from string) int {
	for i, n := range names {
		if strings.EqualFold(n, from) {
			return i
		}
	}
	if dot := strings.IndexByte(from, '.'); dot >= 0 {
		bare := from[dot+1:]
		for i, n := range names {
			if strings.EqualFold(n, bare) {
				return i
			}
		}
		return -1
	}
	match := -1
	for i, n := range names {
		d := strings.LastIndexByte(n, '.')
		if d < 0 || !strings.EqualFold(n[d+1:], from) {
			continue
		}
		if match >= 0 {
			return -1 // ambiguous — refuse to guess
		}
		match = i
	}
	return match
}

// renameColumns returns the output column list: the renames' To names in
// order (project mode), or the input names with matches renamed in place.
func (br *batchRenamer) renameColumns(columns []string) []string {
	if br.project {
		out := make([]string, len(br.renames))
		for i, r := range br.renames {
			out[i] = r.To
		}
		return out
	}
	for i, c := range columns {
		columns[i] = br.renameOne(c)
	}
	return columns
}

func (br *batchRenamer) renameOne(name string) string {
	for _, r := range br.renames {
		if strings.EqualFold(name, r.From) {
			return r.To
		}
	}
	return name
}

// apply transforms one batch: project mode keeps only the renamed/computed
// columns in renames order; rename-only mode renames schema fields in place.
// Nil-safe (returns nil for nil input).
func (br *batchRenamer) apply(b *batch.RecordBatch) *batch.RecordBatch {
	if b == nil {
		return nil
	}
	if !br.project {
		for i := range b.Schema {
			b.Schema[i].Name = br.renameOne(b.Schema[i].Name)
		}
		return b
	}
	names := make([]string, len(b.Schema))
	for j, c := range b.Schema {
		names[j] = c.Name
	}
	// Resolved against THIS batch, every time. Caching the indices from the
	// column list the renamer was built with was wrong in a way a length
	// check cannot catch: a batch of the SAME WIDTH whose columns are in a
	// different order ([b a] where the renamer saw [a b]) silently read them
	// transposed. Re-resolving costs what the pre-#513 code already paid —
	// one resolveRenameSource per rename per batch — and is the only form
	// that is correct for the spill-replay path, whose batches are decoded
	// separately.
	srcIdx := renameSourceIndices(names, br.renames)
	newCols := make([]*batch.Vector, len(br.renames))
	newSchema := make([]parquet.Column, len(br.renames))
	for i, r := range br.renames {
		if e, ok := br.compiled[i]; ok {
			// Expression-bearing rename: evaluate per row, build a new
			// column. Used for wrapped aggregates ("SUM(x)/7.0") whose
			// post-aggregate divisor needs to be applied at gather time.
			// exprType is populated for every compiled index in
			// newBatchRenamer (TypeBool==0, so a zero-value default here would
			// be wrong — the map entry is always present instead).
			outType := br.exprType[i]
			col := evalExprColumn(e, b, outType, br.exprDecl[i])
			newCols[i] = col
			// The SCHEMA follows the vector, not the other way round: an
			// EXACT DECIMAL result is typed from the input schema at the first
			// batch (expr.DecimalResultOf), and a column whose declaration
			// disagreed with its own vector would be read back at the wrong
			// power of ten.
			newSchema[i] = parquet.Column{Name: r.To, Type: col.Type, Nullable: true}
			if col.Type == parquet.TypeDecimal {
				newSchema[i].Precision = br.exprDecPrecision(i, e, b)
				newSchema[i].Scale = col.DecimalData.Scale
			}
			continue
		}
		si := srcIdx[i]
		if si < 0 || si >= len(b.Columns) {
			// Should not happen — project was decided from these names.
			continue
		}
		newCols[i] = b.Columns[si]
		newSchema[i] = b.Schema[si]
		newSchema[i].Name = r.To
	}
	return &batch.RecordBatch{
		Schema:  newSchema,
		Columns: newCols,
		Len:     b.Len,
		Sel:     b.Sel,
	}
}
