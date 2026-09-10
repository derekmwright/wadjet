// This file holds set op plan for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func (p *Planner) buildSetOp(ctx context.Context, node *logical.Node, op string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("%s requires two children", op)
	}
	// Arms with NO COMMON TYPE are refused HERE, at plan time, with
	// PostgreSQL's 42804 — the same refusal the stage DAG takes, from the same
	// walk, so one query has one answer (#648). Left to the runtime, this path
	// let the arms meet under the FIRST arm's box: `SELECT s FROM t UNION ALL
	// SELECT d FROM t` came back as a STRING column holding rendered decimals,
	// and the same pair the other way round failed mid-execution with 22P02 on
	// the first row of text that is not a number.
	if err := setOpArmTypeConflict(node); err != nil {
		return nil, nil, nil, err
	}

	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building %s left side: %w", op, err)
	}

	rightSource, rightOps, _, err := p.buildPipeline(ctx, node.Children[1])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building %s right side: %w", op, err)
	}

	src := &setOpSourceAdapter{
		leftSource:   leftSource,
		leftOps:      leftOps,
		rightSource:  rightSource,
		rightOps:     rightOps,
		all:          node.UnionAll,
		op:           op,
		leftLits:     setOpArmLiterals(node.Children[0]),
		rightLits:    setOpArmLiterals(node.Children[1]),
		leftUnknown:  setOpArmUnknownLits(node.Children[0]),
		rightUnknown: setOpArmUnknownLits(node.Children[1]),
	}

	return src, nil, &exec.CollectSink{}, nil
}

// setOpArmLiterals reads one arm's SELECT list and records, per OUTPUT
// POSITION, the exact DECIMAL a numeric LITERAL there names — the same
// `setOpLitArm` answer the stage DAG builds its arm projection from. A nil
// entry means "not a bare numeric literal", which is every other select item.
//
// PostgreSQL types a numeric constant `numeric` whenever it carries a decimal
// point or an exponent, so `SELECT d FROM t UNION ALL SELECT 1.23456` is a
// numeric union there (#665). The stage DAG resolves that; the single-process
// path built the literal arm's vector from the declared-type layer, which
// still answers float8 for a fractional literal everywhere, and the two paths
// then answered one query two ways: 1234567890123456.78 came back exact from
// the DAG and 1.2345678901234568e+15 from this one, and a join on the union's
// column matched nothing here because a float8 column met a DECIMAL key (#683).
func setOpArmLiterals(arm *logical.Node) []*setOpLitDecimal {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) == 0 {
		return nil
	}
	out := make([]*setOpLitDecimal, len(proj.Projections))
	any := false
	for i, pr := range proj.Projections {
		if pr.ASTExpr == nil {
			continue
		}
		if d, ok := setOpLitArm(pr.ASTExpr); ok {
			lit := d
			out[i] = &lit
			any = true
		}
	}
	if !any {
		return nil
	}
	return out
}

// setOpArmUnknownLits is setOpUnknownLiteralArms for the single-process path,
// whose arms are a NESTED tree rather than the DAG's flattened list: the width
// comes from this arm's own select list, and a nested set-operation arm has no
// output projection of its own, so it contributes no mask (its columns are
// already resolved by its own adapter).
func setOpArmUnknownLits(arm *logical.Node) []bool {
	proj := findOutputProjectionNode(arm)
	if proj == nil {
		return nil
	}
	return setOpUnknownLiteralArms(arm, len(proj.Projections))
}

// setOpApplyLiteralDecls restates a literal arm's column as the DECIMAL its
// SPELLING names, so the arms reconcile through the ordinary ladder.
//
// Applied only when the arm's runtime schema has one column per select item:
// anything else means the pipeline emitted columns this walk did not count,
// and a position is only an address while the two lists line up.
func setOpApplyLiteralDecls(schema []parquet.Column, lits []*setOpLitDecimal) []parquet.Column {
	if len(lits) == 0 || len(lits) != len(schema) {
		return schema
	}
	out := append([]parquet.Column(nil), schema...)
	for i, lit := range lits {
		if lit == nil {
			continue
		}
		out[i].Type = parquet.TypeDecimal
		out[i].Precision, out[i].Scale = lit.decl.Precision, lit.decl.Scale
	}
	return out
}

// setOpLiteralRows replaces a literal column's boxed value with the literal's
// plain decimal TEXT, on every row.
//
// The text is not decoration: the evaluator folds a numeric literal into a
// float64 box, so `1234567890123456.78` is already 1234567890123456.8 by the
// time it reaches this adapter and declaring DECIMAL over that box would put
// an exact type on a rounded number. A DECIMAL arrives here as its rendered
// text anyway (Vector.GetValue), so the literal's own text is the shape every
// reader below already expects — batch.FromRowsChecked parses it at the
// resolved scale and setOpCheckedDecimalText range-checks it, which is what
// gives this path the same 22003 the stage DAG raises for a literal the
// union's own type cannot hold (ADR-0024 item 7).
func setOpLiteralRows(rows []map[string]any, lits []*setOpLitDecimal) []map[string]any {
	if len(lits) == 0 {
		return rows
	}
	for i, lit := range lits {
		if lit == nil {
			continue
		}
		slot := setOpSlotName(i)
		for _, row := range rows {
			if _, ok := row[slot]; ok {
				row[slot] = lit.text
			}
		}
	}
	return rows
}

// setOpSourceAdapter executes both child pipelines and applies the set operation
// (union, intersect, or except) to produce the result.
type setOpSourceAdapter struct {
	leftSource  exec.Source
	leftOps     []exec.UnaryOperator
	rightSource exec.Source
	rightOps    []exec.UnaryOperator
	all         bool
	op          string // "union", "intersect", "except"
	// leftLits / rightLits carry the exact DECIMAL a numeric LITERAL select
	// item names, per output position; nil where the arm has none. See
	// setOpArmLiterals.
	leftLits  []*setOpLitDecimal
	rightLits []*setOpLitDecimal
	// leftUnknown / rightUnknown mark, per output position, the select items
	// that are UNKNOWN-typed literals — a quoted string or a bare NULL, which
	// PostgreSQL types from the OTHER arm. See setOpResolveUnknownLiteralArms.
	leftUnknown  []bool
	rightUnknown []bool

	batches     []*batch.RecordBatch
	idx         int
	initialized bool
}

func (u *setOpSourceAdapter) Init(_ context.Context) error { return nil }

func (u *setOpSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !u.initialized {
		u.initialized = true

		// Run left pipeline
		leftSink := &exec.CollectSink{}
		leftPipe := &exec.Pipeline{
			Source: u.leftSource,
			Ops:    u.leftOps,
			Sink:   leftSink,
		}
		if err := leftPipe.Run(ctx); err != nil {
			return nil, fmt.Errorf("executing %s left side: %w", u.op, err)
		}

		// Run right pipeline
		rightSink := &exec.CollectSink{}
		rightPipe := &exec.Pipeline{
			Source: u.rightSource,
			Ops:    u.rightOps,
			Sink:   rightSink,
		}
		if err := rightPipe.Run(ctx); err != nil {
			return nil, fmt.Errorf("executing %s right side: %w", u.op, err)
		}

		// SQL says the arms of a set operation correspond BY POSITION and
		// the result takes the FIRST arm's column names. These rows are
		// keyed maps, so an arm whose columns are spelled differently has
		// to be re-keyed before anything compares or concatenates them —
		// `SELECT n_regionkey FROM nation UNION SELECT r_regionkey FROM
		// region` deduped nothing (every row of one arm was a distinct map
		// from every row of the other) and batch.FromRows then read the
		// right arm's values under names it does not carry and wrote NULLs.
		//
		// Schema() instead of Batches()[0].Schema — ToRows below releases the
		// sinks' batches as it boxes them — and the arms' two schemas
		// UNIFIED rather than the first one alone. Under the first arm's
		// schema the arm ORDER decided the answer: FromRows re-reads each
		// row's rendered decimal text at the schema's scale, so the first
		// arm's scale truncated the second arm's values (#532); an INTEGER
		// arm was read raw as an unscaled carrier (#547); and a DECIMAL arm
		// under a FLOAT64 first arm failed the store outright while the same
		// pair the other way round silently kept the DECIMAL type (#541).
		// unifySetOpSchemas resolves the common type through the same
		// setOpWiden / setOpDecimalTarget the stage DAG uses, so the two
		// paths cannot answer with different types for the same query.
		//
		// The type is resolved HERE rather than at the FromRows call below
		// because the DEDUP KEY needs it too: a set operation decides
		// membership by equality, so two values the comparator calls equal
		// have to produce one key — which their BOXES alone cannot say, a
		// DECIMAL being rendered text (#499).
		leftSchema := setOpApplyLiteralDecls(leftSink.Schema(), u.leftLits)
		rightSchema := setOpApplyLiteralDecls(rightSink.Schema(), u.rightLits)
		leftSchema, rightSchema = setOpResolveUnknownLiteralArms(
			leftSchema, rightSchema, u.leftUnknown, u.rightUnknown)
		schema := unifySetOpSchemas(leftSchema, rightSchema)

		// The boxes are not uniform across types — a DECIMAL is its rendered
		// TEXT, an integer a raw int64, a float a float64 — so a widened
		// column needs each arm's box MOVED into the shape the unified column
		// reads, not merely relabelled. coerceSetOpArmRows does that for every
		// rung of the ladder before the arms meet, so both the dedup key and
		// FromRows read one shape per column — and ERRORS on a value that does
		// not fit the unified DECIMAL, the same overflow the stage DAG raises
		// (exec.coerceDecimalVector), rather than saturating silently. The
		// right arm is coerced against its OWN schema, before alignSetOpRows
		// re-keys it to the result names.
		//
		// POSITIONALLY, from here to the batch. SQL says the arms of a set
		// operation correspond by POSITION and a result may legally carry two
		// output columns of the same NAME — `SELECT n_name AS u, n_comment AS
		// u FROM nation UNION ALL …` is two columns called `u` in PostgreSQL
		// too. A map keyed by name holds ONE of them, so both output columns
		// came back carrying the SECOND source column's value: every row
		// wrong, no error, and only on this path — the stage DAG answers it
		// correctly, which is what isolated the collapse as the cause (#556,
		// and #844's UNION ALL branch, which is the same map).
		//
		// The rows keep their map form — every helper below reads it, and the
		// DECIMAL, dedup and overflow rules those helpers encode are not what
		// is wrong here — but their KEYS become slot positions, which are
		// addresses. The schemas are renamed to match for the duration and
		// the result batch is renamed back at the end, so nothing outside
		// this function sees a slot name.
		posResult := setOpSlotSchema(schema)
		leftRows, err := coerceSetOpArmRows(
			setOpLiteralRows(setOpArmRows(leftSink, leftSchema), u.leftLits),
			setOpSlotSchema(leftSchema), posResult)
		if err != nil {
			return nil, fmt.Errorf("executing %s left side: %w", u.op, err)
		}
		rightRows, err := coerceSetOpArmRows(
			setOpLiteralRows(setOpArmRows(rightSink, rightSchema), u.rightLits),
			setOpSlotSchema(rightSchema), posResult)
		if err != nil {
			return nil, fmt.Errorf("executing %s right side: %w", u.op, err)
		}

		keyer := newSetOpKeyer(posResult)

		var resultRows []map[string]any

		switch u.op {
		case "intersect":
			resultRows = intersectRows(keyer, leftRows, rightRows, u.all)
		case "except":
			resultRows = exceptRows(keyer, leftRows, rightRows, u.all)
		default: // "union"
			resultRows = append(leftRows, rightRows...)
			if !u.all {
				resultRows = deduplicateRows(keyer, resultRows)
			}
		}

		if len(resultRows) > 0 {
			if schema != nil {
				// FromRowsChecked, not FromRows: this is where the operation's
				// VALUES are materialized, and the unchecked writer answered a
				// DECIMAL with no carrier at the unified scale with the
				// SATURATED end of the Int128 range — a DECIMAL(38,0) arm's
				// 10^30 came back as 17014118346046923173168730371.5884105727
				// under a DECIMAL(38,10) union, silently (#553). ADR-0024
				// item 4: at a value-producing site, no exact carrier is a
				// 22003 error, never the nearest storable number.
				b, err := batch.FromRowsChecked(posResult, resultRows)
				if err != nil {
					return nil, fmt.Errorf("building the %s result: %w", u.op, err)
				}
				// Back to the names the query publishes. The slots were an
				// internal addressing scheme for the rows above; the result
				// takes the FIRST arm's column names, duplicates included.
				for i := range b.Schema {
					if i < len(schema) {
						b.Schema[i].Name = schema[i].Name
					}
				}
				u.batches = []*batch.RecordBatch{b}
			}
		}
	}

	if u.idx >= len(u.batches) {
		return nil, nil
	}
	b := u.batches[u.idx]
	u.idx++
	return b, nil
}

func (u *setOpSourceAdapter) Close() error {
	err := u.leftSource.Close()
	if e := u.rightSource.Close(); e != nil && err == nil {
		err = e
	}
	for _, op := range u.leftOps {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	for _, op := range u.rightOps {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (u *setOpSourceAdapter) RowsScanned() int64 {
	var total int64
	if sp, ok := u.leftSource.(exec.ScanStatsProvider); ok {
		total += sp.RowsScanned()
	}
	if sp, ok := u.rightSource.(exec.ScanStatsProvider); ok {
		total += sp.RowsScanned()
	}
	return total
}

// intersectRows returns rows that appear in both left and right.
// If all is true, preserves duplicate counts (min of left/right occurrences).
func intersectRows(k *setOpKeyer, left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[k.key(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := k.key(row)
			if rightSet[key] > 0 {
				result = append(result, row)
				rightSet[key]--
			}
		}
		return result
	}

	// INTERSECT (distinct): deduplicate, then keep only rows in both
	seen := make(map[string]struct{}, len(left))
	result := make([]map[string]any, 0)
	for _, row := range left {
		key := k.key(row)
		if _, already := seen[key]; already {
			continue
		}
		seen[key] = struct{}{}
		if rightSet[key] > 0 {
			result = append(result, row)
		}
	}
	return result
}

// exceptRows returns rows from left that do not appear in right.
// If all is true, each right occurrence removes one left occurrence.
func exceptRows(k *setOpKeyer, left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[k.key(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := k.key(row)
			if rightSet[key] > 0 {
				rightSet[key]--
			} else {
				result = append(result, row)
			}
		}
		return result
	}

	// EXCEPT (distinct): deduplicate left, exclude rows in right
	seen := make(map[string]struct{}, len(left))
	result := make([]map[string]any, 0)
	for _, row := range left {
		key := k.key(row)
		if _, already := seen[key]; already {
			continue
		}
		seen[key] = struct{}{}
		if rightSet[key] == 0 {
			result = append(result, row)
		}
	}
	return result
}

// alignSetOpRows re-keys one set-operation arm's rows onto the column names
// of the arm that decides the result schema (the first one). Arms correspond
// by POSITION in SQL, but these rows are name-keyed maps, so an arm selecting
// differently-spelled columns is invisible to rowHashKey and to
// batch.FromRows unless its keys are rewritten first.
//
// Returns rows unchanged when the schemas already agree, when either is
// unknown (an arm that produced nothing has no schema), or when the widths
// differ — a width mismatch is a malformed set operation, not something to
// paper over here.
// setOpSlotName is the internal address of one output column of a set
// operation. It is in the reserved hidden-slot namespace, so no query can
// spell it and it cannot collide with a column of either arm.
func setOpSlotName(i int) string { return "__setop_" + strconv.Itoa(i) }

// setOpSlotSchema is cols with every column renamed to its slot. Types,
// precision and scale are untouched — only the ADDRESS changes.
func setOpSlotSchema(cols []parquet.Column) []parquet.Column {
	out := make([]parquet.Column, len(cols))
	for i, c := range cols {
		c.Name = setOpSlotName(i)
		out[i] = c
	}
	return out
}

// setOpArmRows boxes one arm's result with its columns keyed by POSITION.
//
// CollectSink.ToRowValues is the positional form and it is non-nil exactly
// when the map form would lose a column — that is, when two of the arm's
// output columns share a name, which is the shape this exists for. When it is
// nil the map IS the positional form and the arm's own schema order supplies
// the addresses.
func setOpArmRows(sink *exec.CollectSink, schema []parquet.Column) []map[string]any {
	if vals := sink.ToRowValues(); vals != nil {
		out := make([]map[string]any, len(vals))
		for i, cells := range vals {
			m := make(map[string]any, len(schema))
			for j := range schema {
				if j < len(cells) {
					m[setOpSlotName(j)] = cells[j]
				}
			}
			out[i] = m
		}
		return out
	}
	rows := sink.ToRows()
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		m := make(map[string]any, len(schema))
		for j, c := range schema {
			m[setOpSlotName(j)] = row[c.Name]
		}
		out[i] = m
	}
	return out
}

// alignSetOpRows re-keys an arm's rows to the result's column names.
//
// It is no longer reached from the set-operation adapter, which addresses its
// arms by POSITION (setOpArmRows) and therefore needs no re-keying at all.
// Kept for the other caller and because it states the rule the slots enforce:
// the arms correspond by position, and the result takes the first arm's names.
func alignSetOpRows(want, have []parquet.Column, rows []map[string]any) []map[string]any {
	if len(want) == 0 || len(want) != len(have) {
		return rows
	}
	aligned := false
	for i := range want {
		if want[i].Name != have[i].Name {
			aligned = true
			break
		}
	}
	if !aligned {
		return rows
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		re := make(map[string]any, len(want))
		for j := range want {
			re[want[j].Name] = row[have[j].Name]
		}
		out[i] = re
	}
	return out
}

// deduplicateRows removes duplicate rows from a slice of row maps.
func deduplicateRows(k *setOpKeyer, rows []map[string]any) []map[string]any {
	seen := make(map[string]struct{}, len(rows))
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		key := k.key(row)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// rowHashKey generates a string key from a row's column values, with no types
// to consult: names sorted for determinism, values rendered with %v.
//
// It is the FALLBACK now, for a set operation whose schema cannot type the
// rows — an arm that produced nothing has none. setOpKeyer.key is the typed
// path and is what every schema-carrying set operation uses, because %v alone
// cannot say that a DECIMAL's "12.75" and "12.7500" are one value (#499).
func rowHashKey(row map[string]any) string {
	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	// Simple sort for determinism
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(fmt.Sprintf("%v", row[k]))
	}
	return b.String()
}
