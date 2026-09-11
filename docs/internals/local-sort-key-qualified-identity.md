# Local sort key qualified identity

Source: internal/planner/physical/sort_key_slots.go — sortKeyLocalColumn, moved 2026-09-11 (#1026)

```go
// sortKeyLocalColumn is the spelling the single-process Sort resolves an ORDER
// BY term by: THE ONE THE QUERY WROTE, qualifier and all (#989).
//
// The qualifier is the only thing that distinguishes one reference's column
// from another's when the two share a bare name, and a Sort over a join reads
// exactly that stream: `exec.joinOutputSchemaWithMapping` publishes the probe's
// columns bare and qualifies every DUPLICATE build column by its owning alias,
// so `SELECT * FROM q a JOIN q b …` publishes `[order_id amount b.order_id
// b.amount]`. Stripping the qualifier here — which is what `cleanExpr` does —
// made `a.amount` and `b.amount` into ONE key, `amount`, and
// `exec.columnIndexFallback` bound both of them to the first column carrying
// it. `ORDER BY a.order_id, a.amount, b.amount` is a TOTAL order, so exactly
// one sequence is legal (ADR-0013 lists no class this falls under), and the
// single-process and spilled arms answered the trailing key INVERTED inside
// every peer group while both DAG arms — whose sort keys keep the qualified
// spelling — answered PostgreSQL's order (#989).
//
// This is ADR-0026 §6 at the ORDER BY consumer: a consumer binds through the
// identity its PRODUCER published, and never by re-reading a name as
// structure (§2c). It needs no model of which side of the join built, because
// `columnIndexFallback` tries the qualified spelling FIRST and falls back to
// the bare one — so `b.amount` binds `b.amount` when the join qualified b, and
// binds the bare `amount` when the join qualified a instead. The name-based
// resolution the key had before is the second step of the same resolver, so
// every term that resolved before still resolves to the same column; the only
// answer that moves is the one where the stream really does carry the
// qualified column the term names.
//
// #905 gave the same family its POSITION where the Sort's child is a Project
// (`sortKeyLocalSlotPos`), and that address still wins: a star-only query has
// no Project to take a position from, which is why the qualified spelling is
// the address left.
```
