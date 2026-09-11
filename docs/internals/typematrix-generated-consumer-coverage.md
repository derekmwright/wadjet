# Typematrix generated consumer coverage

Source: internal/oracle/typematrix/typematrix.go — package typematrix, moved 2026-09-11 (#1026)
Superseded: The fixture uses separate flat and nested tables, not one table, so container fallback does not hide pooled native scan behavior.

Package typematrix is the fixture and query corpus for the type-coverage
gates: one table carrying all 22 column types, and a generated corpus that
pushes every type through every consumer that can retain, re-key, re-order
or re-encode a value.

Why it exists. Every differential corpus in this repo is built on three
storage types — Int32, Float64 and String (TPC-H schema.go; ClickBench adds
Int64 and Date). So an entire class of wrong-answer defects is structurally
invisible: no gate can see a bug that needs a BYTES, IPv4, UUID, DECIMAL or
nested column to fire. That is not hypothetical. (*Vector).GetValue's
TypeBytes arm returns a slice ALIASING the column arena, so MIN_BY over a
BYTES column answers with whatever the pool wrote into those bytes next —
a silent wrong answer that shipped through TPC-H, ClickBench, the DuckDB
fingerprint corpus, the PostgreSQL oracle, the two-path suite and the shape
fuzzer, because not one of them has a top-level BYTES column.

The corpus is GENERATED from a column table rather than hand-written, so a
23rd type is covered by adding one row to Columns() instead of by
remembering to write ten queries.

This package holds no assertions. Three gates consume it, each supplying its
own reference:

	wadjet.TestTypeMatrixBatchReuse             — poisoned pool vs clean pool
	wadjet.TestTypeMatrixOptimizationInvariance — each kill switch off vs on
	coordinator.TestTypeMatrixTwoPath           — stage DAG vs single process
