# ADR-0003: Hand-written recursive-descent SQL parser

Status: Accepted (long-standing; recorded 2026-07-25)

## Context

The SQL surface grows continuously (273+ scalar functions, network-native
types, vector search). Parser generators (yacc/ANTLR-class) promise
grammar-first evolution; hand-written recursive descent promises control.

## Decision

`internal/planner/sql/` stays a hand-written recursive-descent parser
with a custom AST. New syntax is added by hand; no parser generator, ever
(codified in CLAUDE.md's "what not to do").

## Consequences

- Error messages, recovery, and dialect quirks (PostgreSQL wire
  compatibility is a hard requirement) stay under direct control.
- No generator toolchain in the build; the parser is plain Go that any
  contributor can step through.
- The cost is manual grammar work per feature — accepted because parse
  time has never appeared in a profile and the grammar churn rate is low.
