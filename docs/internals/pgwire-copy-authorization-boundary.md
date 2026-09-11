# Pgwire copy authorization boundary

Source: internal/server/pgwire/server.go — if err := auth.EnforceDMLPolicies(ctx, c.authProvider, c.db.Catalog(), &plansql.ParsedQuery{, moved 2026-09-11 (#1026)

COPY is a WRITE, and it authorizes BEFORE it invites the client to
stream — before `G` and before the ingester exists (#938).

It used to authorize nowhere at all. Unlike INSERT, which reaches
`wadjet.DB.ExecuteParsed` and its `auth.EnforceDMLPolicies` call, COPY
writes through `ingest.Ingester` directly, so a role granted only
`read` was handed CopyInResponse and its rows landed. That is the
bulk-ingest path: the largest write on this door was the one with no
decision on it.

The SECOND question: does the COLUMN LIST survive the identity's column
policy? `auth.EnforceDMLPolicies` over a synthesized INSERT — a COPY
column list is an INSERT target list, so it earns INSERT's answer
(naming a DENIED column is 42703) and no other. Forking a COPY-only rule
here would be a second reading of one question. The first question — may
this identity WRITE this relation — was asked above, before the
relation's existence was reported.

A refusal is sent INSTEAD of CopyInResponse, so the connection stays in
the ordinary message loop and the caller gets its ReadyForQuery; no row
is consumed and the ingester is never constructed.
