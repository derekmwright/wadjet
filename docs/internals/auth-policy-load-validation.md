# Auth policy load validation

Source: internal/auth/abac_validate.go — func ValidateABACPolicies(policies []AccessControlPolicy) error {, moved 2026-09-11 (#1026)

ValidateABACPolicies refuses a policy set whose obligations cannot be
enforced as written. It runs where ParsePolicies' own refusal runs — at
config load and at hot reload — and for the same reason (#802): a
column-access control that cannot be understood must refuse to load, because
the alternative is an operator who believes a column is masked and is served
it in the clear.

The two refusals:

  - A `mask_column` obligation carrying NEITHER `value` nor `mask_func`.
    The enforcement path dropped such an obligation, so the column came back
    in the clear on every door. The type-derived placeholder ('***', 0,
    false) belongs to the LEGACY `policies: columns: {col: mask}` form, which
    MigrateRBACToABAC spells as `mask_func: redact`; an `abac_policies:`
    obligation says what it means with `value:`.
  - A `mask_column` whose `value` is not a SQL EXPRESSION. `value:
    "***REDACTED***"` — the spelling docs/configuration.md shipped for
    twelve releases — does not parse, and the fallback turned every masked
    column, string numeric and timestamp alike, into `0`. A mask that
    silently redefines itself is the same class of defect as one that
    silently disappears.

A `deny_column` or `mask_column` with no target names no column and is
refused for the same reason.

It also enumerates the SECURITY VOCABULARY — effect, action, condition
operator, condition attribute namespace, obligation type — and refuses a
word outside it (#932). Every one of those fields used to accept anything:
`effect: dney` became an ALLOW that granted what the operator wrote a deny
for, an obligation type `deny_colum` was dropped and the column came back in
plaintext, and an operator or attribute the evaluator does not implement
made its rule never match, which beside a broad allow is a grant. A closed
vocabulary that nothing checks is not a vocabulary.
