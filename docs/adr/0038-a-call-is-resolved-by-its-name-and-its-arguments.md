# ADR-0038: A call is resolved by its name AND its arguments, at bind time

Status: Accepted (2026-09-18, #1053 / #1056 / #583)

## Context

`expr.DefaultRegistry` maps a function name to `func(args []any) any`. That is
the whole contract: the name says which body runs, and the body reads
`args[i]`. Nothing between the query text and the body knows how many
arguments the function takes or what any position accepts.

PostgreSQL resolves a call by its name AND its argument types, and reports
every failure of that resolution as one error — `42883 function f(types) does
not exist`. Three shapes reached this engine and none of them was that:

- **a wrong COUNT.** `semver_cmp('1.0.0')` answered NULL, because the body read
  a missing `args[1]` defensively; `upper('a','b')` answered `'A'` with the
  extra argument dropped. A filter over such a call then evaluated NULL for
  every row and dropped the relation, silently.
- **a numeric LITERAL in a text position.** `FuncCall.EvalVec` built a constant
  INT64 vector for it and handed that to a STRING kernel, which indexed an
  offsets array that does not exist on such a vector: `XX000`, an internal
  error, for a call PostgreSQL either answers or refuses.
- **a BYTES operand in a text-only position.** `expr.toString` read those bytes
  as whatever text they spell, so `upper(payload)` answered a value — the
  wrong one, with #570's embedded-NUL hazard behind it.

They are one defect seen three times: the layer that would refuse them has no
declaration to refuse against.

## Decision

**The registry is the signature table, and the refusal happens at BIND time.**

1. `expr.Signature` declares `Min`, `Max` (or `Variadic`) and a per-position
   `ArgDomain` — `ArgAny`, `ArgText`, `ArgTextOrBytes`, `ArgBytes`.
   `funcSignatures` carries one row per registered builtin, and `textDomains`
   carries the positions PostgreSQL restricts.
2. The table is CLOSED in both directions: a registry entry with no row and a
   row naming nothing registered both fail
   (`expr.TestEveryRegisteredFunctionDeclaresItsArity`), and each declared
   arity is checked against the signature `docs/sql-reference.md` documents
   (`TestTheDocumentedSignatureIsTheDeclaredArity`). A new builtin cannot be
   added without declaring what it takes. Three rows name a function this
   package does not register — `embed`, `embed_dim`, `embed_model`, which
   `internal/embedding` registers only when a provider is wired — and they are
   allowed BY NAME in that gate's `registeredElsewhere` list, named rather than
   silently tolerated.
3. `expr.RefuseUnresolvableCall` runs from the binder's own per-call walk
   (`physical.refuseInvalidRowFields`), which BOTH planner entries reach before
   any stage or pipeline exists, with `expr.compileFuncCallNamed` as the
   backstop. The kernel is never reached with an operand it cannot read.
4. **Every grammar spelling this engine rewrites into a call the query did not
   write is enumerated against that table.** `TRIM(BOTH c FROM s)`,
   `POSITION(x IN y)`, `EXTRACT(f FROM t)`, the niladic no-parenthesis
   functions, `AT TIME ZONE`, a computed ROW field, `ILIKE`, `SIMILAR TO`'s
   `similar_to(x, p[, e])`, `LIKE … ESCAPE`'s `like_escape(x, p, e)`, the
   SQL-standard `SUBSTRING`/`OVERLAY`/`NORMALIZE`/`LOCALTIMESTAMP` spellings,
   `LEFT`/`RIGHT` read as function names, the `^` operator's `power(a, b)`,
   the `#` operator's `bitwise_xor(a, b)` and a `JOIN … USING` merge's
   `COALESCE(l.c, r.c)` all land on a name the table declares
   (`physical.TestEveryGrammarRewriteProducesACallItsSignatureAccepts`). What
   that gate asks of each is the produced NAME and ARITY: the merge's entry is
   a two-argument `COALESCE` written as an expression, because the gate parses
   an expression and a `USING` clause needs a FROM clause, so it stands in for
   the call `logical.mergedUsingItem` and `plansql.bindMergedUsingKeys` mint
   rather than reproducing the statement. Its second direction reads the
   planner's sources and fails on a minted name no spelling covers, which is
   how a rewrite that outruns the table is caught: the arity rows had `trim` at
   one argument while the grammar produced two, so a statement the engine used
   to answer became `42883`.

## Consequences

- A call whose count or argument domain no overload accepts is `42883` at plan
  time, with PostgreSQL's message shape, on every door and every arm.
- Where wadjet implements fewer overloads than PostgreSQL, the declaration is
  what this engine HAS, and the missing overload is `42883` — which is what the
  server answers for a signature it does not have either.
- The superset stays: a COLUMN of a non-text type is rendered as its text
  before a string function reads it (ADR-0012's `expr.stringInputFuncs`
  entry). A literal is not, because a literal's type is syntactic.
- One family keeps its own refusal: the TCP flag functions answer `22023
  … requires at least one TCP flag name` for an empty name list (#1018), which
  names what it wanted where a generic `42883` would not, so their declared
  minimum is the count that carries no names.
- A function whose argument grammar is not expressible as a domain — a format
  string, a regexp flag set — is still the body's business; this table says
  what may reach the body, not what the body may refuse.

## Alternatives

- **Refuse inside each body.** 383 bodies, each with its own message, none of
  them reached before the vec kernel that crashes on a bad vector.
- **Refuse in the kernel dispatcher.** It sees the vector, not the call, so it
  cannot produce PostgreSQL's message, and the per-row path never reaches it.
- **Infer the arity from each body's `args` use.** Not decidable, and it
  records the implementation rather than the contract; the documented signature
  is the contract a client writes against.
