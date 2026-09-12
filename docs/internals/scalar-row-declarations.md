# Fixed-schema scalar ROW declarations

A registry function declares fixed fields with `expr.RetRow(fields)`.
`DeclType.Schema` retains them during inference, and physical `declTypeParts`
returns a complete `parquet.Column`, including the fields needed to allocate
child vectors. Projection, group key, window key, hidden sort key, aggregate
input, set-operation arm, worker and gather materializations carry those fields.
The synthetic aggregate key's parallel metadata declaration carries them too.

A derived ROW field binds its parent through the aggregate input rename
resolver before extracting the field. This repairs #1055 for stored and
computed ROWs. `(function(arg)).field` uses the existing `row_field` evaluator
and resolves the field's declaration from the function's fixed schema.

`TestFixedRowScalarInEveryPosition` and its wire twin cover stored and computed
ROWs across five configurations, checking values, five declared fields, and
both wire formats. DISTINCT and GROUP BY force real aggregate drains on the
512 KiB arm. The scalar-subquery cell retains the base local route: the scalar
producer transport accepts only values with a lossless scalar literal spelling.
Function-argument subqueries retain their base route too: that resolver does
not descend into function arguments.
Its ROW value and declaration now survive that route. Every other position
asserts zero local-route increments.

`RetRow(nil)` retains the prior TEXT disposition. The review's same-body
projection/DISTINCT/GROUP BY control checks values on that disposition against
a fixed declaration. An untyped container has no declared fields a derived
table can publish. ARRAY/MAP scalar declarations remain on the existing #1017
disposition; their element schema is a separate class. The existing ROW wire
convention remains text OID 25 (PostgreSQL uses record OID 2249).


`TestFixedRowDeclarationMaterializationConsumers` separately checks the security
projection and aggregate-input metadata: COUNT may inspect a ROW's parent NULL
bit without reading its children, so a value-only count cannot prove that schema.
A fixed function declaration rejects an unknown field with 42703 and field
notation on a fixed scalar result with 42809, including over empty inputs. The
binder and compiler share that declaration check.
