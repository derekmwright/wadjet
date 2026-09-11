# Decimal choice runtime boxing

Source: internal/engine/expr/choice_decimal.go — decimalChoice, moved 2026-09-11 (#1026)

The DECIMAL mode of the constructs that CHOOSE BETWEEN their operands —
CASE, COALESCE, NULLIF, IFNULL, IF, GREATEST, LEAST (ADR-0024 item 2, #695).

It is the runtime half of what expr.CommonDeclType decides at plan time, and
it exists for one reason: a choice whose arms are a DECIMAL and an INTEGER
answers, on the rows the integer wins, with an INTEGER BOX. That box means
something else entirely to a DECIMAL vector — ADR-0018 §4 makes a DECIMAL
value an unscaled integer at the column's scale, so SetValue would store the
integer 100 as 1.00 and SetValueChecked refuses it outright (22003). Neither
is the value PostgreSQL answers.

So the construct rewrites its chosen box into the spelling every DECIMAL
producer here already answers with: the value's rendered TEXT, the same box
a DECIMAL COLUMN and exact arithmetic (binop_decimal.go) hand over. No
consumer of a boxed value needs teaching, and the store resolves the text at
the output vector's own scale through ParseDecimalStringChecked — exact, or
a loud 22003.

The mode is resolved once per node from the first batch, beside
BinOpNumeric's and for the same reason: an operand's type does not exist
until a batch arrives.
