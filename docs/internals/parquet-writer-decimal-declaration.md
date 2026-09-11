# Parquet writer decimal declaration

Source: internal/storage/parquet/file_writer.go — func checkDecimalDeclaration(precision, scale int) (int32, int32, error) {, moved 2026-09-11 (#1026)
Superseded: Only Precision == 0 is the unconstrained sentinel; negative precision is explicitly refused by checkDecimalDeclaration.

checkDecimalDeclaration returns the (precision, scale) a DECIMAL column's
footer annotation will carry, or the reason the column cannot be written.

`Precision <= 0` is this package's documented "unconstrained" sentinel and
becomes 38 (decimalEffectivePrecision), so the FILE's precision — not the
field — is what the scale is measured against, and it is what a foreign
reader will apply. Measured at f415faba, all reached through NewWriter with
Close returning nil (#969):

	DECIMAL(9,-1)  a row of "1.25" read back as 0, and pyarrow refuses the
	               file: "Scale must be a non-negative integer that does not
	               exceed precision for Decimal logical type"
	DECIMAL(4,9)   an EMPTY file still carries the annotation, and pyarrow
	               refuses it the same way
	DECIMAL(0,40)  the file declares decimal(38,40); pyarrow refuses it
	DECIMAL(50,2)  the file declares decimal(38,2) — wadjet reads back its own
	               output as DECIMAL(38,2), not the DECIMAL(50,2) asked for
	DECIMAL(-3,2)  the same silent re-declaration

ParseDecimalParams enforces 1 <= precision <= 38 and 0 <= scale <= precision
for DDL; a Column built in Go bypassed it entirely. Scale == precision is
legal and stays legal (pyarrow opens DECIMAL(38,38)).
