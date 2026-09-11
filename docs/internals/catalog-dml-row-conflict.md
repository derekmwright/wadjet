# Catalog dml row conflict

Source: internal/storage/catalog/dml_commit.go — var ErrDMLRowSuperseded = errors.New("a row this statement supersedes was already superseded by another statement"), moved 2026-09-11 (#1026)

ErrDMLRowSuperseded reports that a DML statement's manifest change cannot be
committed because ANOTHER STATEMENT has already superseded a row this one is
about to supersede.

It is the row-level half of the same rule ErrDMLTargetMoved states over
files, and it is what #691 left open — ADR-0030 said so in its own words:
"Two writers racing each other … both succeed, and the second one's markers
are valid because the files did not move … Closing it needs a conflict rule
over ROWS, which this record does not decide." This is that rule.

The window is the ordinary one: each statement reads the manifest, scans the
files it names, records WHICH ROW OF WHICH FILE it affected, and commits at
the end. Two statements over the same row both see it live, both write a
replacement, and both mark the copy they read — so the manifest ends up
naming BOTH replacements and the key is present twice. Measured on
v0.18.22, `UPDATE … n = 111 WHERE id = 1` against `UPDATE … n = 222 WHERE
id = 1`:

	table afterwards:  1:111:a  1:222:a  2:20:b  3:30:c
	both statements:   UPDATE 1

The same window resurrects a deleted row (an UPDATE whose scan predates a
concurrent DELETE re-publishes the row it read) and reports `DELETE 1` over
a row that is still readable.

A statement that sees this redoes itself against the manifest that replaced
the one it read, exactly as ErrDMLTargetMoved makes it redo; the outcome is
then one of the two serial orders PostgreSQL could have produced.
