-- #1677 line q fixture: one statement per line, run verbatim by wadjet and PostgreSQL 17.11
-- (postgres:17-alpine, initdb --locale=C --encoding=UTF8). p holds each key type twice (x/y, s/t,
-- d/e, m/n) so a derived table can rename, swap, compute or cast one onto the other's name; b's
-- build keys sit outside the stored column's range, so a scan filter keyed by the stored column
-- prunes every row group. pc is ten one-row files, so a filter on the scan that fills the
-- multi-consumer scan cache visibly truncates what the other consumer replays.
CREATE TABLE p (id INTEGER, x INTEGER, y INTEGER, s VARCHAR, t VARCHAR, d DATE, e DATE, m NUMERIC(10,2), n NUMERIC(10,2))
CREATE TABLE r (id INTEGER, x INTEGER, s VARCHAR, d DATE, m NUMERIC(10,2))
CREATE TABLE b (tag VARCHAR, ki INTEGER, ks VARCHAR, kd DATE, km NUMERIC(10,2))
INSERT INTO p VALUES (1, 1, 101, 's01', 't01', DATE '2020-01-01', DATE '2021-01-01', 1.50, 101.50), (2, 2, 102, 's02', 't02', DATE '2020-01-02', DATE '2021-01-02', 2.50, 102.50), (3, 3, 103, 's03', 't03', DATE '2020-01-03', DATE '2021-01-03', 3.50, 103.50), (4, 4, 104, 's04', 't04', DATE '2020-01-04', DATE '2021-01-04', 4.50, 104.50), (5, 5, 105, 's05', 't05', DATE '2020-01-05', DATE '2021-01-05', 5.50, 105.50), (6, 6, 106, 's06', 't06', DATE '2020-01-06', DATE '2021-01-06', 6.50, 106.50), (7, 7, 107, 's07', 't07', DATE '2020-01-07', DATE '2021-01-07', 7.50, 107.50), (8, 8, 108, 's08', 't08', DATE '2020-01-08', DATE '2021-01-08', 8.50, 108.50), (9, 9, 109, 's09', 't09', DATE '2020-01-09', DATE '2021-01-09', 9.50, 109.50), (10, 10, 110, 's10', 't10', DATE '2020-01-10', DATE '2021-01-10', 10.50, 110.50)
INSERT INTO r VALUES (1, 101, 't01', DATE '2021-01-01', 101.50), (2, 102, 't02', DATE '2021-01-02', 102.50), (3, 103, 't03', DATE '2021-01-03', 103.50), (4, 104, 't04', DATE '2021-01-04', 104.50), (5, 105, 't05', DATE '2021-01-05', 105.50), (6, 106, 't06', DATE '2021-01-06', 106.50), (7, 107, 't07', DATE '2021-01-07', 107.50), (8, 108, 't08', DATE '2021-01-08', 108.50), (9, 109, 't09', DATE '2021-01-09', 109.50), (10, 110, 't10', DATE '2021-01-10', 110.50)
INSERT INTO b VALUES ('plain', 3, 's03', DATE '2020-01-03', 3.50), ('other', 105, 't05', DATE '2021-01-05', 105.50), ('comp', 11, 'S05', DATE '2020-01-11', 11.50)
CREATE TABLE pc (id INTEGER, x INTEGER)
INSERT INTO pc VALUES (1, 1)
INSERT INTO pc VALUES (2, 2)
INSERT INTO pc VALUES (3, 3)
INSERT INTO pc VALUES (4, 4)
INSERT INTO pc VALUES (5, 5)
INSERT INTO pc VALUES (6, 6)
INSERT INTO pc VALUES (7, 7)
INSERT INTO pc VALUES (8, 8)
INSERT INTO pc VALUES (9, 9)
INSERT INTO pc VALUES (10, 10)
