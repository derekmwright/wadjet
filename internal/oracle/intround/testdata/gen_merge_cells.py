#!/usr/bin/env python3
# Generates the MERGE-source half of the intround table and measures
# PostgreSQL 17.11's stored rows for every cell, into merge_cells.json (one
# object per cell: its name, its statements, and the rows "id:n4:n8 …").
#
# The cells assign a value that arrives through the MERGE's SOURCE — a derived
# table, a derived table over a derived table, a CTE inside the source, a set
# operation, a join, a VALUES list — with UPDATE and INSERT actions, qualified
# and bare references, into the INTEGER (n4) and BIGINT (n8) columns of t_m.
# Every source row carries (E) + k for k in {-3, -1, 0, 1, 2, 3} and E = 0.5,
# so the float8 sources (half to even) and the numeric ones (half away from
# zero) disagree on -2.5, -0.5, 0.5 and 2.5.
#
#   docker run -d --name wadjet-pg-ir3 --memory=4g -e POSTGRES_USER=wadjet \
#     -e POSTGRES_PASSWORD=wadjet -e POSTGRES_DB=wadjet_oracle \
#     -p 127.0.0.1:57740:5432 postgres:17-alpine -c fsync=off
#   PG_CONTAINER=wadjet-pg-ir3 python3 gen_merge_cells.py
import json, os, re, subprocess

SRC_ROWS = ("(1, -3, 2, 0.50, 0.5), (2, -1, 2, 0.50, 0.5), (3, 0, 2, 0.50, 0.5), (4, 1, 2, 0.50, 0.5), "
            "(5, 2, 2, 0.50, 0.5), (6, 3, 2, 0.50, 0.5), (7, 0, 2, 0.50, 0.5), (8, 0, 2, 0.50, 0.5)")

# (name, expression over src's columns evaluating to 0.5)
EXPRS = [
    ("f8col", "f"),
    ("f8lit", "CAST(0.5 AS DOUBLE PRECISION)"),
    ("sqrt_f8col", "SQRT(f * f)"),
    ("numcol", "d"),
    ("numlit", "0.5"),
    ("div_numcol", "(i / 4.0)"),
    ("cast_f8col_numeric", "CAST(f AS NUMERIC)"),
]

def qualify(e, q):
    return re.sub(r"\b([fdi])\b", q + r".\1", e)

# the source relation, aliased s2(id, v)
def sources(e):
    return [
        ("derived", f"(SELECT id, {e} + k AS v FROM src) s2"),
        ("derived-nested", f"(SELECT id, v FROM (SELECT id, {e} + k AS v FROM src) x) s2"),
        ("cte-in-source", f"(WITH c AS (SELECT id, {e} + k AS v FROM src) SELECT id, v FROM c) s2"),
        ("union-all", f"(SELECT id, {e} + k AS v FROM src WHERE id <= 4 UNION ALL "
                      f"SELECT id, {e} + k FROM src WHERE id > 4) s2"),
        # every column qualified: an unqualified column beside `a.k` in one
        # projection is a separate, recorded defect's shape (the projection's
        # declaration strips the qualifier), not this table's subject
        ("join", f"(SELECT a.id, {qualify(e, 'a')} + a.k AS v FROM src a "
                 f"JOIN (SELECT id AS bid FROM src) b ON a.id = b.bid) s2"),
    ]

HALVES = ["-2.5", "-0.5", "0.5", "1.5", "2.5", "3.5", "0.5", "0.5"]

def values_sources():
    num = ", ".join(f"({i + 1}, {h})" for i, h in enumerate(HALVES))
    f8 = ", ".join(f"({i + 1}, CAST({h} AS DOUBLE PRECISION))" for i, h in enumerate(HALVES))
    return [
        ("values/numlit", f"(VALUES {num}) AS s2(id, v)"),
        ("values/f8lit", f"(VALUES {f8}) AS s2(id, v)"),
        ("values-in-select/numlit", f"(SELECT id, v FROM (VALUES {num}) AS x(id, v)) s2"),
        ("values-in-select/f8lit", f"(SELECT id, v FROM (VALUES {f8}) AS x(id, v)) s2"),
    ]

ACTIONS = [
    ("update", "UPDATE t_m SET n4 = NULL, n8 = NULL", "WHEN MATCHED THEN UPDATE SET n4 = s2.v, n8 = s2.v"),
    ("update-bare", "UPDATE t_m SET n4 = NULL, n8 = NULL", "WHEN MATCHED THEN UPDATE SET n4 = v, n8 = v"),
    ("insert", "DELETE FROM t_m", "WHEN NOT MATCHED THEN INSERT (id, n4, n8) VALUES (s2.id, s2.v, s2.v)"),
    ("insert-bare", "DELETE FROM t_m", "WHEN NOT MATCHED THEN INSERT (id, n4, n8) VALUES (id, v, v)"),
    # an expression OVER the source, not a reference to it
    ("update-computed", "UPDATE t_m SET n4 = NULL, n8 = NULL", "WHEN MATCHED THEN UPDATE SET n4 = s2.v * 1, n8 = s2.v * 1"),
]

def cells():
    rels = []
    for en, e in EXPRS:
        for sn, s in sources(e):
            rels.append((f"{sn}/{en}", "", s))
        # a CTE named by the statement itself, not inside the source
        rels.append((f"with-merge/{en}", f"WITH c AS (SELECT id, {e} + k AS v FROM src) ", "c s2"))
    rels += [(n, "", s) for n, s in values_sources()]
    out = []
    for rn, prefix, rel in rels:
        for an, reset, act in ACTIONS:
            out.append({"n": f"merge-source/{rn}/{an}",
                        "s": [reset, f"{prefix}MERGE INTO t_m USING {rel} ON t_m.id = s2.id {act}"]})
    return out + array_cells()

# An ARRAY column's element read by a subscript, on every door that reads a
# column: t_a holds ARRAY[f + k] (float8[]) and ARRAY[d + k] (numeric[]).
# (intround.Fixture creates it the same way)
ARRAY_TABLE = "CREATE TABLE t_a AS SELECT id, ARRAY[f + k] AS af, ARRAY[d + k] AS ad, id * 0 AS n4, id * 0 AS n8 FROM src"

def array_cells():
    out = []
    for an in ("af", "ad"):
        for door, stmts, read in [
            ("update", ["UPDATE t_a SET n4 = NULL, n8 = NULL", f"UPDATE t_a SET n4 = {an}[1], n8 = {an}[1]"], "t_a"),
            ("merge", ["UPDATE t_m SET n4 = NULL, n8 = NULL",
                       f"MERGE INTO t_m USING t_a ON t_m.id = t_a.id WHEN MATCHED THEN UPDATE SET n4 = t_a.{an}[1], n8 = t_a.{an}[1]"], "t_m"),
            ("merge-derived", ["UPDATE t_m SET n4 = NULL, n8 = NULL",
                               f"MERGE INTO t_m USING (SELECT id, {an} FROM t_a) s2 ON t_m.id = s2.id "
                               f"WHEN MATCHED THEN UPDATE SET n4 = s2.{an}[1], n8 = s2.{an}[1]"], "t_m"),
            ("select", ["DELETE FROM t_s", f"INSERT INTO t_s (id, n4, n8) SELECT id, {an}[1], {an}[1] FROM t_a"], "t_s"),
        ]:
            out.append({"n": f"array-element/{an}/{door}", "s": stmts, "r": read})
    return out

def pg_run(cs):
    lines = ["SET statement_timeout='30s';", "\\set ON_ERROR_STOP off",
             "DROP TABLE IF EXISTS src, t_m, t_s, t_a;",
             "CREATE TABLE src (id INTEGER, k INTEGER, i INTEGER, d NUMERIC(10,2), f DOUBLE PRECISION);",
             f"INSERT INTO src VALUES {SRC_ROWS};",
             "CREATE TABLE t_m (id INTEGER, n4 INTEGER, n8 BIGINT);",
             "CREATE TABLE t_s (id INTEGER, n4 INTEGER, n8 BIGINT);",
             ARRAY_TABLE + ";"]
    for n, c in enumerate(cs):
        lines.append("DELETE FROM t_m;")
        lines.append("INSERT INTO t_m (id) SELECT id FROM src;")
        lines.append(f"SELECT '@@CELL {n}';")
        for s in c["s"]:
            lines.append(s + ";")
            lines.append(f"\\if :ERROR\n\\echo @@ERR {n} :LAST_ERROR_SQLSTATE :LAST_ERROR_MESSAGE\n\\endif")
        lines.append(f"SELECT '@@ROWS {n}', string_agg(id || ':' || coalesce(n4::text, 'NULL') || ':' || "
                     f"coalesce(n8::text, 'NULL'), ' ' ORDER BY id) FROM {c.get('r', 't_m')};")
    p = subprocess.run(["docker", "exec", "-i", os.environ.get("PG_CONTAINER", "wadjet-pg-ir3"), "psql",
                        "-U", "wadjet", "-d", "wadjet_oracle", "-At", "-F", "\t", "-v", "VERBOSITY=terse"],
                       input="\n".join(lines), capture_output=True, text=True)
    return p.stdout

if __name__ == "__main__":
    cs = cells()
    out = pg_run(cs)
    errs, rows = {}, {}
    for line in out.split("\n"):
        m = re.match(r"@@ERR (\d+) (\S+) (.*)", line)
        if m:
            errs.setdefault(int(m.group(1)), m.group(2) + " " + m.group(3))
        m = re.match(r"@@ROWS (\d+)\t(.*)", line)
        if m:
            rows[int(m.group(1))] = m.group(2)
    kept = []
    for n, c in enumerate(cs):
        if n in errs:
            print("PG refuses", c["n"], errs[n])
            continue
        c["w"] = rows[n]
        kept.append(c)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "merge_cells.json"), "w") as f:
        f.write("[\n" + ",\n".join(json.dumps(c) for c in kept) + "\n]\n")
    print(len(cs), "cells;", len(kept), "kept")
