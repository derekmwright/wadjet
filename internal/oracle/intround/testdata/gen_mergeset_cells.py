#!/usr/bin/env python3
# Generates the MERGE SET / INSERT-action expression table of intround and
# measures PostgreSQL 17.11's answer for every cell, into mergeset_cells.json:
# one object per cell with its name, its statements, and either the stored
# rows "id:n4:n8 …" (w) or the SQLSTATE PostgreSQL raises (e).
#
# The table is every expression FORM a MERGE action can assign (a bare target
# column, a bare / qualified source column, arithmetic, five scalar-subquery
# shapes, CASE / COALESCE / NULLIF, an array element, two CASTs, a literal, an
# aggregate, a window function; the parameter lives in the pgwire gate, the
# only door that binds one) × the source's type (float8, numeric, integer,
# text) × the action (WHEN MATCHED UPDATE SET, WHEN NOT MATCHED INSERT VALUES)
# × the source relation (the catalog table, a subquery over it), each cell
# assigning into an INTEGER (n4) and a BIGINT (n8) at once. Source values
# 2.5, 0.5, -2.5, -0.5: a float8 rounds them half to even (2 0 -2 0), a
# numeric half away from zero (3 1 -3 -1).
#
#   docker run -d --name wadjet-pg-ir4 --memory=4g -e POSTGRES_USER=wadjet \
#     -e POSTGRES_PASSWORD=wadjet -e POSTGRES_DB=wadjet_oracle \
#     -e POSTGRES_INITDB_ARGS="--locale=C --encoding=UTF8" \
#     -p 127.0.0.1:57760:5432 postgres:17-alpine -c fsync=off
#   PG_CONTAINER=wadjet-pg-ir4 python3 gen_mergeset_cells.py
import json, os, re, subprocess

# The engine spells DOUBLE where PostgreSQL spells DOUBLE PRECISION; every
# other statement is one text on both (intround.MergeSetFixture).
PG_FIXTURE = [
    "DROP TABLE IF EXISTS s0, s, t",
    "CREATE TABLE s0 (id INTEGER, f DOUBLE PRECISION, d NUMERIC(10,2), i INTEGER, x TEXT)",
    "INSERT INTO s0 VALUES (1, 2.5, 2.50, 5, '2.5'), (2, 0.5, 0.50, 1, '0.5'), "
    "(3, -2.5, -2.50, -5, '-2.5'), (4, -0.5, -0.50, -1, '-0.5')",
    "CREATE TABLE s AS SELECT id, f, d, i, x, ARRAY[f] AS af, ARRAY[d] AS ad, ARRAY[i] AS ai, ARRAY[x] AS ax FROM s0",
    "CREATE TABLE t (id INTEGER, n4 INTEGER, n8 BIGINT, tf DOUBLE PRECISION, td NUMERIC(10,2), ti INTEGER, tx TEXT)",
    "INSERT INTO t SELECT id, 0, 0, f, d, i, x FROM s0",
]

# source type: (name, source column, target column of the same type, array
# column, CAST type name, a literal of the type, NULLIF's never-equal operand)
TYPES = [
    ("f8", "f", "tf", "af", "DOUBLE PRECISION", "CAST('2.5' AS DOUBLE PRECISION)", "99"),
    ("num", "d", "td", "ad", "NUMERIC", "2.5", "99"),
    ("int", "i", "ti", "ai", "INTEGER", "5", "99"),
    ("text", "x", "tx", "ax", "TEXT", "'2.5'", "'zz'"),
]

def forms(c, tc, ac, tname, lit, never):
    return [
        ("bare-target", tc),
        ("bare-source", c),
        ("qualified", f"s.{c}"),
        ("arith", f"s.{c} * 1"),
        ("subq-uncorr", f"(SELECT s2.{c} FROM s s2 WHERE s2.id = 1)"),
        ("subq-corr-source", f"(SELECT s2.{c} FROM s s2 WHERE s2.id = s.id)"),
        ("subq-corr-target", f"(SELECT s2.{c} FROM s s2 WHERE s2.id = t.id)"),
        ("subq-agg", f"(SELECT MAX(s2.{c}) FROM s s2)"),
        ("subq-cast", f"(SELECT CAST(s2.f AS {tname}) FROM s s2 WHERE s2.id = s.id)"),
        ("subq-lit", f"(SELECT {lit})"),
        ("case", f"CASE WHEN s.id > 0 THEN s.{c} END"),
        ("coalesce", f"COALESCE(s.{c}, s.{c})"),
        ("nullif", f"NULLIF(s.{c}, {never})"),
        ("element", f"s.{ac}[1]"),
        ("cast-f8", f"CAST(s.{c} AS DOUBLE PRECISION)"),
        ("cast-numeric", f"CAST(s.{c} AS NUMERIC)"),
        ("literal", lit),
        ("aggregate", f"MAX(s.{c})"),
        ("window", f"MAX(s.{c}) OVER ()"),
    ]

RELATIONS = [("catalog", "s"), ("subquery", "(SELECT * FROM s) s")]

def cells():
    out = []
    for tn, c, tc, ac, tname, lit, never in TYPES:
        for fn, e in forms(c, tc, ac, tname, lit, never):
            for rn, rel in RELATIONS:
                m = f"MERGE INTO t USING {rel} ON t.id = s.id "
                out.append({"n": f"merge-set/{fn}/{tn}/{rn}/update",
                            "s": ["UPDATE t SET n4 = 0, n8 = 0",
                                  m + f"WHEN MATCHED THEN UPDATE SET n4 = {e}, n8 = {e}"]})
                out.append({"n": f"merge-set/{fn}/{tn}/{rn}/insert",
                            "s": ["DELETE FROM t",
                                  m + f"WHEN NOT MATCHED THEN INSERT (id, n4, n8) VALUES (s.id, {e}, {e})"]})
    return out

def pg_run(cs):
    lines = ["SET statement_timeout='30s';", "\\set ON_ERROR_STOP off"]
    for n, c in enumerate(cs):
        lines += [s + ";" for s in PG_FIXTURE]
        lines.append(f"SELECT '@@CELL {n}';")
        for s in c["s"]:
            lines.append(s + ";")
            lines.append(f"\\if :ERROR\n\\echo @@ERR {n} :LAST_ERROR_SQLSTATE :LAST_ERROR_MESSAGE\n\\endif")
        lines.append(f"SELECT '@@ROWS {n}', string_agg(id || ':' || coalesce(n4::text, 'NULL') || ':' || "
                     f"coalesce(n8::text, 'NULL'), ' ' ORDER BY id) FROM t;")
    p = subprocess.run(["docker", "exec", "-i", os.environ.get("PG_CONTAINER", "wadjet-pg-ir4"), "psql",
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
            errs.setdefault(int(m.group(1)), (m.group(2), m.group(3)))
        m = re.match(r"@@ROWS (\d+)\t(.*)", line)
        if m:
            rows[int(m.group(1))] = m.group(2)
    for n, c in enumerate(cs):
        if n in errs:
            c["e"] = errs[n][0]
            c["m"] = errs[n][1]
        else:
            c["w"] = rows[n]
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mergeset_cells.json"), "w") as f:
        f.write("[\n" + ",\n".join(json.dumps(c) for c in cs) + "\n]\n")
    print(len(cs), "cells;", sum(1 for c in cs if "e" in c), "refused by PostgreSQL")
