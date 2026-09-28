#!/usr/bin/env python3
# Measures pg_typeof() of every arithmetic operator and double-precision
# function over every operand category on PostgreSQL 17.11, into
# pg_typeof.tsv (expression TAB type). Expressions PostgreSQL refuses are left
# out. Fixture: m(i int, b bigint, d numeric(10,2), f float8).
#
#   docker run -d --name wadjet-pg-ir --memory=4g -e POSTGRES_USER=wadjet \
#     -e POSTGRES_PASSWORD=wadjet -e POSTGRES_DB=wadjet_oracle \
#     -p 127.0.0.1:57700:5432 postgres:17-alpine -c fsync=off
#   python3 gen_typeof.py
import os
import subprocess

ops = ["+", "-", "*", "/", "%"]
opnds = ["5", "i", "b", "2.0", "d", "f", "2.0::float8", "(5/2.0)", "SQRT(d)"]
exprs = [f"{l} {o} {r}" for o in ops for l in opnds for r in opnds]
exprs += [f"-{u}" if not u[0].isdigit() else f"-({u})" for u in opnds]
fns1 = ["abs", "ceil", "ceiling", "floor", "round", "trunc", "sign", "sqrt", "exp", "ln", "log", "log10",
        "cbrt", "degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan"]
args = ["5", "i", "2.0", "d", "f", "(5/2.0)", "SQRT(d)"]
exprs += [f"{fn.upper()}({a})" for fn in fns1 for a in args]
fns2 = ["power", "pow", "mod", "log", "round", "trunc", "atan2"]
exprs += [f"{fn.upper()}({a}, {b})" for fn in fns2 for a in args for b in ["2", "1.0", "f"]]
exprs += ["PI()", "RANDOM()", "COALESCE(5/2.0, 1)", "COALESCE(5/2.0, f)", "NULLIF(5/2.0, 1)", "GREATEST(5/2.0, 1)",
          "LEAST(5/2.0, d)", "CASE WHEN true THEN 5/2.0 ELSE 1 END", "CASE WHEN true THEN SQRT(d) ELSE f END",
          "EXTRACT(SECOND FROM TIMESTAMP '2020-01-01 00:00:02.5')",
          "EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:02.5')",
          "DATE_PART('second', TIMESTAMP '2020-01-01 00:00:02.5')",
          "CAST(5/2.0 AS NUMERIC)", "(5/2.0)::numeric", "CAST(f AS NUMERIC)", "5/2.0::float8"]
# the CASE family over every pair of operand categories: NULLIF's result is its
# first argument promoted by the `=` its comparison resolves to, so
# NULLIF(numeric, float8) is float8 like COALESCE and CASE are
exprs += [f"{fn}({l}, {r})" for fn in ["NULLIF", "COALESCE", "GREATEST", "LEAST"] for l in opnds for r in opnds]
exprs += [f"CASE WHEN i > 0 THEN {l} ELSE {r} END" for l in opnds for r in opnds]

sql = ["SET statement_timeout='30s';", "DROP TABLE IF EXISTS m;",
       "CREATE TABLE m(i int, b bigint, d numeric(10,2), f float8);",
       "INSERT INTO m VALUES (5, 5, 6.25, 6.25);", "\\set ON_ERROR_STOP off"]
for e in exprs:
    sql.append(f"SELECT 'ROW', '{e.replace(chr(39), chr(39) * 2)}', pg_typeof({e})::text FROM m;")
out = subprocess.run(["docker", "exec", "-i", os.environ.get("PG_CONTAINER", "wadjet-pg-ir"), "psql", "-U", "wadjet", "-d", "wadjet_oracle",
                      "-At", "-F", "\t"], input="\n".join(sql), capture_output=True, text=True).stdout
got = {}
for line in out.split("\n"):
    if line.startswith("ROW\t"):
        _, e, t = line.split("\t")
        got[e] = t
with open("pg_typeof.tsv", "w") as f:
    for e in exprs:
        if e in got:
            f.write(f"{e}\t{got[e]}\n")
