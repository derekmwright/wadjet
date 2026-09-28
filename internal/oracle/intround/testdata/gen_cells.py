#!/usr/bin/env python3
# Generates the intround table and measures PostgreSQL 17.11's stored value for
# every cell, into cells.json (one line per source spelling: its doors, its
# expression or its INSERT … SELECT bodies, and the n4 value per fixture row;
# n8 holds the same). Needs the container gen_typeof.py names:
#
#   python3 gen_cells.py
#
# Every template evaluates to exactly 0.5 over the fixture (i = 2, d = 0.50,
# f = 0.5); intround.go spells the statements from it exactly as pg_run did.
# Templates: every expression evaluates to exactly 0.5 over the fixture
# (src/t_u: i = 2, d = 0.50, f = 0.5). (name, expr, uses_columns)
H_OPERANDS = [  # half-valued operands, one per category spelling
    ("numlit", "0.5", False),
    ("numcol", "d", True),
    ("f8lit", "0.5::float8", False),
    ("f8col", "f", True),
    ("div_numlit", "(1 / 2.0)", False),          # float-carried numeric
    ("div_numcol", "(i / 4.0)", True),           # int col / numeric lit
    ("sqrt_numlit", "SQRT(0.25)", False),        # float-carried numeric
    ("sqrt_numcol", "SQRT(d * d)", True),
    ("sqrt_f8col", "SQRT(f * f)", True),
    ("cbrt_numlit", "CBRT(0.125)", False),       # float8 in PostgreSQL whatever the arg
    ("cast_f8col_numeric", "CAST(f AS NUMERIC)", True),
    ("cast_numlit_f8", "CAST(0.5 AS DOUBLE PRECISION)", False),
    ("extract_epoch", "EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00.5')", False),
]
I_ZERO = [("intlit", "0", False), ("intcol", "(i - 2)", True)]
I_ONE = [("intlit", "1", False), ("intcol", "(i - 1)", True)]

def T():
    out = []
    add = lambda n, e, c: out.append((n, e, c))
    # ---- binary operators, every operand category pairing ----
    for hn, h, hc in H_OPERANDS:
        for zn, z, zc in I_ZERO:
            add(f"op+/{hn}+{zn}", f"{h} + {z}", hc or zc)
            add(f"op+/{zn}+{hn}", f"{z} + {h}", hc or zc)
            add(f"op-/{hn}-{zn}", f"{h} - {z}", hc or zc)
        for on, o, oc in I_ONE:
            add(f"op-/{on}-{hn}", f"{o} - {h}", hc or oc)
            add(f"op*/{hn}*{on}", f"{h} * {o}", hc or oc)
            add(f"op*/{on}*{hn}", f"{o} * {h}", hc or oc)
            add(f"op//{hn}/{on}", f"{h} / {o}", hc or oc)
            add(f"op//{on}/{hn}x4", f"{o} / ({h} * 4)", hc or oc)
        add(f"op%/{hn}+3%3", f"({h} + 3) % 3", hc)
        add(f"unary-/{hn}", f"-({h} - 1)", hc)
    for an, a, ac in H_OPERANDS:
        for bn, b, bc in H_OPERANDS:
            add(f"op+/{an}+{bn}-0.5", f"{a} + {b} - 0.5", ac or bc)
            add(f"op*/{an}*{bn}*2", f"{a} * {b} * 2", ac or bc)
            add(f"op//{an}/{bn}/2", f"{a} / {b} / 2", ac or bc)
    add("op//literal/1/2.0", "1 / 2.0", False)
    add("op//literal/2.5/5", "2.5 / 5", False)
    add("op//intcol/intlit_int_then_numeric", "i / 4 + 0.5", True)  # integer division 0, then numeric
    # ---- functions: identity args in every category, then +/- a numeric 0.5 ----
    ident = {  # fn -> (value-producing args per category, offset to 0.5)
        "SQRT": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.0"), ("numcol", "(d * 2)"), ("f8col", "(f * 2)"), ("divnum", "(2 / 2.0)")],
        "EXP": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "LN": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.0"), ("numcol", "(d * 2)"), ("f8col", "(f * 2)"), ("divnum", "(2 / 2.0)")],
        "LOG": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.0"), ("numcol", "(d * 2)"), ("f8col", "(f * 2)"), ("divnum", "(2 / 2.0)")],
        "LOG10": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.0"), ("numcol", "(d * 2)"), ("f8col", "(f * 2)"), ("divnum", "(2 / 2.0)")],
        "CEIL": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "0.5"), ("numcol", "d"), ("f8col", "f"), ("divnum", "(1 / 2.0)")],
        "CEILING": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "0.5"), ("numcol", "d"), ("f8col", "f"), ("divnum", "(1 / 2.0)")],
        "FLOOR": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.5"), ("numcol", "(d + 1)"), ("f8col", "(f + 1)"), ("divnum", "(3 / 2.0)")],
        "ROUND": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "0.7"), ("numcol", "(d + 0.2)"), ("f8col", "(f + 0.2)"), ("divnum", "(3 / 4.0)")],
        "TRUNC": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.5"), ("numcol", "(d + 1)"), ("f8col", "(f + 1)"), ("divnum", "(3 / 2.0)")],
        "SIGN": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "0.5"), ("numcol", "d"), ("f8col", "f"), ("divnum", "(1 / 2.0)")],
        "ABS": [("intlit", "-1"), ("intcol", "(1 - i)"), ("numlit", "-1.0"), ("numcol", "(0 - d * 2)"), ("f8col", "(0 - f * 2)"), ("divnum", "(-2 / 2.0)")],
        "CBRT": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.0"), ("numcol", "(d * 2)"), ("f8col", "(f * 2)"), ("divnum", "(2 / 2.0)")],
        "DEGREES": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "RADIANS": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "SIN": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "COS": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "TAN": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "ASIN": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
        "ACOS": [("intlit", "1"), ("intcol", "(i - 1)"), ("numlit", "1.0"), ("numcol", "(d * 2)"), ("f8col", "(f * 2)"), ("divnum", "(2 / 2.0)")],
        "ATAN": [("intlit", "0"), ("intcol", "(i - 2)"), ("numlit", "0.0"), ("numcol", "(d - 0.5)"), ("f8col", "(f - 0.5)"), ("divnum", "(0 / 2.0)")],
    }
    # value of fn(identity arg); offset brings it to 0.5
    fval = {"SQRT": 1, "EXP": 1, "LN": 0, "LOG": 0, "LOG10": 0, "CEIL": 1, "CEILING": 1, "FLOOR": 1, "ROUND": 1,
            "TRUNC": 1, "SIGN": 1, "ABS": 1, "CBRT": 1, "DEGREES": 0, "RADIANS": 0, "SIN": 0, "COS": 1, "TAN": 0,
            "ASIN": 0, "ACOS": 0, "ATAN": 0}
    for fn, args in ident.items():
        for cn, a in args:
            off = "+ 0.5" if fval[fn] == 0 else "- 0.5"
            # the float8 offset spelling too, so a numeric function beside a float8 operand is float8
            add(f"fn/{fn}({cn}){off.replace(' ', '')}", f"{fn}({a}) {off}", "i" in a or "d" in a or "f" in a)
            # and with an integer offset only, so the function's own category decides alone
            off_i = "* 1"
            if fval[fn] == 1:
                add(f"fn/{fn}({cn})/2", f"{fn}({a}) / 2", "i" in a or "d" in a or "f" in a)
    for fn in ["POWER", "POW"]:
        for (n, e, c) in [("h,intlit1", "{h}, 1"), ("h,numlit1", "{h}, 1.0"), ("h,f8lit1", "{h}, 1::float8")] if False else []:
            pass
        for hn, h, hc in H_OPERANDS:
            add(f"fn/{fn}({hn},intlit)", f"{fn}({h}, 1)", hc)
            add(f"fn/{fn}({hn},numlit)", f"{fn}({h}, 1.0)", hc)
        add(f"fn/{fn}(intlit,intlit)", f"{fn}(2, -1)", False)
        add(f"fn/{fn}(intcol,intlit)", f"{fn}(i, -1)", True)
        add(f"fn/{fn}(intlit,numlit)", f"{fn}(2, -1.0)", False)
        add(f"fn/{fn}(intcol,numcol)", f"{fn}(i, d - 1.5)", True)
        add(f"fn/{fn}(numlit,f8col)", f"{fn}(4.0, f) - 1.5", True)
        add(f"fn/{fn}(intlit,f8lit)", f"{fn}(4, 0.5::float8) - 1.5", False)
    for hn, h, hc in H_OPERANDS:
        if hn in ("f8lit", "f8col", "sqrt_f8col", "cbrt_numlit", "cast_numlit_f8"):
            continue  # PostgreSQL has no round/trunc(float8, int) and no float8 mod: 42883
        add(f"fn/ROUND({hn}+0.04,1)", f"ROUND({h} + 0.04, 1)", hc)
        add(f"fn/TRUNC({hn}+0.09,1)", f"TRUNC({h} + 0.09, 1)", hc)
        add(f"fn/MOD({hn}+3,3)", f"MOD({h} + 3, 3)", hc)
        add(f"fn/ABS(-{hn})", f"ABS(0 - {h})", hc)
    for hn, h, hc in [x for x in H_OPERANDS if x[0] in ("f8lit", "f8col", "sqrt_f8col", "cbrt_numlit", "cast_numlit_f8")]:
        add(f"fn/ABS(-{hn})", f"ABS(0 - {h})", hc)
    add("fn/ROUND(intlit,1)", "ROUND(1, 1) - 0.5", False)
    add("fn/ROUND(intcol,1)", "ROUND(i - 1, 1) - 0.5", True)
    add("fn/TRUNC(intcol,1)", "TRUNC(i - 1, 1) - 0.5", True)
    add("fn/MOD(intcol,intlit)", "MOD(i + 1, 2) - 0.5", True)
    add("fn/MOD(intlit,intlit)/2", "MOD(7, 2) / 2", False)
    add("fn/LOG(intlit,intlit)", "LOG(4, 2)", False)
    add("fn/LOG(numlit,intlit)", "LOG(4.0, 2)", False)
    add("fn/LOG(intcol,intcol)", "LOG(i + 2, i)", True)
    add("fn/LOG(numcol,numcol)", "LOG(d * 8, d * 4)", True)
    add("fn/ATAN2(numlit,numlit)", "ATAN2(0.0, 1.0) + 0.5", False)
    add("fn/ATAN2(intcol,f8col)", "ATAN2(i - 2, f) + 0.5", True)
    add("fn/PI()", "PI() - PI() + 0.5", False)
    add("fn/RANDOM()", "RANDOM() * 0 + 0.5", False)
    add("fn/DATE_PART(epoch)", "DATE_PART('epoch', TIMESTAMP '1970-01-01 00:00:00.5')", False)
    # ---- the CASE family ----
    for hn, h, hc in H_OPERANDS:
        add(f"fold/COALESCE({hn},intlit)", f"COALESCE({h}, 0)", hc)
        add(f"fold/COALESCE(NULL,{hn})", f"COALESCE(NULL, {h})", hc)
        add(f"fold/COALESCE({hn},f8col)", f"COALESCE({h}, f)", True)
        add(f"fold/COALESCE({hn},numcol)", f"COALESCE({h}, d)", True)
        add(f"fold/NULLIF({hn},intlit)", f"NULLIF({h}, 1)", hc)
        add(f"fold/NULLIF(intcol,{hn})", f"NULLIF(i - 1, {h}) - 0.5", True)
        add(f"fold/GREATEST({hn},intlit)", f"GREATEST({h}, 0)", hc)
        add(f"fold/GREATEST({hn},f8col-1)", f"GREATEST({h}, f - 1)", True)
        add(f"fold/LEAST({hn},intcol)", f"LEAST({h}, i)", True)
        add(f"fold/LEAST({hn},numlit)", f"LEAST({h}, 1.5)", hc)
        add(f"fold/CASE({hn},intlit)", f"CASE WHEN 1 = 1 THEN {h} ELSE 0 END", hc)
        # a float8 second operand: NULLIF returns its first argument PROMOTED by
        # the `float8 = float8` its comparison resolves to, so a numeric first
        # argument comes back float8 (pg_typeof, 17.11)
        add(f"fold/NULLIF({hn},f8col+9)", f"NULLIF({h}, f + 9)", True)
        add(f"fold/NULLIF({hn},f8lit)", f"NULLIF({h}, CAST(9 AS DOUBLE PRECISION))", hc)
        add(f"fold/LEAST({hn},f8lit)", f"LEAST({h}, CAST(9 AS DOUBLE PRECISION))", hc)
        add(f"fold/CASE({hn},f8lit)", f"CASE WHEN 1 = 1 THEN {h} ELSE CAST(1 AS DOUBLE PRECISION) END", hc)
        add(f"fold/CASE({hn},noelse)", f"CASE WHEN 1 = 1 THEN {h} END", hc)
        add(f"fold/CASE({hn},f8col)", f"CASE WHEN 1 = 1 THEN {h} ELSE f END", True)
        add(f"fold/CASE(simple,{hn})", f"CASE i WHEN 2 THEN {h} ELSE 1 END", True)
    names = [n for n, _, _ in out]
    assert len(names) == len(set(names)), 'duplicate template names'
    return out

# query-shaped sources (INSERT … SELECT only): the value passes through a
# plan construct before it meets the target.
SHAPE_OPERANDS = [x for x in H_OPERANDS if x[0] in ("numlit", "numcol", "f8col", "div_numlit", "div_numcol", "sqrt_numcol", "sqrt_f8col", "cast_f8col_numeric", "extract_epoch")] + [
    # a DECIMAL-declared value PostgreSQL types float8. NULLIF(numeric
    # column, float8 expression) is not here: its SELECT refuses on the
    # evaluator's float8 output vector (the #361 guard) before any
    # assignment, at base too — the fold/NULLIF(numcol, …) [select] pins.
    ("nullif_numlit_f8lit", "NULLIF(0.5, CAST(9 AS DOUBLE PRECISION))", False),
]

import json, os, re, subprocess, sys


KS = [(1, -3), (2, -1), (3, 0), (4, 1), (5, 2), (6, 3), (7, 0), (8, 0)]
SRC_ROWS = ", ".join(f"({i}, {k}, 2, 0.50, 0.5)" for i, k in KS)

def cells():
    out = []
    for name, e, cols in T():
        half = f"({e}) + k"
        bare = f"{e}"
        ctl = f"({e}) - 0.1 + 2"
        # VALUES: literal k
        if not cols:
            rows = []
            for i, k in KS[:6]:
                ex = f"({e}) + {k}" if k >= 0 else f"({e}) - {-k}"
                rows.append(f"({i}, {ex}, {ex})")
            rows.append(f"(7, {bare}, {bare})")
            rows.append(f"(8, ({e}) - 0.1 + 2, ({e}) - 0.1 + 2)")
            out.append(dict(name=name, door="values", stmts=["DELETE FROM t_v",
                        "INSERT INTO t_v (id, n4, n8) VALUES " + ", ".join(rows)],
                        read="SELECT id, n4, n8 FROM t_v ORDER BY id"))
        out.append(dict(name=name, door="select", stmts=["DELETE FROM t_s",
            f"INSERT INTO t_s (id, n4, n8) SELECT id, {half}, {half} FROM src WHERE id <= 6",
            f"INSERT INTO t_s (id, n4, n8) SELECT id, {bare}, {bare} FROM src WHERE id = 7",
            f"INSERT INTO t_s (id, n4, n8) SELECT id, ({e}) - 0.1 + 2, ({e}) - 0.1 + 2 FROM src WHERE id = 8"],
            read="SELECT id, n4, n8 FROM t_s ORDER BY id"))
        out.append(dict(name=name, door="update", stmts=["UPDATE t_u SET n4 = NULL, n8 = NULL",
            f"UPDATE t_u SET n4 = {half}, n8 = {half} WHERE id <= 6",
            f"UPDATE t_u SET n4 = {bare}, n8 = {bare} WHERE id = 7",
            f"UPDATE t_u SET n4 = {ctl}, n8 = {ctl} WHERE id = 8"],
            read="SELECT id, n4, n8 FROM t_u ORDER BY id"))
        out.append(dict(name=name, door="merge", stmts=["UPDATE t_m SET n4 = NULL, n8 = NULL",
            f"MERGE INTO t_m USING src ON t_m.id = src.id WHEN MATCHED AND src.id <= 6 THEN UPDATE SET n4 = {half}, n8 = {half}",
            f"MERGE INTO t_m USING src ON t_m.id = src.id WHEN MATCHED AND src.id = 7 THEN UPDATE SET n4 = {bare}, n8 = {bare}",
            f"MERGE INTO t_m USING src ON t_m.id = src.id WHEN MATCHED AND src.id = 8 THEN UPDATE SET n4 = {ctl}, n8 = {ctl}"],
            read="SELECT id, n4, n8 FROM t_m ORDER BY id"))
    # query-shaped sources through INSERT … SELECT
    def sel(name, *selects):
        out.append(dict(name=name, door="select", stmts=["DELETE FROM t_s"] + [
            "INSERT INTO t_s (id, n4, n8) " + s for s in selects], read="SELECT id, n4, n8 FROM t_s ORDER BY id"))
    for hn, h, hc in SHAPE_OPERANDS:
        sel(f"shape/derived/{hn}",
            f"SELECT id, x + k, x + k FROM (SELECT id, k, {h} AS x FROM src) s WHERE id <= 7")
        sel(f"shape/derived-bare/{hn}",
            f"SELECT id, x, x FROM (SELECT id, {h} AS x FROM src) s WHERE id = 7")
        sel(f"shape/cte/{hn}",
            f"WITH c AS (SELECT id, k, {h} AS x FROM src) SELECT id, x + k, x + k FROM c WHERE id <= 7")
        sel(f"shape/union-all/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 3 UNION ALL SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id BETWEEN 4 AND 7")
        sel(f"shape/union-with-intarm/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 7 UNION ALL SELECT id, k, k FROM src WHERE id = 8")
        sel(f"shape/union-with-f8arm/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 7 UNION ALL SELECT id, f, f FROM src WHERE id = 8")
        for agg in ["SUM", "AVG", "MIN", "MAX"]:
            sel(f"shape/agg-{agg}/{hn}",
                f"SELECT id, {agg}(x) + k, {agg}(x) + k FROM (SELECT id, k, {h} AS x FROM src) s WHERE id <= 7 GROUP BY id, k")
            sel(f"shape/window-{agg}/{hn}",
                f"SELECT id, {agg}(x) OVER (PARTITION BY id) + k, {agg}(x) OVER (PARTITION BY id) + k FROM (SELECT id, k, {h} AS x FROM src) s WHERE id <= 7")
        sel(f"shape/window-first_value/{hn}",
            f"SELECT id, FIRST_VALUE(x) OVER (PARTITION BY id ORDER BY id) + k, FIRST_VALUE(x) OVER (PARTITION BY id ORDER BY id) + k FROM (SELECT id, k, {h} AS x FROM src) s WHERE id <= 7")
        sel(f"shape/distinct/{hn}",
            f"SELECT DISTINCT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 7")
        sel(f"shape/join/{hn}",
            f"SELECT a.id, x + a.k, x + a.k FROM src a JOIN (SELECT id, {h} AS x FROM src) b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/orderlimit/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 7 ORDER BY id LIMIT 7")
        if not hc:
            sel(f"shape/scalar-subquery/{hn}",
                f"SELECT id, (SELECT {h}) + k, (SELECT {h}) + k FROM src WHERE id <= 7")
    # the category through every other plan construct a source value can come
    # through: a join whose arms publish the SAME name at different categories
    # (the float8 base column `f` beside a derived `f`), a scalar subquery with a
    # FROM, a recursive CTE, the other set operations, LATERAL, a VALUES list,
    # a value window and a CTE chain
    for hn, h, hc in SHAPE_OPERANDS:
        der = f"(SELECT id, {h} AS f FROM src)"
        sel(f"shape/join-samename-base/{hn}",
            f"SELECT a.id, a.f + a.k, a.f + a.k FROM src a JOIN {der} b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/join-samename-derived/{hn}",
            f"SELECT a.id, b.f + a.k, b.f + a.k FROM src a JOIN {der} b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/join-samename-reversed/{hn}",
            f"SELECT b.id, b.f + b.k, b.f + b.k FROM {der} a JOIN src b ON a.id = b.id WHERE b.id <= 7")
        sel(f"shape/join-samename-bare/{hn}",
            f"SELECT a.id, a.f, a.f FROM src a JOIN {der} b ON a.id = b.id WHERE a.id = 7")
        sel(f"shape/join-self-cte/{hn}",
            f"WITH c AS (SELECT id, k, {h} AS f FROM src) SELECT a.id, b.f + a.k, b.f + a.k FROM c a JOIN c b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/join-self-mixed/{hn}",
            f"WITH c AS (SELECT id, k, {h} AS f FROM src), e AS (SELECT id, k, f FROM src) SELECT a.id, b.f + a.k, b.f + a.k FROM c a JOIN e b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/join-using-base/{hn}",
            f"SELECT a.id, a.f + a.k, a.f + a.k FROM src a JOIN {der} b USING (id) WHERE a.id <= 7")
        sel(f"shape/join-using-derived/{hn}",
            f"SELECT a.id, b.f + a.k, b.f + a.k FROM src a JOIN {der} b USING (id) WHERE a.id <= 7")
        sel(f"shape/join-three-base/{hn}",
            f"SELECT a.id, c.f + a.k, c.f + a.k FROM src a JOIN src c ON a.id = c.id JOIN {der} b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/join-three-derived/{hn}",
            f"SELECT a.id, b.f + a.k, b.f + a.k FROM src a JOIN src c ON a.id = c.id JOIN {der} b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/join-three-mid/{hn}",
            f"SELECT a.id, c.f + a.k, c.f + a.k FROM src a JOIN {der} b ON a.id = b.id JOIN src c ON a.id = c.id WHERE a.id <= 7")
        sel(f"shape/join-left-base/{hn}",
            f"SELECT a.id, a.f + a.k, a.f + a.k FROM src a LEFT JOIN {der} b ON a.id = b.id WHERE a.id <= 7")
        sel(f"shape/scalar-subquery-corr/{hn}",
            f"SELECT id, (SELECT {h} FROM src s2 WHERE s2.id = src.id) + k, (SELECT {h} FROM src s2 WHERE s2.id = src.id) + k FROM src WHERE id <= 7")
        sel(f"shape/scalar-subquery-uncorr/{hn}",
            f"SELECT id, (SELECT {h} FROM src s2 WHERE s2.id = 1) + k, (SELECT {h} FROM src s2 WHERE s2.id = 1) + k FROM src WHERE id <= 7")
        sel(f"shape/scalar-subquery-agg/{hn}",
            f"SELECT id, (SELECT MAX({h}) FROM src s2) + k, (SELECT MAX({h}) FROM src s2) + k FROM src WHERE id <= 7")
        sel(f"shape/recursive-cte/{hn}",
            f"WITH RECURSIVE r(n, id, k, x) AS (SELECT 1, id, k, {h} FROM src WHERE id <= 7 UNION ALL SELECT n + 1, id, k, x FROM r WHERE n < 2) SELECT id, x + k, x + k FROM r WHERE n = 2")
        sel(f"shape/union-distinct/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 3 UNION SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id BETWEEN 4 AND 7")
        sel(f"shape/intersect/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 7 INTERSECT SELECT id, ({h}) + k, ({h}) + k FROM src")
        sel(f"shape/except/{hn}",
            f"SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id <= 7 EXCEPT SELECT id, ({h}) + k, ({h}) + k FROM src WHERE id = 8")
        sel(f"shape/lateral-from/{hn}",
            f"SELECT a.id, l.x + a.k, l.x + a.k FROM src a CROSS JOIN LATERAL (SELECT {h} AS x FROM src s2 WHERE s2.id = a.id) l WHERE a.id <= 7")
        sel(f"shape/lateral-nofrom/{hn}",
            f"SELECT a.id, l.x + a.k, l.x + a.k FROM src a CROSS JOIN LATERAL (SELECT {h} AS x) l WHERE a.id <= 7")
        sel(f"shape/window-last_value/{hn}",
            f"SELECT id, LAST_VALUE(x) OVER (PARTITION BY id) + k, LAST_VALUE(x) OVER (PARTITION BY id) + k FROM (SELECT id, k, {h} AS x FROM src) s WHERE id <= 7")
        sel(f"shape/cte-chain/{hn}",
            f"WITH c AS (SELECT id, k, {h} AS x FROM src), c2 AS (SELECT id, k, x FROM c) SELECT id, x + k, x + k FROM c2 WHERE id <= 7")
        if not hc:
            rows = ", ".join(f"({i}, {k}, {h})" for i, k in KS[:7])
            sel(f"shape/values-from/{hn}",
                f"SELECT v.id, v.x + v.k, v.x + v.k FROM (VALUES {rows}) v(id, k, x)")
    # the STDDEV / VARIANCE family over {0, 1}: 0.5 is var_samp and stddev_pop
    for cat, lo, hi in [("int", "i - 2", "i - 1"), ("numeric", "d - 0.5", "d + 0.5"), ("float8", "f - 0.5", "f + 0.5")]:
        for agg in ["STDDEV_POP", "VAR_SAMP", "VARIANCE"]:
            sel(f"shape/agg-{agg}/{cat}",
                f"SELECT id, {agg}(v) + k, {agg}(v) + k FROM (SELECT id, k, {lo} AS v FROM src UNION ALL SELECT id, k, {hi} FROM src) s WHERE id <= 7 GROUP BY id, k")
    return out

def pg_run(cells, ddl_pg):
    lines = ["SET statement_timeout='30s';", "\\set ON_ERROR_STOP off"] + ddl_pg
    for n, c in enumerate(cells):
        lines.append(f"SELECT '@@CELL {n}';")
        for s in c["stmts"]:
            lines.append(s + ";")
            lines.append(f"\\if :ERROR\n\\echo @@ERR {n} :LAST_ERROR_SQLSTATE :LAST_ERROR_MESSAGE\n\\endif")
        lines.append(f"SELECT '@@ROWS {n}', string_agg(id || ':' || coalesce(n4::text, 'NULL') || ':' || coalesce(n8::text, 'NULL'), ' ' ORDER BY id) FROM ({c['read']}) r;")
    p = subprocess.run(["docker", "exec", "-i", os.environ.get("PG_CONTAINER", "wadjet-pg-ir"), "psql", "-U", "wadjet", "-d", "wadjet_oracle", "-At", "-F", "\t", "-v", "VERBOSITY=terse"],
                       input="\n".join(lines), capture_output=True, text=True)
    # stderr carries errors; attribute them to the cell they follow
    return p.stdout, p.stderr, "\n".join(lines)

if __name__ == "__main__":
    cs = cells()
    ddl = ["DROP TABLE IF EXISTS src, t_v, t_s, t_u, t_m;",
           "CREATE TABLE src (id INTEGER, k INTEGER, i INTEGER, d NUMERIC(10,2), f DOUBLE PRECISION);",
           f"INSERT INTO src VALUES {SRC_ROWS};",
           "CREATE TABLE t_v (id INTEGER, n4 INTEGER, n8 BIGINT);",
           "CREATE TABLE t_s (id INTEGER, n4 INTEGER, n8 BIGINT);",
           "CREATE TABLE t_u (id INTEGER, k INTEGER, i INTEGER, d NUMERIC(10,2), f DOUBLE PRECISION, n4 INTEGER, n8 BIGINT);",
           f"INSERT INTO t_u (id, k, i, d, f) SELECT id, k, i, d, f FROM src;",
           "CREATE TABLE t_m (id INTEGER, n4 INTEGER, n8 BIGINT);",
           "INSERT INTO t_m (id) SELECT id FROM src;"]
    out, err, script = pg_run(cs, ddl)
    errs = {}
    rows = {}
    for line in out.split("\n"):
        m = re.match(r"@@ERR (\d+) (\S+) (.*)", line)
        if m: errs.setdefault(int(m.group(1)), []).append(m.group(2)+" "+m.group(3))
        m = re.match(r"@@ROWS (\d+)\t(.*)", line)
        if m: rows[int(m.group(1))] = m.group(2)
    kept, dropped = [], []
    for n, c in enumerate(cs):
        if n in errs:
            dropped.append((c["name"], c["door"], errs[n][0])); continue
        c["want"] = rows[n]; kept.append(c)
    json.dump(kept, open("cells_full.json","w"))
    open("dropped.txt","w").write("\n".join("\t".join(d) for d in dropped))
    print(len(cs), "cells;", len(kept), "kept;", len(dropped), "PG refuses")

def compact():
    cs = json.load(open("cells_full.json"))
    tmpl = {}
    order = []
    for c in cs:
        key = c["name"]
        if key not in tmpl:
            order.append(key)
            t = {"n": key, "d": "", "w": " ".join(":".join(r.split(":")[:2]) for r in c["want"].split())}
            assert all(r.split(":")[1] == r.split(":")[2] for r in c["want"].split())
            if key.startswith("shape/"):
                t["q"] = [s[len("INSERT INTO t_s (id, n4, n8) "):] for s in c["stmts"][1:]]
            else:
                # recover the expression from the SELECT door's bare statement
                t["e"] = None
            tmpl[key] = t
        tmpl[key]["d"] += c["door"][0]
    exprs = {name: e for name, e, _ in T()}
    for k in order:
        if tmpl[k].get("e", 1) is None:
            tmpl[k]["e"] = exprs[k]
    return [tmpl[k] for k in order]

if __name__ == "__main__":
    out = compact()
    with open("cells.json", "w") as f:
        f.write("[\n" + ",\n".join(json.dumps(t, separators=(",", ":")) for t in out) + "\n]\n")
