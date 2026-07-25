-- Register the SF100 TPC-H parquet (wadjet-bench-sf100-use2, Polars-sourced
-- schema: BIGINT keys, DOUBLE prices, DATE dates, lineitem comment column
-- named "comments") as Hive external tables in the Glue-backed catalog.
-- Idempotent: IF NOT EXISTS throughout. ${BUCKET} substituted by the runner.

CREATE SCHEMA IF NOT EXISTS hive.tpch;

CREATE TABLE IF NOT EXISTS hive.tpch.lineitem (
  l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber BIGINT,
  l_quantity BIGINT, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,
  l_returnflag VARCHAR, l_linestatus VARCHAR,
  l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
  l_shipinstruct VARCHAR, l_shipmode VARCHAR, comments VARCHAR
) WITH (external_location = 's3://${BUCKET}/lineitem', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.orders (
  o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus VARCHAR,
  o_totalprice DOUBLE, o_orderdate DATE, o_orderpriority VARCHAR,
  o_clerk VARCHAR, o_shippriority BIGINT, o_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/orders', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.customer (
  c_custkey BIGINT, c_name VARCHAR, c_address VARCHAR, c_nationkey BIGINT,
  c_phone VARCHAR, c_acctbal DOUBLE, c_mktsegment VARCHAR, c_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/customer', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.supplier (
  s_suppkey BIGINT, s_name VARCHAR, s_address VARCHAR, s_nationkey BIGINT,
  s_phone VARCHAR, s_acctbal DOUBLE, s_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/supplier', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.nation (
  n_nationkey BIGINT, n_name VARCHAR, n_regionkey BIGINT, n_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/nation', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.region (
  r_regionkey BIGINT, r_name VARCHAR, r_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/region', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.part (
  p_partkey BIGINT, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR,
  p_type VARCHAR, p_size BIGINT, p_container VARCHAR,
  p_retailprice DOUBLE, p_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/part', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.tpch.partsupp (
  ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty BIGINT,
  ps_supplycost DOUBLE, ps_comment VARCHAR
) WITH (external_location = 's3://${BUCKET}/partsupp', format = 'PARQUET');
