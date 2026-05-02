INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.01);

COPY (SELECT r_regionkey::INTEGER AS r_regionkey, r_name, r_comment FROM region) TO '/tmp/tpch-sf001/region.parquet' (FORMAT PARQUET);
COPY (SELECT n_nationkey::INTEGER AS n_nationkey, n_name, n_regionkey::INTEGER AS n_regionkey, n_comment FROM nation) TO '/tmp/tpch-sf001/nation.parquet' (FORMAT PARQUET);
COPY (SELECT s_suppkey::INTEGER AS s_suppkey, s_name, s_address, s_nationkey::INTEGER AS s_nationkey, s_phone, s_acctbal::DOUBLE AS s_acctbal, s_comment FROM supplier) TO '/tmp/tpch-sf001/supplier.parquet' (FORMAT PARQUET);
COPY (SELECT p_partkey::INTEGER AS p_partkey, p_name, p_mfgr, p_brand, p_type, p_size::INTEGER AS p_size, p_container, p_retailprice::DOUBLE AS p_retailprice, p_comment FROM part) TO '/tmp/tpch-sf001/part.parquet' (FORMAT PARQUET);
COPY (SELECT ps_partkey::INTEGER AS ps_partkey, ps_suppkey::INTEGER AS ps_suppkey, ps_availqty::INTEGER AS ps_availqty, ps_supplycost::DOUBLE AS ps_supplycost, ps_comment FROM partsupp) TO '/tmp/tpch-sf001/partsupp.parquet' (FORMAT PARQUET);
COPY (SELECT c_custkey::INTEGER AS c_custkey, c_name, c_address, c_nationkey::INTEGER AS c_nationkey, c_phone, c_acctbal::DOUBLE AS c_acctbal, c_mktsegment, c_comment FROM customer) TO '/tmp/tpch-sf001/customer.parquet' (FORMAT PARQUET);
COPY (SELECT o_orderkey::INTEGER AS o_orderkey, o_custkey::INTEGER AS o_custkey, o_orderstatus, o_totalprice::DOUBLE AS o_totalprice, strftime(o_orderdate, '%Y-%m-%d') AS o_orderdate, o_orderpriority, o_clerk, o_shippriority::INTEGER AS o_shippriority, o_comment FROM orders) TO '/tmp/tpch-sf001/orders.parquet' (FORMAT PARQUET);
COPY (SELECT l_orderkey::INTEGER AS l_orderkey, l_partkey::INTEGER AS l_partkey, l_suppkey::INTEGER AS l_suppkey, l_linenumber::INTEGER AS l_linenumber, l_quantity::DOUBLE AS l_quantity, l_extendedprice::DOUBLE AS l_extendedprice, l_discount::DOUBLE AS l_discount, l_tax::DOUBLE AS l_tax, l_returnflag, l_linestatus, strftime(l_shipdate, '%Y-%m-%d') AS l_shipdate, strftime(l_commitdate, '%Y-%m-%d') AS l_commitdate, strftime(l_receiptdate, '%Y-%m-%d') AS l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM lineitem) TO '/tmp/tpch-sf001/lineitem.parquet' (FORMAT PARQUET);
