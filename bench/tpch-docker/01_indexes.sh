#!/bin/bash
set -e
psql --dbname "tpch" --username "$POSTGRES_USER" <<-EOSQL
CREATE UNIQUE INDEX IF NOT EXISTS partsupp_pkey ON public.partsupp USING btree (ps_partkey, ps_suppkey);
 CREATE INDEX IF NOT EXISTS nation_n_name_idx ON public.nation USING btree (n_name);
 CREATE UNIQUE INDEX IF NOT EXISTS nation_pkey ON public.nation USING btree (n_nationkey);
 CREATE INDEX IF NOT EXISTS lineitem_l_orderkey_idx1 ON public.lineitem USING btree (l_orderkey);
 CREATE INDEX IF NOT EXISTS lineitem_l_suppkey_idx ON public.lineitem USING btree (l_suppkey);
 CREATE INDEX IF NOT EXISTS lineitem_l_orderkey_idx ON public.lineitem USING btree (l_orderkey);
 CREATE INDEX IF NOT EXISTS lineitem_l_partkey_idx ON public.lineitem USING btree (l_partkey);
 CREATE INDEX IF NOT EXISTS lineitem_l_quantity_idx ON public.lineitem USING btree (l_quantity);
 CREATE INDEX IF NOT EXISTS lineitem_l_shipdate_idx ON public.lineitem USING btree (l_shipdate);
 CREATE INDEX IF NOT EXISTS lineitem_l_commitdate_l_receiptdate_idx ON public.lineitem USING btree (l_commitdate, l_receiptdate);
 CREATE UNIQUE INDEX IF NOT EXISTS lineitem_pkey ON public.lineitem USING btree (l_orderkey, l_linenumber);
 CREATE INDEX IF NOT EXISTS orders_o_custkey_idx ON public.orders USING hash (o_custkey);
 CREATE INDEX IF NOT EXISTS orders_o_orderpriority_idx ON public.orders USING btree (o_orderpriority);
 CREATE INDEX IF NOT EXISTS orders_o_orderdate_idx ON public.orders USING btree (o_orderdate);
 CREATE UNIQUE INDEX IF NOT EXISTS orders_pkey ON public.orders USING btree (o_orderkey);
 CREATE UNIQUE INDEX IF NOT EXISTS region_pkey ON public.region USING btree (r_regionkey);
 CREATE UNIQUE INDEX IF NOT EXISTS part_pkey ON public.part USING btree (p_partkey);
 CREATE INDEX IF NOT EXISTS supplier_s_nationkey_idx ON public.supplier USING btree (s_nationkey);
 CREATE UNIQUE INDEX IF NOT EXISTS supplier_pkey ON public.supplier USING btree (s_suppkey);
 CREATE INDEX IF NOT EXISTS customer_c_mktsegment_idx ON public.customer USING btree (c_mktsegment);
 CREATE INDEX IF NOT EXISTS customer_c_nationkey_idx ON public.customer USING btree (c_nationkey);
 CREATE UNIQUE INDEX IF NOT EXISTS customer_pkey ON public.customer USING btree (c_custkey);
EOSQL
