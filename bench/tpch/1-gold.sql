-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998

drop materialized view q1;

create materialized view q1 as
  select
	l_returnflag,
	l_linestatus,
  l_shipdate,
	sum(l_quantity) as v_sum_qty,
	sum(l_extendedprice) as v_sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as v_sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as v_sum_charge,
	sum(l_discount) as v_sum_disc,
	count(*) as v_count_order
  from
	lineitem
  group by
  l_shipdate,
	l_returnflag,
	l_linestatus
  order by
	l_returnflag,
	l_linestatus;

create index on q1 (l_shipdate);
analyze q1;

explain analyze select
	l_returnflag,
	l_linestatus,
	sum(v_sum_qty) as sum_qty,
	sum(v_sum_base_price) as sum_base_price,
	sum(v_sum_disc_price) as sum_disc_price,
	sum(v_sum_charge) as sum_charge,
	(sum(v_sum_qty) / count(*)) as avg_qty,
	(sum(v_sum_base_price) / count(*)) as avg_price,
	(sum(v_sum_disc) / count(*)) as avg_disc,
	sum(v_count_order) as count_order
  from
	    q1
 where
	l_shipdate <= date '1998-12-01' - interval '90' day
 group by
	l_returnflag,
	l_linestatus
;
