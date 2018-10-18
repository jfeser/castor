-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998

create materialized view q1 as
  select
	l_returnflag,
	l_linestatus,
  l_shipdate,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	sum(l_extendedprice) as sum_price,
	sum(l_discount) as sum_disc,
	count(*) as count_order
  from
	lineitem
  group by
	l_returnflag,
  l_shipdate,
	l_linestatus
  order by
	l_returnflag,
	l_linestatus;

create index on q1 (l_shipdate);

select
	l_returnflag,
	l_linestatus,
	sum(sum_qty),
	sum(sum_base_price),
	sum(sum_disc_price),
	sum(sum_charge),
	sum(sum_qty) / sum(count_order),
  sum(sum_price) / sum(count_order),
  sum(sum_disc) / sum(count_order),
	sum(count_order)
  from
	    q1
 where
	l_shipdate <= date '1998-12-01' - interval ':1' day
 group by l_returnflag, l_linestatus;
