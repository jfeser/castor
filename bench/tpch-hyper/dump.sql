\copy (select l_returnflag, l_linestatus, l_shipdate, sum(l_quantity) as v_sum_qty, sum(l_extendedprice) as v_sum_base_price,	sum(l_extendedprice * (1 - l_discount)) as v_sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as v_sum_charge,	 sum(l_discount) as v_sum_disc,	count(*) as v_count_order from lineitem group by  l_shipdate,	l_returnflag,	l_linestatus order by	l_returnflag,	l_linestatus) to 'q1.tbl' delimiter '|';

create temp view q2 as (select
         s_suppkey,
	       n_name,
	       p_partkey,
	       p_mfgr,
         p_size,
         p_type,
         r_name
         from
	           part,
	         supplier,
	         partsupp,
	         nation,
	         region
        where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and ps_supplycost = (
		select
			min(ps_supplycost)
		  from
			    partsupp,
			  supplier,
			  nation,
			  region as r
		 where
			p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and s_nationkey = n_nationkey
	and n_regionkey = r.r_regionkey
      and region.r_regionkey = r.r_regionkey
	)
  order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey);
\copy (select * from q2) to 'q2.tbl' delimiter '|';

create temp view q2_supplier as (select
         s_suppkey,
         s_acctbal,
	       s_name,
	       s_address,
	       s_phone,
         s_comment,
         from
	           supplier);
\copy (select * from q2_supplier) to 'q2_supplier.tbl' delimiter '|';

create temp view q4 as (
  select o_orderpriority, o_orderdate, count(*) as agg2
    from orders
   where exists (
     select *
       from lineitem
      where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
   group by o_orderpriority, o_orderdate);
\copy (select * from q4) to 'q4.tbl' delimiter '|';

create temp view q6 as (
  select l_extendedprice, l_discount, l_shipdate, l_quantity from lineitem);
\copy (select * from q6) to 'q6.tbl' delimiter '|';
