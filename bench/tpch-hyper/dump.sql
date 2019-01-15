create temp view q1 as (select l_returnflag, l_linestatus, l_shipdate, sum(l_quantity) as v_sum_qty, sum(l_extendedprice) as v_sum_base_price,	sum(l_extendedprice * (1 - l_discount)) as v_sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as v_sum_charge,	 sum(l_discount) as v_sum_disc,	count(*) as v_count_order from lineitem group by  l_shipdate,	l_returnflag,	l_linestatus order by	l_returnflag,	l_linestatus
  );
\copy q1 to 'q1.tbl' delimiter '|';

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
         s_comment
         from supplier);
\copy (select * from q2_supplier) to 'q2_supplier.tbl' delimiter '|';

create temp view q3 as (
  select
    l_orderkey, o_orderdate, o_shippriority,
    l_shipdate, c_mktsegment, l_extendedprice, l_discount
    from lineitem, orders, customer
   where c_custkey = o_custkey and l_orderkey = o_orderkey
);
\copy (select * from q3) to 'q3.tbl' delimiter '|';

create temp view q4 as (
  select o_orderpriority, o_orderdate, count(*) as agg2
    from orders
   where exists (
     select *
       from lineitem
      where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
   group by o_orderpriority, o_orderdate);
\copy (select * from q4) to 'q4.tbl' delimiter '|';

create temp view q5 as (
  select o_orderdate, n_name, r_name, sum((l_extendedprice * (1 - l_discount))) as agg3
    from orders, nation, region, lineitem, supplier, customer
   where l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_custkey = o_custkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey
   group by o_orderdate, n_name, r_name);
\copy (select * from q5) to 'q5.tbl' delimiter '|';

create temp view q6 as (
  select l_extendedprice, l_discount, l_shipdate, l_quantity from lineitem);
\copy (select * from q6) to 'q6.tbl' delimiter '|';

create temp view q7 as (
  select n1.n_name, n2.n_name, year(l_shipdate) as l_year, sum((l_extendedprice * (1 - l_discount))) as revenue
    from supplier, lineitem, orders, customer, nation as n1, nation as n2
   where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and c_nationkey = n2.n_nationkey and l_shipdate > date '1995-01-01' and l_shipdate < date '1996-12-31' and s_nationkey = n1.n_nationkey
   );
\copy (select * from q7) to 'q7.tbl' delimiter '|';

create temp view q7 as (
  select n1.n_name, n2.n_name, year(l_shipdate) as l_year, sum((l_extendedprice * (1 - l_discount))) as revenue
    from supplier, lineitem, orders, customer, nation as n1, nation as n2
   where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and c_nationkey = n2.n_nationkey and l_shipdate > date '1995-01-01' and l_shipdate < date '1996-12-31' and s_nationkey = n1.n_nationkey
);
\copy (select * from q7) to 'q7.tbl' delimiter '|';

create temp view q8 as (
  select r_name, year(o_orderdate) as o_year, (l_extendedprice * (1 - l_discount)) as volume, n2.n_name as nation_name, p_type from
                                                                                                                                   part, lineitem, supplier, orders, customer, nation as n1, nation as n2
                                                                                                                                   where p_partkey = l_partkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and s_nationkey = n2.n_nationkey and o_orderdate >= date '1995-01-01' and o_orderdate <= date '1996-12-31'
);
\copy (select * from q8) to 'q8.tbl' delimiter '|';

create temp view q9 as (
  select (strpos(part.p_name, 'black') > 0) as wit1__0,
         (strpos(part.p_name, 'blue') > 0) as wit1__1,
         (strpos(part.p_name, 'brown') > 0) as wit1__2,
         (strpos(part.p_name, 'green') > 0) as wit1__3,
         (strpos(part.p_name, 'grey') > 0) as wit1__4,
         (strpos(part.p_name, 'navy') > 0) as wit1__5,
         (strpos(part.p_name, 'orange') > 0) as wit1__6,
         (strpos(part.p_name, 'pink') > 0) as wit1__7,
         (strpos(part.p_name, 'purple') > 0) as wit1__8,
         (strpos(part.p_name, 'red') > 0) as wit1__9,
         (strpos(part.p_name, 'white') > 0) as wit1__10,
         (strpos(part.p_name, 'yellow') > 0) as wit1__11,
         n_name as nation,
         year(o_orderdate) as o_year,
         ((l_extendedprice * (1 - l_discount))
         - (ps_supplycost * l_quantity)) as amount,
         p_name
    from supplier, lineitem, partsupp, part, orders
   where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey);
\copy (select * from q9) to 'q9.tbl' delimiter '|';

create temp view q10_1 as (
  select sum(((l_extendedprice * 1) -
              l_discount)) as revenue, n_name, c_custkey from
                                        customer, orders, nation, lineitem where
                                                                             c_custkey = o_custkey and c_nationkey = n_nationkey and l_orderkey = o_orderkey and l_returnflag = 'R'
group by c_custkey, n_name
);
  \copy (select * from q10_1) to 'q10_1.tbl' delimiter '|';

  create temp view q10_2 as (
    select c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment from customer);
\copy (select * from q10_2) to 'q10_2.tbl' delimiter '|';

create temp view q12 as (
  select l_receiptdate, sum((if (not((orders.o_orderpriority = '1-URGENT')) &&
                                 not((orders.o_orderpriority = '2-HIGH'))) then 1 else 0)) as agg7, sum((if ((orders.o_orderpriority = '1-URGENT') ||
                                                                                                             (orders.o_orderpriority = '2-HIGH')) then 1 else 0)) as agg6, l_shipmode from
                                                                                                                                                                                          lineitem, orders where l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_orderkey = o_orderkey group by l_shipmode
);
\copy (select * from q12) to 'q12.tbl' delimiter '|';

create temp view q14 as (
  select sum((if (strpos(p_type, 'PROMO') = 1) then (l_extendedprice
                                                          *
                                                          (1 -
                                                          l_discount)) else 0.0)) as agg1, sum((l_extendedprice * (1 - l_discount))) as agg2, l_shipdate from lineitem, part where p_partkey = l_partkey
);
\copy (select * from q13) to 'q13.tbl' delimiter '|';

-- create temp view q15 as (
--   select s_suppkey, s_name, s_address, s_phone, l_shipdate,
--          (select sum((l.l_extendedprice *
--                       (1 - l.l_discount))) from lineitem as l where l.l_shipdate >= l_shipdate and l.l_shipdate <= l_shipdate + '3' month group by l_suppkey) as total_revenue

--     from lineitem, supplier
--    where s_suppkey = l_suppkey and 
--    group by l_suppkey

--            (
--     select max(total_revenue_i) from (select sum((l.l_extendedprice *
--                                                   (1 - l.l_discount))) as total_revenue_i from lineitem as l where l.l_shipdate >= l_shipdate and l.l_shipdate <= l_shipdate + '3' month group by l_suppkey) as t) as total_revenue
--     from supplier

create temp view q16 as (
  select p_type, p_brand, p_size, count(*) as supplier_cnt
    from part, partsupp where p_partkey = ps_partkey and not exists (select * from supplier where ps_suppkey = s_suppkey and strpos(supplier.s_comment, 'Customer') >= 1 and strpos(supplier.s_comment, 'Complaints') >= 1)
   group by p_type, p_brand, p_size);
\copy (select * from q16) to 'q16.tbl' delimiter '|';

create temp view q17 as (
  select p_brand, p_container, l_quantity, l_extendedprice, p_partkey from
                                                                          part, lineitem where p_partkey = l_partkey);
\copy (select * from q17) to 'q17.tbl' delimiter '|';

create temp view q17_2 as (
  select l_partkey, (0.2 * avg(l_quantity)) as l_avgquantity from lineitem group by l_partkey);
\copy (select * from q17_2) to 'q17_2.tbl' delimiter '|';
