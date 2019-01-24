create temp table q15_helper as (
  select l1.l_shipdate, l2.l_suppkey, sum(l2.l_extendedprice * (1 - l2.l_discount)) as total_revenue
    from (select l_shipdate, l_suppkey from lineitem) as l1,
         (select l_shipdate, l_suppkey, l_extendedprice, l_discount from lineitem) as l2
   where l2.l_shipdate >= l1.l_shipdate
     and l2.l_shipdate < l1.l_shipdate + interval '3' month
   group by l1.l_shipdate, l2.l_suppkey
);
create index on q15_helper (l_shipdate);
create index on q15_helper (total_revenue);

create temp table q15 as (select
                            l_shipdate,
                            s_suppkey,
                            s_name,
                            s_address,
                            s_phone,
                            total_revenue
                            from
                                q15_helper as qhh, supplier as s
                           where
                 l_suppkey = s_suppkey and
                 total_revenue = (
                   select
                     max(total_revenue)
                     from
                         q15_helper as qh
                    where qh.l_shipdate = qhh.l_shipdate 
                 ));

\copy (select * from q15) to 'q15.tbl' delimiter '|';
