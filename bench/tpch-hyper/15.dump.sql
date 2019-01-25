create temp table q15 as (select d as l_shipdate, s_suppkey, s_name, s_address, s_phone, total_revenue
  from (select distinct l_shipdate as d from lineitem) as t5, lateral 
         (select s_suppkey, s_name, s_address, s_phone, total_revenue
            from supplier,
                 (select l_suppkey as skey,
                         sum(l_extendedprice * (1 - l_discount)) as total_revenue
                    from lineitem
                   where l_shipdate >= d and
                         l_shipdate < d + interval '3 month'
                   group by l_suppkey) as t1,
                 (select max(total_revenue) as max_total_revenue
                    from
                        (select sum(l_extendedprice * (1 - l_discount)) as total_revenue
                           from lineitem
                          where l_shipdate >= d and
                                l_shipdate < d + interval '3 month'
                          group by l_suppkey) as t2) as t3
           where s_suppkey = skey and total_revenue = max_total_revenue) as t4);


\copy (select * from q15) to 'q15.tbl' delimiter '|';
