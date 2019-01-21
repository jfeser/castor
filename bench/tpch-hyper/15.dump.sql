create temp table q15_helper as (
       select
           l1.l_shipdate,
              l1.l_suppkey,
                  sum(l2.l_extendedprice * (1 - l2.l_discount)) as total_revenue
                                           from
                                                  (select distinct l_shipdate, l_suppkey from lineitem) as l1,
                                                          lineitem as l2
                                                                    where l2.l_shipdate >= l1.l_shipdate
                                                                         and l2.l_shipdate < l1.l_shipdate + interval '3' month
                                                                              and l2.l_suppkey = l1.l_suppkey
                                                                                   group by l1.l_shipdate, l1.l_suppkey
                                                                                   );
                                                                                   create index on q15_helper (l_shipdate);

create temp table q15 as (select
  l_shipdate,
    s_suppkey,
      s_name,
        s_address,
          s_phone,
            total_revenue
            from
              q15_helper, supplier
              where
                 l_suppkey = s_suppkey and
                           total_revenue = (
                                           select
                                                max(total_revenue)
                                                    from
                                                            q15_helper as qh
                                                                 where qh.l_shipdate = l_shipdate
                                                                       ));

\copy (select * from q15) to 'q15.tbl' delimiter '|';
