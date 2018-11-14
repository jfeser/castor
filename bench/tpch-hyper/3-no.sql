select l_orderkey, o_orderdate, o_shippriority, sum(l_extendedprice * (1 - l_discount))
  from q3
 where l_shipdate > date '1995-03-15' and
       o_orderdate < date '1995-03-15' and
       c_mktsegment = 'BUILDING'
 group by l_orderkey, o_orderdate, o_shippriority;
