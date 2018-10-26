select o_orderpriority, sum(agg2) as order_count
  from q4
 where o_orderdate >= date '1993-07-01'
	 and o_orderdate < date '1993-07-01' + interval '3' month
 order by o_orderpriority;
