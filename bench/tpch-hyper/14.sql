select ((100 * sum(agg1)) / sum(agg2)) as promo_revenue
  from q14 where l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;
