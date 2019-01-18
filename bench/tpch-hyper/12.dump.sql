CREATE temp VIEW q12 AS (
  SELECT
    l_receiptdate,
    sum((
      CASE WHEN (((orders.o_orderpriority = '1-URGENT'))
                 OR ((orders.o_orderpriority = '2-HIGH'))) THEN
                                                              1
      ELSE
         0
      END)) AS agg_high,
    sum((
      CASE WHEN ((orders.o_orderpriority <> '1-URGENT')
                 AND (orders.o_orderpriority <> '2-HIGH')) THEN
                                                              1
      ELSE
         0
      END)) AS agg_low,
    l_shipmode
    FROM
        lineitem,
      orders
   WHERE
        l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_orderkey = o_orderkey
   GROUP BY
        l_shipmode,
        l_receiptdate
);
\copy (select * from q12) to 'q12.tbl' delimiter '|';
