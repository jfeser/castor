CREATE temp VIEW q14 AS (
  SELECT
    sum((
      CASE WHEN p_type LIKE 'PROMO%' THEN
                                         (l_extendedprice * (1 - l_discount))
      ELSE
         0.0
      END)) AS agg1,
    sum((l_extendedprice * (1 - l_discount))) AS agg2,
    l_shipdate
    FROM
        lineitem,
      part
   WHERE
        p_partkey = l_partkey
   GROUP BY
        l_shipdate
);

\copy (select * from q14) to 'q14.tbl' delimiter '|';
