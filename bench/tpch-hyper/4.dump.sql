CREATE temp VIEW q4 AS (
  SELECT
    o_orderpriority,
    o_orderdate,
    count(*) AS agg2
    FROM
        orders
   WHERE
        EXISTS (
          SELECT
            *
            FROM
                lineitem
           WHERE
                l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
   GROUP BY
            o_orderpriority,
            o_orderdate
);

\copy (select * from q4) to 'q4.tbl' delimiter '|';
