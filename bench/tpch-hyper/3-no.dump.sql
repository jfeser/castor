CREATE temp VIEW q3 AS (
  SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority,
    l_shipdate,
    c_mktsegment,
    l_extendedprice,
    l_discount
    FROM
        lineitem,
      orders,
      customer
   WHERE
        c_custkey = o_custkey
    AND l_orderkey = o_orderkey
);

\copy (select * from q3) to 'q3.tbl' delimiter '|';
