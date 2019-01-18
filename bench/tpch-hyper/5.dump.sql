CREATE temp VIEW q5 AS (
  SELECT
    o_orderdate,
    n_name,
    r_name,
    sum((l_extendedprice * (1 - l_discount))) AS agg3
    FROM
        orders,
      nation,
      region,
      lineitem,
      supplier,
      customer
   WHERE
        l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_custkey = o_custkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
   GROUP BY
        o_orderdate,
        n_name,
        r_name
);

\copy (select * from q5) to 'q5.tbl' delimiter '|';
