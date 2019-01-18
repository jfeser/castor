CREATE temp VIEW q8 AS (
  SELECT
    r_name,
    extract(year FROM o_orderdate) AS o_year,
    (l_extendedprice * (1 - l_discount)) AS volume,
    n2.n_name AS nation_name,
    p_type
    FROM
        part,
      lineitem,
      supplier,
      orders,
      customer,
      region,
      nation AS n1,
      nation AS n2
   WHERE
        p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate >= date '1995-01-01'
    AND o_orderdate <= date '1996-12-31'
);

\copy (select * from q8) to 'q8.tbl' delimiter '|';
