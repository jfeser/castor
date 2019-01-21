CREATE temp VIEW q7 AS (
  SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    extract(year FROM l_shipdate) AS l_year,
    sum((l_extendedprice * (1 - l_discount))) AS revenue
    FROM
        supplier,
      lineitem,
      orders,
      customer,
      nation AS n1,
      nation AS n2
   WHERE
        s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND c_nationkey = n2.n_nationkey
    AND l_shipdate >= date '1995-01-01'
    AND l_shipdate <= date '1996-12-31'
    AND s_nationkey = n1.n_nationkey
   GROUP BY
        n1.n_name,
        n2.n_name,
        extract(year FROM l_shipdate)
);

\copy (select * from q7) to 'q7.tbl' delimiter '|';
