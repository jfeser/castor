CREATE temp VIEW q10_1 AS (
  SELECT
    sum(((l_extendedprice * 1) - l_discount)) AS revenue,
    n_name,
    c_custkey,
    o_orderdate
    FROM
        customer,
      orders,
      nation,
      lineitem
   WHERE
        c_custkey = o_custkey
    AND c_nationkey = n_nationkey
    AND l_orderkey = o_orderkey
    AND l_returnflag = 'R'
   GROUP BY
        c_custkey,
        n_name,
        o_orderdate
);

\copy (select * from q10_1) to 'q10_1.tbl' delimiter '|';
CREATE temp VIEW q10_2 AS (
  SELECT
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
    FROM
        customer
);

\copy (select * from q10_2) to 'q10_2.tbl' delimiter '|';
