CREATE temp VIEW q18_1 AS (
  SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity) as sum_l_quantity
    FROM
        customer, orders, lineitem
      lineitem
   WHERE
     c_custkey = o_custkey and o_orderkey = l_orderkey
   group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
);

\copy (select * from q18_1) to 'q19_1.tbl' delimiter '|';

CREATE temp VIEW q18_2 AS (
  SELECT
    l_orderkey,
    sum(l_quantity) as sum_l_quantity
    FROM
        lineitem
   GROUP BY
        l_orderkey
);

\copy (select * from q18_2) to 'q18_2.tbl' delimiter '|';
