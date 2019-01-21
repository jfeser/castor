create temp table q10_helper as (
select o_orderdate, n_name, c_custkey, c_name, c_acctbal, c_phone, c_address, c_comment, l_extendedprice, l_discount
from customer, orders, nation, lineitem
where l_returnflag = 'R' and c_nationkey = n_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey
);
create index on q10_helper (o_orderdate);

CREATE temp VIEW q10_1 AS (
  SELECT
    sum(((l_extendedprice * 1) - l_discount)) AS revenue,
    n_name,
    c_custkey,
    o1.o_orderdate
    FROM
      (select distinct o_orderdate from orders) as o1,
      q10_helper as o2
   WHERE
   o2.o_orderdate >= o1.o_orderdate and o2.o_orderdate < o1.o_orderdate + interval '3' month
   GROUP BY
   o1.o_orderdate,
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
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
