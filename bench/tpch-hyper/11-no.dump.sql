CREATE temp VIEW q11_1 AS (
  SELECT
    n_name,
    sum(ps_supplycost * ps_availqty) AS const33
    FROM
        partsupp,
      supplier,
      nation
   WHERE
        ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
   GROUP BY
        n_name
);

\copy (select * from q11_1) to 'q11_1.tbl' delimiter '|';
CREATE temp VIEW q11_2 AS (
  SELECT
    n_name,
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value_
    FROM
        nation,
      partsupp,
      supplier
   WHERE
        ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
   GROUP BY
        n_name,
        ps_partkey
);

\copy (select * from q11_2) to 'q11_2.tbl' delimiter '|';
