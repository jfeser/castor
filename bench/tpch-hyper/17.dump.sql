CREATE temp VIEW q17 AS (
  SELECT
    p_brand,
    p_container,
    l_quantity,
    l_extendedprice,
    p_partkey
    FROM
        part,
      lineitem
   WHERE
        p_partkey = l_partkey
);

\copy (select * from q17) to 'q17.tbl' delimiter '|';
CREATE temp VIEW q17_2 AS (
  SELECT
    l_partkey,
    trunc((0.2 * avg(l_quantity)), 5) AS l_avgquantity
    FROM
        lineitem
   GROUP BY
        l_partkey
);

\copy (select * from q17_2) to 'q17_2.tbl' delimiter '|';
