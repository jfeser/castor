CREATE temp VIEW q6 AS (
  SELECT
    l_extendedprice,
    l_discount,
    l_shipdate,
    l_quantity
    FROM
        lineitem
);

\copy (select * from q6) to 'q6.tbl' delimiter '|';
