CREATE temp VIEW q1 AS (
  SELECT
    l_returnflag,
    l_linestatus,
    l_shipdate,
    sum(l_quantity) AS v_sum_qty,
    sum(l_extendedprice) AS v_sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS v_sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS v_sum_charge,
    sum(l_discount) AS v_sum_disc,
    count(*) AS v_count_order
    FROM
        lineitem
   GROUP BY
        l_shipdate,
        l_returnflag,
        l_linestatus
);

\copy (select * from q1) to 'q1.tbl' delimiter '|';
