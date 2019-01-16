SELECT
    l_returnflag,
    l_linestatus,
    sum(v_sum_qty) AS sum_qty,
    sum(v_sum_base_price) AS sum_base_price,
    sum(v_sum_disc_price) AS sum_disc_price,
    sum(v_sum_charge) AS sum_charge,
    (sum(v_sum_qty) / count(*)) AS avg_qty,
    (sum(v_sum_base_price) / count(*)) AS avg_price,
    (sum(v_sum_disc) / count(*)) AS avg_disc,
    sum(v_count_order) AS count_order
FROM
    q1
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

