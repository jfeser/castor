SELECT
    o_orderpriority,
    sum(agg2) AS order_count
FROM
    q4
WHERE
    o_orderdate >= date '1993-07-01'
    AND o_orderdate < date '1993-07-01' + interval '3' month
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;

