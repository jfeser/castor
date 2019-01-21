SELECT
    ((100 * sum(agg1)) / sum(agg2)) AS promo_revenue
FROM
    q14
WHERE
    l_shipdate >= date '1995-09-01'
    AND l_shipdate < date '1995-09-01' + interval '1' month;

