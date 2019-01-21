SELECT
    l_shipmode,
    sum(agg_high) AS high_line_count,
    sum(agg_low) AS low_line_count
FROM
    q12
WHERE
    l_receiptdate >= date '1994-01-01'
    AND l_receiptdate < date '1994-01-01' + interval '1' year
    AND (l_shipmode = 'MAIL'
        OR l_shipmode = 'SHIP')
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;

