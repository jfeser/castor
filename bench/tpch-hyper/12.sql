SELECT
    l_shipmode,
    sum(agg6) AS high_line_count,
    sum(agg7) AS low_line_count
FROM
    q12
WHERE
    l_receiptdate >= date '1994-01-01'
    AND l_receiptdate < date '1994-01-01' + interval '1' year
    AND (l_shipmode = 'MAIL'
        OR l_shipmode = 'SHIP')
     group by l_shipmode
ORDER BY
    l_shipmode;

