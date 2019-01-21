SELECT
    o_year,
    sum(
        CASE WHEN nation_name = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM
    q8
WHERE
    p_type = 'ECONOMY ANODIZED STEEL'
    AND r_name = 'AMERICA'
GROUP BY
    o_year;

