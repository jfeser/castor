SELECT
    (sum(l_extendedprice) / 7.0) AS avg_yearly
FROM
    q17
WHERE
    l_quantity < (
        SELECT
            l_avgquantity
        FROM
            q17_2
        WHERE
            p_partkey = l_partkey)
    WHERE
        p_brand = 'Brand#23'
        AND p_container = 'MED BOX';

