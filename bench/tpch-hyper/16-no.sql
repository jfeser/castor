SELECT
    p_type,
    p_brand,
    p_size supplier_cnt
FROM
    q16
WHERE
    NOT (p_brand = 'Brand#45')
    AND NOT (strpos(part.p_type, 'MEDIUM POLISHED') = 1)
    AND (p_size = 49 || p_size = 14 || p_size = 23 || p_size = 45 || p_size = 19 || p_size = 3 || p_size = 36 || p_size = 9);

