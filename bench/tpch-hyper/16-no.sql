SELECT
    p_type,
    p_brand,
    p_size,
    supplier_cnt
FROM
    q16
WHERE
    p_brand <> 'Brand#45'
    AND p_type not like 'MEDIUM POLISHED%'
    AND (p_size = 49 or p_size = 14 or p_size = 23 or p_size = 45 or p_size = 19 or p_size = 3 or p_size = 36 or p_size = 9);

