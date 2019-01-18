SELECT
    p_type,
    p_brand,
    p_size,
    supplier_cnt
FROM
    q16
WHERE
    p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND (p_size = 49
        OR p_size = 14
        OR p_size = 23
        OR p_size = 45
        OR p_size = 19
        OR p_size = 3
        OR p_size = 36
        OR p_size = 9);

