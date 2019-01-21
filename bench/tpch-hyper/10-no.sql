SELECT
    q10_1.c_custkey,
    c_name,
    revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    q10_1,
    q10_2
WHERE
    o_orderdate = date '1993-10-01'
    AND q10_1.c_custkey = q10_2.c_custkey;

