SELECT
    o_orderdate,
    q10_1.c_custkey,
    revenue,
    n_name,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
FROM
    q10_1,
    q10_2
WHERE
    o_orderdate = date '1993-10-01'
    AND q10_1.c_custkey = q10_2.c_custkey;

