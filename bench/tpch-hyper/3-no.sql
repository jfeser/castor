SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority,
    sum(l_extendedprice * (1 - l_discount))
FROM
    q3
WHERE
    l_shipdate > date '1995-03-15'
    AND o_orderdate < date '1995-03-15'
    AND c_mktsegment = 'BUILDING'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority;

