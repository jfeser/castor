CREATE temp VIEW q9 AS (
    SELECT
        (strpos(part.p_name, 'black') > 0) AS wit1__0,
        (strpos(part.p_name, 'blue') > 0) AS wit1__1,
        (strpos(part.p_name, 'brown') > 0) AS wit1__2,
        (strpos(part.p_name, 'green') > 0) AS wit1__3,
        (strpos(part.p_name, 'grey') > 0) AS wit1__4,
        (strpos(part.p_name, 'navy') > 0) AS wit1__5,
        (strpos(part.p_name, 'orange') > 0) AS wit1__6,
        (strpos(part.p_name, 'pink') > 0) AS wit1__7,
        (strpos(part.p_name, 'purple') > 0) AS wit1__8,
        (strpos(part.p_name, 'red') > 0) AS wit1__9,
        (strpos(part.p_name, 'white') > 0) AS wit1__10,
        (strpos(part.p_name, 'yellow') > 0) AS wit1__11,
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        ((l_extendedprice * (1 - l_discount)) - (ps_supplycost * l_quantity)) AS amount,
        p_name
    FROM
        supplier,
        lineitem,
        partsupp,
        part,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
);

\copy (select * from q9) to 'q9.tbl' delimiter '|';
