CREATE temp VIEW q1 AS (
    SELECT
        l_returnflag,
        l_linestatus,
        l_shipdate,
        sum(l_quantity) AS v_sum_qty,
        sum(l_extendedprice) AS v_sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS v_sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS v_sum_charge,
        sum(l_discount) AS v_sum_disc,
        count(*) AS v_count_order
    FROM
        lineitem
    GROUP BY
        l_shipdate,
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus
);

\copy (select * from q1) to 'q1.tbl' delimiter '|';
CREATE temp VIEW q2 AS (
    SELECT
        s_suppkey,
        n_name,
        p_partkey,
        p_mfgr,
        p_size,
        p_type,
        r_name
    FROM
        part,
        supplier,
        partsupp,
        nation,
        region
    WHERE
        p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND ps_supplycost = (
            SELECT
                min(ps_supplycost)
            FROM
                partsupp,
                supplier,
                nation,
                region AS r
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r.r_regionkey
                AND region.r_regionkey = r.r_regionkey)
        ORDER BY
            s_acctbal DESC,
            n_name,
            s_name,
            p_partkey
);

\copy (select * from q2) to 'q2.tbl' delimiter '|';
CREATE temp VIEW q2_supplier AS (
    SELECT
        s_suppkey,
        s_acctbal,
        s_name,
        s_address,
        s_phone,
        s_comment
    FROM
        supplier
);

\copy (select * from q2_supplier) to 'q2_supplier.tbl' delimiter '|';
CREATE temp VIEW q3 AS (
    SELECT
        l_orderkey,
        o_orderdate,
        o_shippriority,
        l_shipdate,
        c_mktsegment,
        l_extendedprice,
        l_discount
    FROM
        lineitem,
        orders,
        customer
    WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
);

\copy (select * from q3) to 'q3.tbl' delimiter '|';
CREATE temp VIEW q4 AS (
    SELECT
        o_orderpriority,
        o_orderdate,
        count(*) AS agg2
    FROM
        orders
    WHERE
        EXISTS (
            SELECT
                *
            FROM
                lineitem
            WHERE
                l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
        GROUP BY
            o_orderpriority,
            o_orderdate
);

\copy (select * from q4) to 'q4.tbl' delimiter '|';
CREATE temp VIEW q5 AS (
    SELECT
        o_orderdate,
        n_name,
        r_name,
        sum((l_extendedprice * (1 - l_discount))) AS agg3
    FROM
        orders,
        nation,
        region,
        lineitem,
        supplier,
        customer
    WHERE
        l_orderkey = o_orderkey
        AND l_suppkey = s_suppkey
        AND c_custkey = o_custkey
        AND c_nationkey = s_nationkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
    GROUP BY
        o_orderdate,
        n_name,
        r_name
);

\copy (select * from q5) to 'q5.tbl' delimiter '|';
CREATE temp VIEW q6 AS (
    SELECT
        l_extendedprice,
        l_discount,
        l_shipdate,
        l_quantity
    FROM
        lineitem
);

\copy (select * from q6) to 'q6.tbl' delimiter '|';
CREATE temp VIEW q7 AS (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        sum((l_extendedprice * (1 - l_discount))) AS revenue
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation AS n1,
        nation AS n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND c_nationkey = n2.n_nationkey
        AND l_shipdate > date '1995-01-01'
        AND l_shipdate < date '1996-12-31'
        AND s_nationkey = n1.n_nationkey
    GROUP BY
        n1.n_name,
        n2.n_name,
        extract(year FROM l_shipdate)
    ORDER BY
        n1.n_name,
        n2.n_name,
        l_year
);

\copy (select * from q7) to 'q7.tbl' delimiter '|';
CREATE temp VIEW q8 AS (
    SELECT
        r_name,
        extract(year FROM o_orderdate) AS o_year,
        (l_extendedprice * (1 - l_discount)) AS volume,
        n2.n_name AS nation_name,
        p_type
    FROM
        part,
        lineitem,
        supplier,
        orders,
        customer,
        region,
        nation AS n1,
        nation AS n2
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate >= date '1995-01-01'
        AND o_orderdate <= date '1996-12-31'
);

\copy (select * from q8) to 'q8.tbl' delimiter '|';
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
CREATE temp VIEW q10_1 AS (
    SELECT
        sum(((l_extendedprice * 1) - l_discount)) AS revenue,
        n_name,
        c_custkey
    FROM
        customer,
        orders,
        nation,
        lineitem
    WHERE
        c_custkey = o_custkey
        AND c_nationkey = n_nationkey
        AND l_orderkey = o_orderkey
        AND l_returnflag = 'R'
    GROUP BY
        c_custkey,
        n_name
);

\copy (select * from q10_1) to 'q10_1.tbl' delimiter '|';
CREATE temp VIEW q10_2 AS (
    SELECT
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment
    FROM
        customer
);

\copy (select * from q10_2) to 'q10_2.tbl' delimiter '|';
CREATE temp VIEW q12 AS (
    SELECT
        l_receiptdate,
        sum((
                CASE WHEN (NOT ((orders.o_orderpriority = '1-URGENT'))
                        AND NOT ((orders.o_orderpriority = '2-HIGH'))) THEN
                    1
                ELSE
                    0
                END)) AS agg7,
        sum((
                CASE WHEN ((orders.o_orderpriority = '1-URGENT')
                        OR (orders.o_orderpriority = '2-HIGH')) THEN
                    1
                ELSE
                    0
                END)) AS agg6,
        l_shipmode
    FROM
        lineitem,
        orders
    WHERE
        l_commitdate < l_receiptdate
        AND l_shipdate < l_commitdate
        AND l_orderkey = o_orderkey
    GROUP BY
        l_shipmode,
        l_receiptdate
);

\copy (select * from q12) to 'q12.tbl' delimiter '|';
CREATE temp VIEW q14 AS (
    SELECT
        sum((
                CASE WHEN (strpos(p_type, 'PROMO') = 1) THEN
                    (l_extendedprice * (1 - l_discount))
                ELSE
                    0.0
                END)) AS agg1,
        sum((l_extendedprice * (1 - l_discount))) AS agg2,
        l_shipdate
    FROM
        lineitem,
        part
    WHERE
        p_partkey = l_partkey
    GROUP BY
        l_shipdate
);

\copy (select * from q14) to 'q13.tbl' delimiter '|';
CREATE temp VIEW q15 AS (
    SELECT
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        l_shipdate,
        (
            SELECT
                sum((l.l_extendedprice * (1 - l.l_discount)))
            FROM
                lineitem AS l
            WHERE
                l.l_shipdate >= l_shipdate
                AND l.l_shipdate <= l_shipdate + interval '3' month
            GROUP BY
                l_suppkey) AS total_revenue
        FROM
            lineitem,
            supplier
        WHERE
            s_suppkey = l_suppkey
            AND
        GROUP BY
            l_suppkey (
                SELECT
                    max(total_revenue_i)
                FROM (
                    SELECT
                        sum((l.l_extendedprice * (1 - l.l_discount))) AS total_revenue_i
                    FROM
                        lineitem AS l
                    WHERE
                        l.l_shipdate >= l_shipdate
                        AND l.l_shipdate <= l_shipdate + interval '3' month
                    GROUP BY
                        l_suppkey) AS t) AS total_revenue
            FROM
                supplier CREATE temp VIEW q16 AS (
                    SELECT
                        p_type,
                        p_brand,
                        p_size,
                        count(*) AS supplier_cnt
                    FROM
                        part,
                        partsupp
                    WHERE
                        p_partkey = ps_partkey
                        AND NOT EXISTS (
                            SELECT
                                *
                            FROM
                                supplier
                            WHERE
                                ps_suppkey = s_suppkey
                                AND strpos(supplier.s_comment, 'Customer') >= 1
                                AND strpos(supplier.s_comment, 'Complaints') >= 1)
                        GROUP BY
                            p_type,
                            p_brand,
                            p_size
);

\copy (select * from q16) to 'q16.tbl' delimiter '|';
CREATE temp VIEW q17 AS (
    SELECT
        p_brand,
        p_container,
        l_quantity,
        l_extendedprice,
        p_partkey
    FROM
        part,
        lineitem
    WHERE
        p_partkey = l_partkey
);

\copy (select * from q17) to 'q17.tbl' delimiter '|';
CREATE temp VIEW q17_2 AS (
    SELECT
        l_partkey,
        (0.2 * avg(l_quantity)) AS l_avgquantity
    FROM
        lineitem
    GROUP BY
        l_partkey
);

\copy (select * from q17_2) to 'q17_2.tbl' delimiter '|';
