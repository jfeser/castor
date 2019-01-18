CREATE TABLE q1 (
    l_returnflag char(1) NOT NULL,
    l_linestatus char(1) NOT NULL,
    l_shipdate date NOT NULL,
    v_sum_qty integer NOT NULL,
    v_sum_base_price numeric(16, 6) NOT NULL,
    v_sum_disc_price numeric(16, 6) NOT NULL,
    v_sum_charge numeric(16, 6) NOT NULL,
    v_sum_disc numeric(16, 6) NOT NULL,
    v_count_order integer NOT NULL
);

CREATE INDEX q1_idx ON q1 (l_shipdate);

CREATE TABLE q2 (
    s_suppkey integer NOT NULL,
    n_name char(25) NOT NULL,
    p_partkey integer NOT NULL,
    p_mfgr char(25) NOT NULL,
    p_size integer NOT NULL,
    p_type varchar(25) NOT NULL,
    r_name char(25) NOT NULL
);

CREATE INDEX q2_idx ON q2 (r_name);

CREATE TABLE q2_supplier (
    s_suppkey integer NOT NULL,
    s_acctbal decimal (12,
        2) NOT NULL,
    s_name char(25) NOT NULL,
    s_address varchar(40) NOT NULL,
    s_phone char(15) NOT NULL,
    s_comment varchar(101) NOT NULL,
    PRIMARY KEY (s_suppkey)
);

CREATE TABLE q3 (
    l_orderkey integer NOT NULL,
    o_orderdate date NOT NULL,
    o_shippriority integer NOT NULL,
    l_shipdate date NOT NULL,
    c_mktsegment char(10) NOT NULL,
    l_extendedprice decimal (12,
        2) NOT NULL,
    l_discount decimal (12,
        2) NOT NULL
);

CREATE TABLE q4 (
    o_orderpriority char(15) NOT NULL,
    o_orderdate date NOT NULL,
    agg2 integer NOT NULL
);

CREATE INDEX q4_idx ON q4 (o_orderdate);

CREATE TABLE q5 (
    o_orderdate date NOT NULL,
    r_name char(25) NOT NULL,
    n_name char(25) NOT NULL,
    agg3 numeric(16, 6) NOT NULL
);

CREATE INDEX q5_idx ON q5 (r_name, o_orderdate);

CREATE TABLE q6 (
    l_extendedprice decimal (12,
        2) NOT NULL,
    l_discount decimal (12,
        2) NOT NULL,
    l_shipdate date NOT NULL,
    l_quantity decimal (12,
        2) NOT NULL
);

CREATE INDEX q6_idx ON q6 (l_shipdate, l_discount, l_quantity);

CREATE TABLE q7 (
    supp_nation char(25) NOT NULL,
    cust_nation char(25) NOT NULL,
    l_year integer NOT NULL,
    revenue numeric(16, 6) NOT NULL
);

CREATE INDEX q7_idx ON q7 (supp_nation, cust_nation);

CREATE TABLE q8 (
    r_name char(25) NOT NULL,
    o_year integer NOT NULL,
    volume numeric(16, 6) NOT NULL,
    nation_name char(25) NOT NULL,
    p_type varchar(25) NOT NULL
);

CREATE INDEX q8_idx ON q8 (r_name);

CREATE TABLE q9 (
    wit1__0 boolean NOT NULL,
    wit1__1 boolean NOT NULL,
    wit1__2 boolean NOT NULL,
    wit1__3 boolean NOT NULL,
    wit1__4 boolean NOT NULL,
    wit1__5 boolean NOT NULL,
    wit1__6 boolean NOT NULL,
    wit1__7 boolean NOT NULL,
    wit1__8 boolean NOT NULL,
    wit1__9 boolean NOT NULL,
    wit1__10 boolean NOT NULL,
    wit1__11 boolean NOT NULL,
    nation char(25) NOT NULL,
    o_year integer NOT NULL,
    amount numeric(16, 6) NOT NULL,
    p_name varchar(55) NOT NULL
);

-- CREATE TABLE q10_1 (
--     revenue numeric (16, 6) NOT NULL,
--     n_name char(25) NOT NULL,
--     c_custkey integer NOT NULL
-- );
-- CREATE TABLE q10_2 (
--     c_custkey integer NOT NULL,
--     c_name varchar(25) NOT NULL,
--     c_address varchar(40) NOT NULL,
--     c_nationkey integer NOT NULL,
--     c_phone char(15) NOT NULL,
--     c_acctbal decimal (12,
--         2) NOT NULL,
--     c_mktsegment char(10) NOT NULL,
--     c_comment varchar(117) NOT NULL
-- );
-- CREATE INDEX q10_1_idx ON q10_1 (o_orderdate);
-- CREATE INDEX q10_2_idx ON q10_2 (c_custkey);

CREATE TABLE q11_1 (
    n_name char(25) NOT NULL,
    const33 numeric(18, 2) NOT NULL
);

CREATE INDEX q11_1_idx ON q11_1 (n_name);

CREATE TABLE q11_2 (
    n_name char(25) NOT NULL,
    ps_partkey integer NOT NULL,
    value_ numeric(16, 6) NOT NULL
);

CREATE TABLE q12 (
    l_receiptdate date NOT NULL,
    agg_high integer NOT NULL,
    agg_low integer NOT NULL,
    l_shipmode char(10) NOT NULL
);

CREATE INDEX q12_idx ON q12 (l_receiptdate);

CREATE TABLE q14 (
    l_shipdate date NOT NULL,
    agg1 numeric(16, 6) NOT NULL,
    agg2 numeric(16, 6) NOT NULL
);

CREATE INDEX q14_idx ON q14 (l_shipdate);

CREATE TABLE q15 (
    l_shipdate date NOT NULL,
    s_suppkey integer NOT NULL,
    s_name char(25) NOT NULL,
    s_address varchar(40) NOT NULL,
    s_phone char(15) NOT NULL,
    total_revenue numeric(16, 6) NOT NULL
);

CREATE INDEX q15_idx ON q15 (l_shipdate);

CREATE TABLE q16 (
    p_type varchar(25) NOT NULL,
    p_brand char(10) NOT NULL,
    p_size integer NOT NULL,
    supplier_ct integer NOT NULL
);

CREATE TABLE q17 (
    p_brand char(10) NOT NULL,
    p_container char(10) NOT NULL,
    l_quantity decimal (12,
        2) NOT NULL,
    l_extendedprice decimal (12,
        2) NOT NULL,
    p_partkey integer NOT NULL
);

CREATE INDEX q17_idx ON q17 (p_brand, p_container);

CREATE TABLE q17_2 (
    l_partkey integer NOT NULL,
    l_avgquantity numeric(16, 6) NOT NULL
);

CREATE INDEX q17_idx_2 ON q17_2 (l_partkey);

-- CREATE TABLE q18_1 (
--     l_orderkey integer NOT NULL
-- );
-- CREATE INDEX q18_1_idx ON q18_1 (l_orderkey);
-- CREATE TABLE q18_2 (
--     c_name varchar(25) NOT NULL,
--     c_custkey integer NOT NULL,
--     o_orderdate date NOT NULL,
--     o_orderkey integer NOT NULL,
--     o_totalprice decimal (12,
--         2) NOT NULL
-- );
-- CREATE INDEX q18_2_idx ON q18_2 (o_orderkey);
