--
-- PostgreSQL database dump
--

CREATE DATABASE tpch_1k;


\connect tpch_1k

--
-- Name: customer; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.customer (
    c_custkey integer,
    c_name character varying(25),
    c_address character varying(40),
    c_nationkey bigint,
    c_phone character(15),
    c_acctbal numeric,
    c_mktsegment character(10),
    c_comment character varying(117)
);


--
-- Name: lineitem; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.lineitem (
    l_orderkey bigint,
    l_partkey bigint,
    l_suppkey bigint,
    l_linenumber integer,
    l_quantity numeric,
    l_extendedprice numeric,
    l_discount numeric,
    l_tax numeric,
    l_returnflag character(1),
    l_linestatus character(1),
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct character(25),
    l_shipmode character(10),
    l_comment character varying(44)
);



--
-- Name: nation; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.nation (
    n_nationkey integer NOT NULL,
    n_name character(25),
    n_regionkey bigint NOT NULL,
    n_comment character varying(152)
);

--
-- Name: orders; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.orders (
    o_orderkey integer,
    o_custkey bigint,
    o_orderstatus character(1),
    o_totalprice numeric,
    o_orderdate date,
    o_orderpriority character(15),
    o_clerk character(15),
    o_shippriority integer,
    o_comment character varying(79)
);

--
-- Name: part; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.part (
    p_partkey integer,
    p_name character varying(55),
    p_mfgr character(25),
    p_brand character(10),
    p_type character varying(25),
    p_size integer,
    p_container character(10),
    p_retailprice numeric,
    p_comment character varying(23)
);

--
-- Name: partsupp; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.partsupp (
    ps_partkey bigint,
    ps_suppkey bigint,
    ps_availqty integer,
    ps_supplycost numeric,
    ps_comment character varying(199)
);

--
-- Name: region; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.region (
    r_regionkey integer NOT NULL,
    r_name character(25),
    r_comment character varying(152)
);

--
-- Name: supplier; Type: TABLE; Schema: public; Owner: jack
--

CREATE TABLE public.supplier (
    s_suppkey integer,
    s_name character(25),
    s_address character varying(40),
    s_nationkey bigint,
    s_phone character(15),
    s_acctbal numeric,
    s_comment character varying(101)
);


