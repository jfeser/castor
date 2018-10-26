create table q1 (
  l_returnflag char(1) not null,
  l_linestatus char(1) not null,
  l_shipdate date not null,
  v_sum_qty integer not null,
  v_sum_base_price numeric(16,6) not null,
  v_sum_disc_price numeric(16,6) not null,
  v_sum_charge numeric(16,6) not null,
  v_sum_disc numeric(16,6) not null,
  v_count_order integer not null
);
create index q1_idx on q1 (l_shipdate);

create table q2 (
  s_suppkey integer not null,
  n_name char(25) not null,
  p_partkey integer not null,
  p_mfgr char(25) not null,
  p_size integer not null,
  p_type varchar(25) not null,
  r_name char(25) not null
);
create index q2_idx on q2 (r_name);

create table q2_supplier (
s_suppkey integer not null,
s_acctbal decimal(12,2) not null,
s_name char(25) not null,
s_address varchar(40) not null,
s_phone char(15) not null,
s_comment varchar(101) not null,
primary key (s_suppkey)
);

create table q6 (
  l_extendedprice decimal(12,2) not null,
  l_discount decimal(12,2) not null,
  l_shipdate date not null,
  l_quantity decimal(12,2) not null
);
create index q6_idx on q6 (l_shipdate, l_discount, l_quantity);
