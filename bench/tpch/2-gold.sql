-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998

drop materialized view q2;
create materialized view q2 as
  select
  s_suppkey,
	n_name,
	p_partkey,
	p_mfgr,
  p_size,
  p_type,
  r_name
  from
	part,
	supplier,
	partsupp,
	nation,
	region
  where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and ps_supplycost = (
		select
			min(ps_supplycost)
		  from
			    partsupp,
			  supplier,
			  nation,
			  region as r
		 where
			p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and s_nationkey = n_nationkey
	and n_regionkey = r.r_regionkey
      and region.r_regionkey = r.r_regionkey
	)
  order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey;

create index on q2 (r_name);

explain analyze select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
  from
    q2,
	  supplier
  where
  q2.s_suppkey = supplier.s_suppkey
	and p_size = 15
	and p_type like '%BRASS'
	and r_name = 'EUROPE'
;
