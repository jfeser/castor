create materialized view q2 as
  select 	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment,
  p_type,
  r_name,
  ps_supplycost,
  p_size
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
  order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
  ;

create materialized view q2a as
  select ps_partkey, ps_supplycost, r_name from
	supplier,
	partsupp,
	nation,
	region
  where
  s_suppkey = ps_suppkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  ;

create index on q2 (p_size, r_name);
create index on q2a (ps_partkey, r_name);

select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
  from
      q2
 where
	p_size = :1
	and p_type like '%:2'
	and r_name = ':3'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from q2a
		 where
      p_partkey = ps_partkey
			and r_name = ':3'
	)
 order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey;

