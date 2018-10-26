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
      q2,
	  q2_supplier
 where
  q2.s_suppkey = supplier.s_suppkey
	and p_size = 15
	and p_type like '%BRASS'
	and r_name = 'EUROPE'
 order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
;
