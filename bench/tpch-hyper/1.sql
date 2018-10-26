select
	l_returnflag,
	l_linestatus,
	sum(v_sum_qty) as sum_qty,
	sum(v_sum_base_price) as sum_base_price,
	sum(v_sum_disc_price) as sum_disc_price,
	sum(v_sum_charge) as sum_charge,
	(sum(v_sum_qty) / count(*)) as avg_qty,
	(sum(v_sum_base_price) / count(*)) as avg_price,
	(sum(v_sum_disc) / count(*)) as avg_disc,
	sum(v_count_order) as count_order
  from
	    q1
 where
	l_shipdate <= date '1998-12-01' - interval '90' day
 group by
	l_returnflag,
	l_linestatus
 order by
	l_returnflag,
	l_linestatus
  ;
