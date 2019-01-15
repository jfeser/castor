select o_year, sum(case
		               when nation_name = 'BRAZIL' then volume
		               else 0
	                 end) / sum(volume) as mkt_share
  from q8
 where p_type = 'ECONOMY ANODIZED STEEL' and r_name = 'AMERICA';
