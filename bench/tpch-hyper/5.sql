select n_name, sum(agg3) as revenue from q5 where o_orderdate >= date '1994-01-01'
	                                            and o_orderdate < date '1994-01-01' + interval '1' year and n_name = 'ASIA';
