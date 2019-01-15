select l_shipmode, sum(agg6) as high_line_count, sum(agg7) as low_line_count
  from q12 where l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year and (l_shipmode = 'MAIL' or l_shipmode = 'SHIP') order by l_shipmode;
