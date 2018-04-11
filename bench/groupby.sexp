((name groupby)
 (params
  ((id ((Int 20) (Int 15) (Int 16) (Int 8)))))
 (sql "select id, min(xpos), max(xpos) from taxi group by id")
 (query "
Agg([taxi.id, Min(taxi.xpos), Max(taxi.xpos)], [taxi.id], taxi)")
 )
