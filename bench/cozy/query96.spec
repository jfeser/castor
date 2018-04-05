Query96:
  state store_sales : Bag<{ ss_sold_time_sk : Int }>
  state household_demographics : Bag<(Int)>
  state time_dim : Bag<{ t_minute : Int, t_hour : Int, t_time_sk : Int }>
  state store : Bag<(Int)>

  query main(hour: Int, depcnt: Int)
    len [t1 | t1 <- store_sales, t2 <- [t | t <- time_dim, t.t_minute >= 30 and t.t_hour == hour], t1.ss_sold_time_sk == t2.t_time_sk]
