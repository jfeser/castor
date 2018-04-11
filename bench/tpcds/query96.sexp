((name query96)
 (params
  ((hour ((Int 20) (Int 15) (Int 16) (Int 8)))
   (depcnt ((Int 0) (Int 1) (Int 2) (Int 3) (Int 4) (Int 5) (Int 6) (Int 7) (Int 8) (Int 9)))))
 (sql "
select count(*) from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = $0
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = $1
    and store.s_store_name = 'ese'
order by count(*)
")
 (query "
Count(EqJoin(store_sales.ss_sold_time_sk,
             time_dim.t_time_sk,
             EqJoin(store_sales.ss_hdemo_sk,
                    household_demographics.hd_demo_sk,
                    EqJoin(store_sales.ss_store_sk,
                           store.s_store_sk,
                           store_sales,
                           store),
                    Filter(household_demographics.hd_dep_count = depcnt:int, household_demographics)),
             Filter(time_dim.t_minute >= 30 && time_dim.t_hour = hour:int, time_dim)))
")
 )
