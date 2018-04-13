((name query84)
 (params (
          (city ((String Midway)))
          (income ((Int 45000)))
          ))
   (sql "
 select c_customer_id, c_last_name, c_first_name
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  $1
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  $0
   and ib_upper_bound   <=  $0 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 100;
")
   (query "
EqJoin(customer.c_current_hdemo_sk, household_demographics.hd_demo_sk,
EqJoin(customer.c_current_cdemo_sk, customer_demographics.cd_demo_sk,
EqJoin(customer.c_current_addr_sk, customer_address.ca_address_sk,
customer, Filter(customer_address.ca_city = city:string, customer_address)),
EqJoin(store_returns.sr_cdemo_sk, customer_demographics.cd_demo_sk,
store_returns, customer_demographics)),
EqJoin(income_band.ib_income_band_sk, household_demographics.hd_income_band_sk,
Filter(income_band.ib_lower_bound >= income:int && income_band.ib_upper_bound <= income:int + 50000, income_band), household_demographics))
")
   )
