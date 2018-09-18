;; define YEAR=random(1998,2002,uniform);
;; define QOY=random(1,2,uniform);
;; define _LIMIT=100;

;; [_LIMITA] select [_LIMITB] ca_zip
;; ,sum(cs_sales_price)
;; from catalog_sales
;; ,customer
;; ,customer_address
;; ,date_dim
;; where cs_bill_customer_sk = c_customer_sk
;; and c_current_addr_sk = ca_address_sk 
;; and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
;;                                       '85392', '85460', '80348', '81792')
;;             or ca_state in ('CA','WA','GA')
;;             or cs_sales_price > 500)
;; and cs_sold_date_sk = d_date_sk
;; and d_qoy = [QOY] and d_year = [YEAR]
;; group by ca_zip
;; order by ca_zip
;; [_LIMITC];

orderby([ca_zip],
  groupby([ca_zip, sum(ca_sales_price)], [ca_zip],
    join(cs_bill_customer_sk = c_customer_sk,
      join(cs_sold_date_sk = d_date_sk,
          catalog_sales,
          filter(d_qoy=param1 && d_year=param0, date_dim)),
      join(c_current_addr_sk = ca_address_sk,
        customer,
        filter(TODO, customer_address)))),
  asc)