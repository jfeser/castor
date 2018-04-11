; define IMID  = random(1,1000,uniform); 
; define YEAR  = random(1998,2002,uniform);
; define CSDATE = date([YEAR]+"-01-01",[YEAR]+"-04-01",sales);
; define _LIMIT=100;

; [_LIMITA] select [_LIMITB] sum(cs_ext_discount_amt)  as "excess discount amount" 
; from 
;    catalog_sales 
;    ,item 
;    ,date_dim
; where
; i_manufact_id = [IMID]
; and i_item_sk = cs_item_sk 
; and d_date between '[CSDATE]' and 
;         (cast('[CSDATE]' as date) + 90 days)
; and d_date_sk = cs_sold_date_sk 
; and cs_ext_discount_amt  
;      > ( 
;          select 
;             1.3 * avg(cs_ext_discount_amt) 
;          from 
;             catalog_sales 
;            ,date_dim
;          where 
;               cs_item_sk = i_item_sk 
;           and d_date between '[CSDATE]' and
;                              (cast('[CSDATE]' as date) + 90 days)
;           and d_date_sk = cs_sold_date_sk 
;       ) 
; [_LIMITC]; 
