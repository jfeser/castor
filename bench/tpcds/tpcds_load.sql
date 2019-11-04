-- 
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- Gradient Systems
--
copy dbgen_version from '/tmp/dbgen_version.dat' with (format csv, delimiter '|');

copy customer_address from '/tmp/customer_address.dat'  with (format csv, delimiter '|');

copy customer_demographics from '/tmp/customer_demographics.dat'  with (format csv, delimiter '|');

copy date_dim from '/tmp/date_dim.dat'  with (format csv, delimiter '|');

copy warehouse from '/tmp/warehouse.dat'  with (format csv, delimiter '|');

copy ship_mode from '/tmp/ship_mode.dat'  with (format csv, delimiter '|');

copy time_dim from '/tmp/time_dim.dat'  with (format csv, delimiter '|');

copy reason from '/tmp/reason.dat'  with (format csv, delimiter '|');

copy income_band from '/tmp/income_band.dat'  with (format csv, delimiter '|');

copy item from '/tmp/item.dat'  with (format csv, delimiter '|');

copy store from '/tmp/store.dat'  with (format csv, delimiter '|');

copy call_center from '/tmp/call_center.dat'  with (format csv, delimiter '|');

copy customer from '/tmp/customer.dat'  with (format csv, delimiter '|');

copy web_site from '/tmp/web_site.dat'  with (format csv, delimiter '|');

copy store_returns from '/tmp/store_returns.dat'  with (format csv, delimiter '|');

copy household_demographics from '/tmp/household_demographics.dat'  with (format csv, delimiter '|');

copy web_page from '/tmp/web_page.dat'  with (format csv, delimiter '|');

copy promotion from '/tmp/promotion.dat'  with (format csv, delimiter '|');

copy catalog_page from '/tmp/catalog_page.dat'  with (format csv, delimiter '|');

copy inventory from '/tmp/inventory.dat'  with (format csv, delimiter '|');

copy catalog_returns from '/tmp/catalog_returns.dat'  with (format csv, delimiter '|');

copy web_returns from '/tmp/web_returns.dat'  with (format csv, delimiter '|');

copy web_sales from '/tmp/web_sales.dat'  with (format csv, delimiter '|');

copy catalog_sales from '/tmp/catalog_sales.dat'  with (format csv, delimiter '|');

copy store_sales from '/tmp/store_sales.dat'  with (format csv, delimiter '|');
