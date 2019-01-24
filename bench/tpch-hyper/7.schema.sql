CREATE TABLE q7 (
  supp_nation char(25) NOT NULL,
  cust_nation char(25) NOT NULL,
  l_year integer NOT NULL,
  revenue numeric(16, 6) NOT NULL
);

CREATE INDEX q7_idx ON q7 (supp_nation, cust_nation);
