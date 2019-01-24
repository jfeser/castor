CREATE TABLE q18_1 (
  c_name varchar(25) NOT NULL,
  c_custkey integer NOT NULL,
  o_orderkey integer NOT NULL,
  o_orderdate date NOT NULL,
  o_totalprice decimal (12, 2) NOT NULL,
  sum_l_quantity integer not null
);
CREATE TABLE q18_2 (
l_orderkey integer NOT NULL,
sum_l_quantity integer not null
);
CREATE INDEX q18_2_idx ON q18_2 (l_orderkey);
