CREATE TABLE q18_1 (
  l_orderkey integer NOT NULL,
  sum_l_quantity integer not null
);
CREATE INDEX q18_1_idx ON q18_1 (l_orderkey);
CREATE TABLE q18_2 (
  c_name varchar(25) NOT NULL,
  c_custkey integer NOT NULL,
  o_orderkey integer NOT NULL,
  o_orderdate date NOT NULL,
  o_totalprice decimal (12, 2) NOT NULL,
  sum_l_quantity integer not null
);
