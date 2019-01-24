CREATE TABLE q10_1 (
  revenue numeric (16, 6) NOT NULL,
  n_name char(25) NOT NULL,
  c_custkey integer NOT NULL,
  o_orderdate date NOT NULL
);
CREATE TABLE q10_2 (
  c_custkey integer NOT NULL,
  c_name varchar(25) NOT NULL,
  c_address varchar(40) NOT NULL,
  c_nationkey integer NOT NULL,
  c_phone char(15) NOT NULL,
  c_acctbal decimal (12,
                     2) NOT NULL,
                     c_mktsegment char(10) NOT NULL,
                     c_comment varchar(117) NOT NULL
);
CREATE INDEX q10_1_idx ON q10_1 (o_orderdate);
CREATE INDEX q10_2_idx ON q10_2 (c_custkey);
