CREATE TABLE q3 (
  l_orderkey integer NOT NULL,
  o_orderdate date NOT NULL,
  o_shippriority integer NOT NULL,
  l_shipdate date NOT NULL,
  c_mktsegment char(10) NOT NULL,
  l_extendedprice decimal (12,
                           2) NOT NULL,
                           l_discount decimal (12,
                                               2) NOT NULL
);
