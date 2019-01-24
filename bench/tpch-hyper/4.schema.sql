CREATE TABLE q4 (
  o_orderpriority char(15) NOT NULL,
  o_orderdate date NOT NULL,
  agg2 integer NOT NULL
);

CREATE INDEX q4_idx ON q4 (o_orderdate);
