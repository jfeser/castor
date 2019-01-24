CREATE TABLE q12 (
  l_receiptdate date NOT NULL,
  agg_high integer NOT NULL,
  agg_low integer NOT NULL,
  l_shipmode char(10) NOT NULL
);

CREATE INDEX q12_idx ON q12 (l_receiptdate);
