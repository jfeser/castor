CREATE TABLE q14 (
  agg1 numeric(16, 6) NOT NULL,
  agg2 numeric(16, 6) NOT NULL,
  l_shipdate date NOT NULL
);

CREATE INDEX q14_idx ON q14 (l_shipdate);
