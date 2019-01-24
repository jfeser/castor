CREATE TABLE q5 (
  o_orderdate date NOT NULL,
  n_name char(25) NOT NULL,
  r_name char(25) NOT NULL,
  agg3 numeric(16, 6) NOT NULL
);

CREATE INDEX q5_idx ON q5 (r_name, o_orderdate);
