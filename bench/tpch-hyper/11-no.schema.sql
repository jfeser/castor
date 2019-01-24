CREATE TABLE q11_1 (
  n_name char(25) NOT NULL,
  const33 numeric(18, 2) NOT NULL
);

CREATE INDEX q11_1_idx ON q11_1 (n_name);

CREATE TABLE q11_2 (
  n_name char(25) NOT NULL,
  ps_partkey integer NOT NULL,
  value_ numeric(16, 6) NOT NULL
);
