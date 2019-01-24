CREATE TABLE q15 (
  l_shipdate date NOT NULL,
  s_suppkey integer NOT NULL,
  s_name char(25) NOT NULL,
  s_address varchar(40) NOT NULL,
  s_phone char(15) NOT NULL,
  total_revenue numeric(16, 6) NOT NULL
);

CREATE INDEX q15_idx ON q15 (l_shipdate);
