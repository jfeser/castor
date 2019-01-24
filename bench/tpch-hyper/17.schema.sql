CREATE TABLE q17 (
  p_brand char(10) NOT NULL,
  p_container char(10) NOT NULL,
  l_quantity decimal (12,
                      2) NOT NULL,
                      l_extendedprice decimal (12,
                                               2) NOT NULL,
                                               p_partkey integer NOT NULL
);

CREATE INDEX q17_idx ON q17 (p_brand, p_container);

CREATE TABLE q17_2 (
  l_partkey integer NOT NULL,
  l_avgquantity decimal(16,6) NOT NULL
);

CREATE INDEX q17_idx_2 ON q17_2 (l_partkey);
