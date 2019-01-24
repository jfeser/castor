CREATE TABLE q6 (
  l_extendedprice decimal (12,
                           2) NOT NULL,
                           l_discount decimal (12,
                                               2) NOT NULL,
                                               l_shipdate date NOT NULL,
                                               l_quantity decimal (12,
                                                                   2) NOT NULL
);

CREATE INDEX q6_idx ON q6 (l_shipdate, l_discount, l_quantity);
