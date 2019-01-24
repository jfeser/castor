CREATE TABLE q1 (
  l_returnflag char(1) NOT NULL,
  l_linestatus char(1) NOT NULL,
  l_shipdate date NOT NULL,
  v_sum_qty integer NOT NULL,
  v_sum_base_price numeric(16, 6) NOT NULL,
  v_sum_disc_price numeric(16, 6) NOT NULL,
  v_sum_charge numeric(16, 6) NOT NULL,
  v_sum_disc numeric(16, 6) NOT NULL,
  v_count_order integer NOT NULL
);

CREATE INDEX q1_idx ON q1 (l_shipdate);
