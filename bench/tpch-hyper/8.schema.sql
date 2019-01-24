CREATE TABLE q8 (
  r_name char(25) NOT NULL,
  o_year integer NOT NULL,
  volume numeric(16, 6) NOT NULL,
  nation_name char(25) NOT NULL,
  p_type varchar(25) NOT NULL
);

CREATE INDEX q8_idx ON q8 (r_name);
