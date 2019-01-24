CREATE TABLE q2 (
  s_suppkey integer NOT NULL,
  n_name char(25) NOT NULL,
  p_partkey integer NOT NULL,
  p_mfgr char(25) NOT NULL,
  p_size integer NOT NULL,
  p_type varchar(25) NOT NULL,
  r_name char(25) NOT NULL
);

CREATE INDEX q2_idx ON q2 (r_name);

CREATE TABLE q2_supplier (
  s_suppkey integer NOT NULL,
  s_acctbal decimal (12,
                     2) NOT NULL,
                     s_name char(25) NOT NULL,
                     s_address varchar(40) NOT NULL,
                     s_phone char(15) NOT NULL,
                     s_comment varchar(101) NOT NULL,
                     PRIMARY KEY (s_suppkey)
);
