CREATE temp VIEW q2 AS (
  SELECT
    s_suppkey,
    n_name,
    p_partkey,
    p_mfgr,
    p_size,
    p_type,
    r_name
    FROM
        part,
      supplier,
      partsupp,
      nation,
      region
   WHERE
        p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND ps_supplycost = (
      SELECT
        min(ps_supplycost)
        FROM
            partsupp,
          supplier,
          nation,
          region AS r
       WHERE
                p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r.r_regionkey
            AND region.r_regionkey = r.r_regionkey)
);

\copy (select * from q2) to 'q2.tbl' delimiter '|';

CREATE temp VIEW q2_supplier AS (
  SELECT
    s_suppkey,
    s_acctbal,
    s_name,
    s_address,
    s_phone,
    s_comment
    FROM
        supplier
);

\copy (select * from q2_supplier) to 'q2_supplier.tbl' delimiter '|';
