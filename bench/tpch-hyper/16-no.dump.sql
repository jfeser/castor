CREATE temp VIEW q16 AS (
  SELECT
    p_type,
    p_brand,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
    FROM
        part,
      partsupp
   WHERE
        p_partkey = ps_partkey
    AND ps_suppkey NOT IN (
      SELECT
        s_suppkey
        FROM
            supplier
       WHERE
                s_comment LIKE '%Customer%Complaints%')
   GROUP BY
            p_type,
            p_brand,
            p_size
);

\copy (select * from q16) to 'q16.tbl' delimiter '|';
