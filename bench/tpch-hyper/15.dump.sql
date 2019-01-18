CREATE temp VIEW q15 AS (
select l_shipdate_k, "s_suppkey", 
       "s_name",
       "s_address",
       "s_phone",
       "total_revenue" from (SELECT DISTINCT "l_shipdate" as l_shipdate_k FROM "lineitem") AS "t154",
            LATERAL (
                SELECT
                    "s_suppkey", 
                    "s_name",
                    "s_address",
                    "s_phone",
                    "total_revenue"
                FROM (
                    SELECT
                      "s_suppkey", 
                      "s_name",
                      "s_address",
                      "s_phone",
                      "total_revenue"
                    FROM (
                        SELECT
                          "s_suppkey", 
                          "s_name",
                          "s_address",
                          "s_phone",
                          "total_revenue"
                        FROM supplier,
                            (
                                SELECT
                                    "l_suppkey" as supplier_no,
                                    sum(("l_extendedprice") * ((1) - ("l_discount"))) AS "total_revenue"
                                FROM
                                    "lineitem"
                                WHERE ((("l_shipdate") >= l_shipdate_k)
                                    AND (("l_shipdate") < (("l_shipdate_k") + (interval '(3) month'))))
                            GROUP BY
                                ("l_suppkey")) as t45
                        WHERE (("s_suppkey") = ("supplier_no"))) as g, (
                        SELECT
                            max("total_revenue_i") AS "max_total_revenue"
                        FROM (
                            SELECT
                                l_suppkey AS "supplier_no",
                                sum((l_extendedprice) * ((1) - ("l_discount"))) AS "total_revenue_i"
                            FROM
                                "lineitem" 
                            WHERE ((("l_shipdate") >= ("l_shipdate_k"))
                                AND (("l_shipdate") < (("l_shipdate_k") + (interval '(3) month'))))
                        GROUP BY
                            ("l_suppkey")) AS "t56") AS "t108"
                WHERE (("total_revenue") = ("max_total_revenue"))) as h) as t23
);
\copy (select * from q15) to 'q15.tbl' delimiter '|';
