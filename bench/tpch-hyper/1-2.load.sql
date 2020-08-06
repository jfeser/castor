COPY q1
FROM
'q1.tbl' DELIMITER '|';
COPY q2
FROM
'q2.tbl' DELIMITER '|';
COPY q2_supplier
FROM
'q2_supplier.tbl' DELIMITER '|';
