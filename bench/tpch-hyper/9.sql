SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM
    q9
WHERE
    CASE WHEN ':1' = 'yellow' THEN
        wit1_11
    WHEN ':1' = 'white' THEN
        wit1_10
    WHEN ':1' = 'red' THEN
        wit1_9
    WHEN ':1' = 'purple' THEN
        wit1_8
    WHEN ':1' = 'pink' THEN
        wit1_7
    WHEN ':1' = 'orange' THEN
        wit1_6
    WHEN ':1' = 'navy' THEN
        wit1_5
    WHEN ':1' = 'grey' THEN
        wit1_4
    WHEN ':1' = 'green' THEN
        wit1_3
    WHEN ':1' = 'brown' THEN
        wit1_2
    WHEN ':1' = 'blue' THEN
        wit1_1
    WHEN ':1' = 'black' THEN
        wit1_0
    ELSE
        strpos(p_name, 'green') > 0
    END
ORDER BY
    nation,
    o_year;

