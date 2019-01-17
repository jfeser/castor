SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM
    q9
WHERE
    CASE WHEN ':1' = 'yellow' THEN
        wit1__11
    WHEN ':1' = 'white' THEN
        wit1__10
    WHEN ':1' = 'red' THEN
        wit1__9
    WHEN ':1' = 'purple' THEN
        wit1__8
    WHEN ':1' = 'pink' THEN
        wit1__7
    WHEN ':1' = 'orange' THEN
        wit1__6
    WHEN ':1' = 'navy' THEN
        wit1__5
    WHEN ':1' = 'grey' THEN
        wit1__4
    WHEN ':1' = 'green' THEN
        wit1__3
    WHEN ':1' = 'brown' THEN
        wit1__2
    WHEN ':1' = 'blue' THEN
        wit1__1
    WHEN ':1' = 'black' THEN
        wit1__0
    ELSE
        strpos(p_name, 'green') > 0
    END
group by nation, o_year
ORDER BY
    nation,
    o_year desc;

