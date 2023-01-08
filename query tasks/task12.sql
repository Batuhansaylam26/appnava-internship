WITH
  main AS (
  SELECT
    user_pseudo_id,
    COUNT(DISTINCT event_name) AS cnt
  FROM
    `top-cedar-355405.endcol.ec_0619_1808`
  GROUP BY
    user_pseudo_id)
SELECT
  main.user_pseudo_id,
  data1.event_name,
  main.cnt
FROM
  main
JOIN
  `top-cedar-355405.endcol.ec_0619_1808` data1
ON
  main.user_pseudo_id=data1.user_pseudo_id
WHERE
  main.cnt=1
  AND data1.event_name="ar" ;
SELECT
  user_pseudo_id,
  event_name
FROM
  `top-cedar-355405.endcol.ec_0619_1808`
WHERE
  user_pseudo_id="532a8d7f265e37a11f429ad590415e0e"