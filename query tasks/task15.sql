WITH
  cte AS (
  SELECT
    *
  FROM
    `top-cedar-355405.first.seventh`
  WHERE
    event_name="Cu" ),
  cte2 AS(
  SELECT
    (
    SELECT
      value.int_value
    FROM
      UNNEST (cte.event_params)
    WHERE
      key="gasi" ) AS session_id,
    (
    SELECT
      value.string_value
    FROM
      UNNEST (cte.event_params)
    WHERE
      key="Cn" ) AS card_name,
    event_timestamp AS times
  FROM
    cte ),
  cte3 AS (
  SELECT
    session_id,
    card_name,
    COUNT(times) AS cnt
  FROM
    cte2
  GROUP BY
    session_id,
    card_name
  ORDER BY
    cnt DESC )
SELECT
  session_id,
  ARRAY_AGG(STRUCT(card_name,
      cnt)) AS params
FROM
  cte3
GROUP BY
  session_id