WITH
  first1 AS (
  SELECT
    *
  FROM
    `top-cedar-355405.endcol.ec_0619_1808`),
  second AS (
  SELECT
    *,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(first1.event_params)
    WHERE
      key="SN" ) AS key
  FROM
    first1
  WHERE
    event_name IN ("GIST",
      "GISK",
      "GIC",
      "TSC") ),
  third as (
  SELECT
    *
  FROM
    second
  WHERE
    key IN ("NPN",
    "F1U")
    OR key IS NULL
  ORDER BY
    event_timestamp)
select user_pseudo_id, array_agg(struct(event_timestamp,event_name,event_params,user_first_touch_timestamp,version,platform,country)) from third group by user_pseudo_id