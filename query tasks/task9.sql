SELECT
  string_field_0 AS `user_pseudo_id`,
  timestamp_field_1 AS `event_time`,
  string_field_2 AS `event_name`,
  int64_field_3 AS `session_id`
FROM
  `top-cedar-355405.first.forth` ;
WITH
  FIRST AS (
  SELECT
    string_field_0 AS `user_pseudo_id`,
    timestamp_field_1 AS `event_time`,
    string_field_2 AS `event_name`,
    int64_field_3 AS `session_id`
  FROM
    `top-cedar-355405.first.forth` )
SELECT
  user_pseudo_id,
  ARRAY_AGG(STRUCT(event_time,
      event_name,
      session_id)) AS user_prop
FROM
  FIRST
GROUP BY
  user_pseudo_id;
WITH
  second AS (
  SELECT
    string_field_0 AS `user_pseudo_id`,
    timestamp_field_1 AS `event_time`,
    string_field_2 AS `event_name`,
    int64_field_3 AS `session_id`
  FROM
    `top-cedar-355405.first.forth`
  ORDER BY
    timestamp_field_1),
  third AS (
  SELECT
    string_field_0 AS `user_pseudo_id`,
    COUNT(string_field_2) AS count_app_remove
  FROM
    `top-cedar-355405.first.forth`
  WHERE
    string_field_2="app_remove"
  GROUP BY
    string_field_0 )
SELECT
  user_pseudo_id,
  COUNT(event_name) AS event_count,
  COUNT(session_id) AS session_count,
  MIN(event_time) AS time_min,
  MAX(event_time) AS time_max,
  ARRAY_AGG(STRUCT(event_time,
      event_name,
      session_id)) AS user_prop
FROM
  second
GROUP BY
  user_pseudo_id ;
WITH
  second AS (
  SELECT
    string_field_0 AS `user_pseudo_id`,
    timestamp_field_1 AS `event_time`,
    string_field_2 AS `event_name`,
    int64_field_3 AS `session_id`
  FROM
    `top-cedar-355405.first.forth`
  ORDER BY
    timestamp_field_1),
  third AS (
  SELECT
    string_field_0 AS `user_pseudo_id`,
    timestamp_field_1 AS `event_time`,
    string_field_2 AS `event_name`,
    int64_field_3 AS `session_id`
  FROM
    `top-cedar-355405.first.forth`
  ORDER BY
    timestamp_field_1),
  forth AS (
  SELECT
    user_pseudo_id,
    COUNT(event_name) AS event_count,
    COUNT(session_id) AS session_count,
    MIN(event_time) AS time_min,
    MAX(event_time) AS time_max,
    ARRAY_AGG(STRUCT(event_time,
        event_name,
        session_id)) AS user_prop
  FROM
    second
  GROUP BY
    user_pseudo_id )
SELECT
  *,
  (CASE
      WHEN "app_remove" IN UNNEST(forth.user_prop.event_name) THEN "YES"
    ELSE
    "NO"
  END
    ) AS app_remove_exist,
  (
  SELECT
    event_name
  FROM
    UNNEST(forth.user_prop )
  ORDER BY
    event_time DESC
  LIMIT
    1) AS last_event,
  ROUND(DATE_DIFF(time_max,time_min,millisecond)/60000, 2) AS overall_timediff,
  ROUND(DATE_DIFF((
      SELECT
        MAX(event_time)
      FROM (
        SELECT
          event_time
        FROM
          UNNEST(forth.user_prop)
        WHERE
          session_id IS NOT NULL
        LIMIT
          2)),(
      SELECT
        MIN(event_time)
      FROM (
        SELECT
          event_time
        FROM
          UNNEST(forth.user_prop)
        WHERE
          session_id IS NOT NULL
        LIMIT
          2)),millisecond)/60000,2) AS first_session_duration
FROM
  forth