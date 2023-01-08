WITH
  second AS (
  SELECT
    s AS `user_pseudo_id`,
    t AS `event_time`,
    s AS `event_name`,
    i AS `session_id`
  FROM
    `top-cedar-355405.first.forth`
  ORDER BY
    timestamp_field_1),
  forth AS (
  SELECT
    user_pseudo_id,
    COUNT(event_name) AS event_count,
    COUNT(session_id) AS session_count,
    ARRAY_AGG(STRUCT(event_time,
        event_name,
        session_id)) AS user_prop
  FROM
    second
  GROUP BY
    user_pseudo_id),
  fifth AS(
  SELECT
    *,
    (
    SELECT
      MIN(event_time)
    FROM (
      SELECT
        event_time
      FROM
        UNNEST(forth.user_prop )
      WHERE
        session_id IS NOT NULL
      ORDER BY
        event_time
      LIMIT
        2)) AS time_min,
    (
    SELECT
      MAX(event_time)
    FROM (
      SELECT
        event_time
      FROM
        UNNEST(forth.user_prop )
      WHERE
        session_id IS NOT NULL
      ORDER BY
        event_time
      LIMIT
        2)) AS time_max
  FROM
    forth )
SELECT
  *,
  (CASE
      WHEN "app_remove" IN UNNEST(fifth.user_prop.event_name) THEN "YES"
    ELSE
    "NO"
  END
    ) AS app_remove_exist,
  (
  SELECT
    event_name
  FROM
    UNNEST(fifth.user_prop )
  ORDER BY
    event_time DESC
  LIMIT
    1) AS last_event,
  ROUND(DATE_DIFF(time_max,time_min,millisecond)/60000, 2) AS first_session_duration,
  ROUND(DATE_DIFF((
      SELECT
        MAX(event_time)
      FROM (
        SELECT
          event_time
        FROM
          UNNEST(fifth.user_prop)
        WHERE
          session_id IS NOT NULL )),(
      SELECT
        MIN(event_time)
      FROM (
        SELECT
          event_time
        FROM
          UNNEST(fifth.user_prop)
        WHERE
          session_id IS NOT NULL )),millisecond)/60000,2) AS overall_duration
FROM
  fifth