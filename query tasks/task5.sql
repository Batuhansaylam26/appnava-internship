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
  third AS (
  SELECT
    user_pseudo_id,
    event_name,
    COUNT(event_name),
    (CASE
        WHEN COUNT(event_name)>1 THEN "1"
      ELSE
      "0"
    END
      ) AS is_event_more_than_one
  FROM
    second
  GROUP BY
    user_pseudo_id,
    event_name )
SELECT
  user_pseudo_id,
  ARRAY_AGG(STRUCT(event_name,
      is_event_more_than_one)) AS event_more_than_one
FROM
  third
GROUP BY
  user_pseudo_id
ORDER BY
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
    user_pseudo_id,
    event_name,
    session_id,
    COUNT(event_name) AS counts,
    (CASE
        WHEN COUNT(event_name)>1 THEN "1"
      ELSE
      "0"
    END
      ) AS is_event_more_than_one
  FROM
    second
  GROUP BY
    user_pseudo_id,
    session_id,
    event_name ),
  forth AS (
  SELECT
    user_pseudo_id,
    session_id,
    ARRAY_AGG(STRUCT(event_name,
        is_event_more_than_one,
        counts)) AS event_more_than_one
  FROM
    third
  GROUP BY
    user_pseudo_id,
    session_id
  ORDER BY
    user_pseudo_id)
SELECT
  user_pseudo_id,
  ARRAY_AGG(STRUCT(session_id,
      event_more_than_one)) AS prop
FROM
  forth
GROUP BY
  user_pseudo_id
ORDER BY
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
    user_pseudo_id,
    event_name,
    COUNT(event_name),
    (CASE
        WHEN COUNT(event_name)>1 THEN "1"
      ELSE
      "0"
    END
      ) AS is_event_more_than_one
  FROM
    second
  GROUP BY
    user_pseudo_id,
    event_name )
SELECT
  user_pseudo_id,
  ARRAY_AGG(STRUCT(event_name,
      is_event_more_than_one)) AS event_more_than_one
FROM
  third
GROUP BY
  user_pseudo_id
ORDER BY
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
    user_pseudo_id,
    event_name,
    session_id,
    COUNT(event_name) AS counts,
    (CASE
        WHEN COUNT(event_name)>1 THEN "1"
      ELSE
      "0"
    END
      ) AS is_event_more_than_one
  FROM
    second
  GROUP BY
    user_pseudo_id,
    session_id,
    event_name ),
  forth AS (
  SELECT
    event_name,
    COUNT(session_id) AS count_session,
    ARRAY_AGG(STRUCT( is_event_more_than_one,
        user_pseudo_id,
        session_id)) AS event_more_than_one
  FROM
    third
  WHERE
    is_event_more_than_one="1"
  GROUP BY
    event_name
  ORDER BY
    event_name)
SELECT
  *
FROM
  forth