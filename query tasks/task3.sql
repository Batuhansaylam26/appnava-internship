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
  first AS (
  SELECT
    user_pseudo_id,
    session_id,
    ARRAY_AGG(STRUCT(event_time,
        event_name)) AS sessions
  FROM
    second
  GROUP BY
    user_pseudo_id,
    session_id ),
  aa AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(STRUCT(session_id,
        sessions)) AS user_data
  FROM
    first
  GROUP BY
    user_pseudo_id ),
  third AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(STRUCT(event_time)) AS bb
  FROM
    second
  GROUP BY
    user_pseudo_id ),
  prob AS (
  SELECT
    user_pseudo_id,
    ROUND(TIMESTAMP_DIFF((
        SELECT
          MAX(event_time)
        FROM (
          SELECT
            event_time
          FROM
            UNNEST(third.bb))), (
        SELECT
          MIN(event_time)
        FROM (
          SELECT
            event_time
          FROM
            UNNEST(third.bb))), millisecond)/60000,2) AS overall_duration
  FROM
    third ),
  forth AS (
  SELECT
    user_pseudo_id,
    session_id,
    ARRAY_AGG(STRUCT(event_time)) AS bb
  FROM
    second
  GROUP BY
    user_pseudo_id,
    session_id ),
  sixth AS (
  SELECT
    user_pseudo_id,
    FIRST_VALUE(session_id) OVER (PARTITION BY user_pseudo_id ORDER BY user_pseudo_id, event_time) AS session_id,
    event_time
  FROM
    second
  WHERE
    session_id IS NOT NULL ),
  seventh AS (
  SELECT
    user_pseudo_id,
    session_id,
    ARRAY_AGG(STRUCT(event_time)) AS first_session
  FROM
    sixth
  GROUP BY
    user_pseudo_id,
    session_id
  ORDER BY
    user_pseudo_id ),
  fifth AS (
  SELECT
    user_pseudo_id,
    (
    SELECT
      MAX(event_time)
    FROM (
      SELECT
        event_time
      FROM
        UNNEST(seventh.first_session))) AS time_max,
    (
    SELECT
      MIN(event_time)
    FROM (
      SELECT
        event_time
      FROM
        UNNEST(seventh.first_session))) AS time_min,
    ROUND(TIMESTAMP_DIFF((
        SELECT
          MAX(event_time)
        FROM (
          SELECT
            event_time
          FROM
            UNNEST(seventh.first_session))), (
        SELECT
          MIN(event_time)
        FROM (
          SELECT
            event_time
          FROM
            UNNEST(seventh.first_session))), millisecond)/60000,2) AS first_session_duration
  FROM
    seventh ),
  ninth AS (
  SELECT
    user_pseudo_id,
    COUNT(event_name) AS event_count
  FROM
    second
  GROUP BY
    user_pseudo_id ),
  eleven AS (
  SELECT
    DISTINCT user_pseudo_id,
    session_id
  FROM
    second ),
  tenth AS (
  SELECT
    DISTINCT user_pseudo_id,
    COUNT(session_id) AS session_count
  FROM
    eleven
  GROUP BY
    user_pseudo_id ),
  twelve AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(STRUCT(event_name)) AS event_names
  FROM
    second
  GROUP BY
    user_pseudo_id ),
  thirteen AS (
  SELECT
    user_pseudo_id,
    (CASE
        WHEN "app_remove" IN UNNEST(twelve.event_names.event_name) THEN "YES"
      ELSE
      "NO"
    END
      ) AS app_remove_exist
  FROM
    twelve ),
  fifteen AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(STRUCT(event_name,
        event_time)) AS events
  FROM
    second
  GROUP BY
    user_pseudo_id ),
  forteen AS (
  SELECT
    user_pseudo_id,
    (
    SELECT
      event_name
    FROM
      UNNEST(fifteen.events)
    ORDER BY
      event_time DESC
    LIMIT
      1) AS last_event
  FROM
    fifteen )
SELECT
  fifth.*,
  aa.user_data,
  prob.overall_duration,
  ninth.event_count,
  tenth.session_count,
  thirteen.app_remove_exist,
  forteen.last_event
FROM
  prob
LEFT JOIN
  fifth
ON
  prob.user_pseudo_id =fifth.user_pseudo_id
JOIN
  aa
ON
  prob.user_pseudo_id=aa.user_pseudo_id
JOIN
  ninth
ON
  prob.user_pseudo_id=ninth.user_pseudo_id
JOIN
  tenth
ON
  prob.user_pseudo_id=tenth.user_pseudo_id
JOIN
  thirteen
ON
  prob.user_pseudo_id=thirteen.user_pseudo_id
JOIN
  forteen
ON
  prob.user_pseudo_id=forteen.user_pseudo_id
ORDER BY
  prob.user_pseudo_id;