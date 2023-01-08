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
  fifth AS (
  SELECT
    user_pseudo_id,
    event_name,
    session_id,
    counts,
    is_event_more_than_one,
    DENSE_RANK() OVER(PARTITION BY user_pseudo_id, session_id ORDER BY counts DESC ) AS ranks
  FROM
    third
  ORDER BY
    ranks ),
  forth AS (
  SELECT
    user_pseudo_id,
    session_id,
    ARRAY_AGG(STRUCT(event_name,
        is_event_more_than_one,
        counts,
        ranks)) AS event_more_than_one
  FROM
    fifth
  WHERE
    ranks <=3
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
  user_pseudo_id