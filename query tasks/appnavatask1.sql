WITH
  second AS (
  SELECT
    stringf AS `userid`,
    timestampf AS `eventtime`,
    stringf AS `eventname`,
    int64f AS `sessionid`
  FROM
    `top-cedar-355405.first.forth`
  ORDER BY
    timestampf),
  forth AS (
  SELECT
    userid,
    sessionid,
    ARRAY_AGG(STRUCT(eventtime,
        ename)) AS userprop
  FROM
    second
  GROUP BY
    sessionid,
    userid ),
  eighth AS (
  SELECT
    userid,
    ARRAY_AGG(STRUCT(eventtime)) AS bb
  FROM
    second
  GROUP BY
    userid),
  prob AS (
  SELECT
    userid,
    ROUND(TIMESTAMP_DIFF((
        SELECT
          MAX(eventtime)
        FROM (
          SELECT
            eventtime
          FROM
            UNNEST(eighth.bb))), (
        SELECT
          MIN(eventtime)
        FROM (
          SELECT
            eventtime
          FROM
            UNNEST(eighth.bb))), millisecond)/60000,2) AS overall_duration
  FROM
    eighth),
  sixth AS(
  SELECT
    userid,
    ARRAY_AGG(STRUCT(sessionid,
        userprop)) AS sessions
  FROM
    forth
  GROUP BY
    userid ),
  fifth AS(
  SELECT
    *,
    (
    SELECT
      MIN(eventtime)
    FROM (
      SELECT
        eventtime
      FROM
        UNNEST(forth.userprop )
      WHERE
        sessionid IS NOT NULL
      ORDER BY
        eventtime )
    LIMIT
      1) AS time_min,
    (
    SELECT
      DISTINCT MAX(eventtime)
    FROM (
      SELECT
        eventtime
      FROM
        UNNEST(forth.userprop )
      WHERE
        sessionid IS NOT NULL
      ORDER BY
        eventtime )
    LIMIT
      1) AS time_max
  FROM
    forth
  ORDER BY
    userid),
  seventh AS (
  SELECT
    userid,
    ARRAY_AGG(STRUCT(sessionid,
        userprop,
        time_min,
        time_max)) AS session
  FROM
    fifth
  GROUP BY
    userid
  ORDER BY
    userid),
  tr AS (
  SELECT
    userid,
    sessionid,
    ARRAY_AGG(STRUCT(eventtime)) AS bb2
  FROM
    second
  GROUP BY
    userid,
    sessionid ),
  tr1 AS (
  SELECT
    userid,
    ROUND(TIMESTAMP_DIFF((
        SELECT
          MAX(eventtime)
        FROM (
          SELECT
            eventtime
          FROM
            UNNEST(tr.bb2))), (
        SELECT
          MIN(eventtime)
        FROM (
          SELECT
            eventtime
          FROM
            UNNEST(tr.bb2))), millisecond)/60000,2) AS firstduration
  FROM
    tr )
SELECT
  seventh.userid,
  ARRAY_AGG(STRUCT(seventh.session,
      prob.overallduration,
      tr1.firstduration)) AS users
FROM
  prob
JOIN
  seventh
ON
  prob.userid=seventh.userid
JOIN
  tr1
ON
  tr1.userid = seventh.userid
GROUP BY
  userid
ORDER BY
  userid