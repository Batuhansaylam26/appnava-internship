WITH
  main AS (
  SELECT
    *
  FROM
    `top-cedar-355405.endcol.eC_0619_1808`
  WHERE
    event_name="TIC"
  ORDER BY
    event_timestamp),
  second AS (
  SELECT
    user_pseudo_id,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS day,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="PI") AS part,
    ARRAY_AGG(STRUCT(TIME(TIMESTAMP_MICROS(event_timestamp)) AS times)) AS hours
  FROM
    main
  WHERE
    event_name="TSC"
  GROUP BY
    user_pseudo_id,
    day,
    part),
  third AS (
  SELECT
    *,
    ((
      SELECT
        MAX(times)
      FROM
        UNNEST(second.hours))-(
      SELECT
        MIN(times)
      FROM
        UNNEST(second.hours))) AS duration
  FROM
    second),
  forth AS (
  SELECT
    part,
    day,
    (CASE
        WHEN part="S" THEN NULL
      ELSE
      SUM(duration)
    END
      ) AS summation,
    AVG(duration) AS mean
  FROM
    third
  GROUP BY
    part,
    day)
SELECT
  part,
  ARRAY_AGG(STRUCT(day,
      summation,
      mean)) AS params
FROM
  forth
GROUP BY
  part;
WITH
  main AS (
  SELECT
    *
  FROM
    `top-cedar-355405.endcol.eC_0619_1808`
  WHERE
    event_name IN ("GIST",
      "GISK",
      "GIC",
      "TSC")
  ORDER BY
    event_timestamp),
  second AS (
  SELECT
    user_pseudo_id,
    event_name,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS day,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="PI") AS part,
    ARRAY_AGG(STRUCT(TIME(TIMESTAMP_MICROS(event_timestamp)) AS times)) AS hours
  FROM
    main
  GROUP BY
    user_pseudo_id,
    event_name,
    day,
    part),
  third AS (
  SELECT
    *,
    ((
      SELECT
        MAX(times)
      FROM
        UNNEST(second.hours))-(
      SELECT
        MIN(times)
      FROM
        UNNEST(second.hours))) AS duration
  FROM
    second),
  forth AS (
  SELECT
    event_name,
    part,
    day,
    (CASE
        WHEN part="S" THEN NULL
      ELSE
      SUM(duration)
    END
      ) AS summation,
    AVG(duration) AS mean
  FROM
    third
  GROUP BY
    event_name,
    part,
    day)
SELECT
  event_name,
  part,
  ARRAY_AGG(STRUCT(day,
      summation,
      mean)) AS params
FROM
  forth
GROUP BY
  event_name,
  part
ORDER BY
  event_name,
  part