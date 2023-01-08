WITH
  main AS (
  SELECT
    *
  FROM
    `top-cedar-355405.endcol.enlesscolonies_0619_1808`
  WHERE
    event_name="TutorialStepCompleted"
  ORDER BY
    event_timestamp),
  second AS(
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
    part,
    day),
  third AS (
  SELECT
    user_pseudo_id,
    part,
    day,
    hours,
    (
    SELECT
      MAX(times)
    FROM
      UNNEST(second.hours)) AS day1,
    (
    SELECT
      (MAX(times)-MIN(times))
    FROM
      UNNEST(second.hours)) AS duration
  FROM
    second ),
  forth AS (
  SELECT
    user_pseudo_id,
    part,
    ARRAY_AGG(STRUCT(day,
        duration)) AS params
  FROM
    third
  GROUP BY
    user_pseudo_id,
    part),
  fifth AS (
  SELECT
    user_pseudo_id,
    part,
    (
    SELECT
      SUM(duration)
    FROM
      UNNEST(forth.params)) AS total_duration,
    (
    SELECT
      MAX(day)
    FROM
      UNNEST(forth.params)) AS end_day
  FROM
    forth ),
  sixth AS (
  SELECT
    part,
    end_day,
    (CASE
        WHEN part="S" THEN NULL
      ELSE
      SUM(total_duration)
    END
      ) AS summation,
    AVG(total_duration) AS mean
  FROM
    fifth
  WHERE
    end_day>= "2022-08-08"
  GROUP BY
    part,
    end_day
  ORDER BY
    end_day)
SELECT
  part,
  (CASE
      WHEN part="C" THEN 2
      WHEN part="sT" THEN 3
      WHEN part="P" THEN 4
    ELSE
    5
  END
    ) AS partID,
  ARRAY_AGG(STRUCT(end_day,
      mean,
      summation)) AS params
FROM
  sixth
GROUP BY
  part
ORDER BY
  partID;
WITH
  main AS (
  SELECT
    *
  FROM
    `top-cedar-355405.endcol.enlesscolonies_0619_1808`
  WHERE
    event_name="TSC"
  ORDER BY
    event_timestamp),
  second AS(
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
    part,
    day),
  third AS (
  SELECT
    user_pseudo_id,
    part,
    day,
    hours,
    (
    SELECT
      MAX(times)
    FROM
      UNNEST(second.hours)) AS day1,
    (
    SELECT
      (MAX(times)-MIN(times))
    FROM
      UNNEST(second.hours)) AS duration
  FROM
    second ),
  forth AS (
  SELECT
    user_pseudo_id,
    part,
    ARRAY_AGG(STRUCT(day,
        duration)) AS params
  FROM
    third
  GROUP BY
    user_pseudo_id,
    part),
  fifth AS (
  SELECT
    user_pseudo_id,
    part,
    (
    SELECT
      SUM(duration)
    FROM
      UNNEST(forth.params)) AS total_duration,
    (
    SELECT
      MAX(day)
    FROM
      UNNEST(forth.params)) AS end_day
  FROM
    forth ),
  sixth AS (
  SELECT
    part,
    end_day,
    (CASE
        WHEN part="S" THEN NULL
      ELSE
      SUM(total_duration)
    END
      ) AS summation,
    AVG(total_duration) AS mean
  FROM
    fifth
  WHERE
    end_day>= "2022-08-03"
  GROUP BY
    part,
    end_day
  ORDER BY
    end_day)
SELECT
  part,
  (CASE
      WHEN part="C" THEN 2
      WHEN part="St" THEN 3
      WHEN part="P" THEN 4
    ELSE
    5
  END
    ) AS partID,
  ARRAY_AGG(STRUCT(end_day,
      mean,
      summation)) AS params
FROM
  sixth
GROUP BY
  part
ORDER BY
  partID