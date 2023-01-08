SELECT
  *
FROM
  `top-cedar-355405.first.fifth`
WHERE
  event_name="ACC" ;
SELECT
  DISTINCT event_name
FROM
  `top-cedar-355405.first.fifth` ;
WITH
  main AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ) AS rank1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="CN"
      AND event_name="CU") AS event_names1
  FROM
    `top-cedar-355405.first.fifth`
  ORDER BY
    event_timestamp )
SELECT
  user_pseudo_id,
  ARRAY_AGG(STRUCT(rank1,
      event_timestamp,
      event_name,
      event_names1)) AS params
FROM
  main
GROUP BY
  user_pseudo_id;
WITH
  main AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ) AS rank1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="CN") AS event_names1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="SI") AS session,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      event_name="ACC"
      AND key="Total") AS total
  FROM
    `top-cedar-355405.first.fifth`
  ORDER BY
    event_timestamp )
SELECT
  user_pseudo_id,
  session,
  ARRAY_AGG(STRUCT(rank1,
      event_name,
      event_names1,
      event_params,
      event_timestamp)) AS params
FROM
  main
GROUP BY
  user_pseudo_id,
  session;
WITH
  main AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ) AS rank1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="CN") AS event_names1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="SI") AS session,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      event_name="ACC"
      AND key="Total") AS total
  FROM
    `top-cedar-355405.first.fifth`
  ORDER BY
    event_timestamp )
SELECT
  user_pseudo_id,
  session,
  ARRAY_AGG(STRUCT(rank1,
      event_name,
      event_names1,
      total)) AS params
FROM
  main
GROUP BY
  user_pseudo_id,
  session;
SELECT
  DISTINCT (
  SELECT
    value.string_value
  FROM
    UNNEST(event_params)
  WHERE
    key="CN")
FROM
  `top-cedar-355405.first.fifth` ;
WITH
  main AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ) AS rank1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="CN") AS event_names1,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      key="SI") AS session,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      event_name="ACC"
      AND key="Total") AS total
  FROM
    `top-cedar-355405.first.fifth`
  ORDER BY
    event_timestamp ),
  third AS (
  SELECT
    user_pseudo_id,
    session,
    ARRAY_AGG(STRUCT(rank1,
        event_name,
        event_names1,
        total)) AS params
  FROM
    main
  GROUP BY
    user_pseudo_id,
    session),
  forth AS (
  SELECT
    user_pseudo_id,
    session,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="M2x") AS M2x,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="CS") AS CSleanShot,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="M") AS M,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="DD") AS DDrDiet,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="L3Pts") AS L3Pts,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="P") AS P,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="FZ") AS FZ,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="G") AS G,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="SM") AS SM,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="CPlus10") AS CPlus10,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_names1="BT") AS BT,
    (
    SELECT
      COUNT(event_names1)
    FROM
      UNNEST(params)
    WHERE
      event_name="XL") AS XL
  FROM
    third)
SELECT
  user_pseudo_id,
  session,
  ARRAY_AGG(STRUCT(M2x,
      CS,
      M,
      DD,
      L3Pts,
      P,
      FZ,
      G,
      SM,
      CPlus10,
      BT,
      Xl)) AS params
FROM
  forth
GROUP BY
  user_pseudo_id,
  session ;