WITH
  forth AS(
  SELECT
    event_type,
    event_properties_game,
    event_properties_stage,
    event_properties_difficulty,
    event_properties_category
  FROM
    `top-cedar-355405.bdfdxbxb.third`
  WHERE
    event_type="GE" )
SELECT
  event_type,
  event_properties_game,
  event_properties_stage,
  ARRAY_AGG(STRUCT(event_properties_difficulty )) AS properties,
FROM
  forth
GROUP BY
  event_type,
  event_properties_category,
  event_properties_game,
  event_properties_stage
ORDER BY
  event_type,
  event_properties_category,
  event_properties_game,
  event_properties_stage;
WITH
  fifth AS(
  SELECT
    event_type,
    event_properties_content,
    event_properties_percent,
    event_properties_mediatype
  FROM
    `top-cedar-355405.bdfdxbxb.third`
  WHERE
    event_type="CE" )
SELECT
  event_type,
  event_properties_content,
  event_properties_mediatype,
  ARRAY_AGG(STRUCT(event_properties_percent )) AS properties,
FROM
  fifth
GROUP BY
  event_type,
  event_properties_content,
  event_properties_mediatype
ORDER BY
  event_type,
  event_properties_content,
  event_properties_mediatype;
WITH
  sixth AS(
  SELECT
    event_type,
    event_properties_game,
    event_properties_stage,
    event_properties_difficulty,
    event_properties_category,
    event_properties_content,
    event_properties_percent,
    event_properties_mediatype
  FROM
    `top-cedar-355405.bdfdxbxb.third`
  WHERE
    event_type="CE"
    OR event_type="GE" )
SELECT
  event_type,
  event_properties_content,
  event_properties_mediatype,
  event_properties_game,
  event_properties_stage,
  ARRAY_AGG(STRUCT(event_properties_difficulty,
      event_properties_percent )) AS properties,
FROM
  sixth
GROUP BY
  event_type,
  event_properties_content,
  event_properties_mediatype,
  event_properties_game,
  event_properties_stage
ORDER BY
  event_type,
  event_properties_content,
  event_properties_mediatype,
  event_properties_game,
  event_properties_stage;