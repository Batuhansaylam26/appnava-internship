WITH
firstt as (
  select * from `top-cedar-355405.first.tfunnel`
),
  grouped_events AS (
  SELECT
    event_name,
    COUNT(distinct user_pseudo_id) AS count_event,
    count(*) as all_count,
    count((select value.string_value from unnest(firstt.event_params) where key="Skip" and (value.string_value="True" or value.string_value is null))) as count_skip
  FROM
    firstt
  GROUP BY
    event_name
  ORDER BY
    count_event DESC )
SELECT
  event_name,
  all_count,
  count_event,
  LAG(count_event, 1) OVER (ORDER BY count_event ) AS previous,
  count_event/(
  SELECT
    MAX(count_event)
  FROM
    grouped_events) AS total_percentage,
  count_event/LAG(count_event, 1) OVER (ORDER BY count_event DESC) AS percentage_by_step,
  (count_event-(LAG(count_event, 1) OVER (ORDER BY count_event DESC) )) AS difference,
  count_skip,
  LAG(count_skip, 1) OVER (ORDER BY count_event DESC) AS previous_skip
FROM
  grouped_events
ORDER BY
  count_event DESC