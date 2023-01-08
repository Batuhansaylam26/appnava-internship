ITH
  grouped_events AS (
  SELECT
    event_name,
    COUNT(*) AS count_event
  FROM
    `top-cedar-355405.first.tfunnel`
  GROUP BY
    event_name
  ORDER BY
    count_event DESC )
SELECT
  event_name,
  count_event,
  LAG(count_event, 1) OVER (ORDER BY count_event DESC) AS up,
  count_event/(
  SELECT
    MAX(count_event)
  FROM
    grouped_events) AS total_percentage,
  count_event/LAG(count_event, 1) OVER (ORDER BY count_event DESC) AS percentage_survival_by_step,
  (count_event-(LAG(count_event, 1) OVER (ORDER BY count_event DESC) )) AS difference
FROM
  grouped_events
ORDER BY
  count_event DESC