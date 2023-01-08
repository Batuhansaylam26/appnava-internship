from google.cloud import bigquery
import os 

path="top-cedar-355405-b88a1dbd19c0.json"
os.environ["GOOGLE_APPLICATİON_CREDENTIALS"]=path
client = bigquery.Client()
name_group_query = """
    with second as (
  SELECT string_field_0 as `userid` ,timestampfield as `eventtime`,stringfield as `eventname`,int64 as `session_id`  FROM `top-cedar-355405.first.forth` order by timestamp_field_1),
  third as (
  select userid,eventname,
  (case when count(eventname)>1 then "1" else "0" end) as is_event_more_than_one
   from second group by userid,eventname
  )
  select userid,
  array_agg(struct(eventname,is_event_more_than_one)) as event_more_than_one 
  from third group by userid order by userid
"""
query_results = client.query(name_group_query) 
for row in query_results._result():
    print(row)
