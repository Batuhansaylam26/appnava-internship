from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import pandas as pd
credentials = service_account.Credentials.from_service_account_file(
"C:/Users/ASUS/Desktop/staj/task/top-cedar-355405-bf9f57278a7d.json")


client = bigquery.Client(credentials= credentials,project="top-cedar-355405")
Q1 = """
SELECT
  eventtimestamp
FROM
  `top-cedar-355405.endcol.enlesscolonies_0619_1808` limit 100
"""
# labelling our query job
query_job1 = client.query(Q1,project="top-cedar-355405").to_dataframe()
query_job1