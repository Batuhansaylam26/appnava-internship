from google.cloud import bigquery
import os
import pandas_gbq
import pandas as pd
import google.auth
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/ASUS/Desktop/staj/task/top-cedar-355405-bf9f57278a7d.json"
credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery"])
client = bigquery.Client(credentials= credentials,project="top-cedar-355405")

Q1 = """
SELECT
  *
FROM
  `top-cedar-355405.endcol.enlesscolonies_0619_1808` limit 100
"""
# labelling our query job
query_job1 = client.query(Q1)

# results as a dataframe
df1 = query_job1.result().to_dataframe()
df1

"""
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Download/nava-356366-66bd4c050fdf.json"
credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/bigquery"])
client = bigquery.Client(credentials=credentials, project='nava-356366')
import os
import pandas as pd
from google.cloud import bigquery
import google.auth"""