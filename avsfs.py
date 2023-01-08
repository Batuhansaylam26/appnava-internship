from itertools import count
from venv import create
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import pyspark.sql.functions as f
import pandas as pd
import numpy as np
spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()
data=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
d1=data
d1["counts"]=d1.groupby(["userid","eventname"])["eventtime"].transform("count")
d1['h'] = np.where(d1['counts'] >1 , 1, 0)
print(d1.head())


df=spark.createDataFrame(data)
df2=df.select("userid","eventname","eventtime")
df3=df2.groupBy("userid","eventname").count().select("userid","eventname",f.col("count").alias("counts"))
df4=df3.withColumn('D', f.when(f.col('counts') > 1, 1).otherwise(0))
dataf=df4.select("*").distinct().toPandas()
print(dataf)