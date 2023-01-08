import pandas as pd
import numpy as np
from pyspark.sql import *
data=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
SQLContext.registerDataFrameAsTable(data, "table1")
df=SQLContext.sql("select userid from table1")
df.show()