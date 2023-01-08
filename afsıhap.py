import pandas as pd
import numpy as np
d1=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
d1 = d1.groupby(["userid","eventname"]).agg(
    count_event=pd.NamedAgg(column="eventtime", aggfunc="count")
)
d1['is_more_than_one'] = np.where(d1['countevent'] >1 , 1, 0)
print(d1)


d1=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
d1 = d1.groupby(["userid","session_id","eventname"]).agg(
    count_event=pd.NamedAgg(column="eventtime", aggfunc="count")
)
d1['is_more_than_one'] = np.where(d1['countevent'] >1 , 1, 0)
print(d1)


d1=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
d1 = d1.groupby(["userid","eventname"]).agg(
    count_event=pd.NamedAgg(column="eventtime", aggfunc="count")
)
d1['is_more_than_one'] = np.where(d1['countevent'] >1 , 1, 0)
d1["ranks"]=d1.groupby(["userid"])["countevent"].rank(ascending=0)
print(d1.sort_values("ranks"))


d1=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
d1 = d1.groupby(["userid","session_id","eventname"]).agg(
    count_event=pd.NamedAgg(column="eventtime", aggfunc="count")
)
d1['is_more_than_one'] = np.where(d1['count_event'] >1 , 1, 0)
d1["ranks"]=d1.groupby(["userid","session_id"])["countevent"].rank(ascending=1)
print(d1.sort_values("ranks",ascending=False))
