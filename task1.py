import pandas as pd
import numpy as np
from pandas_legislators import df
data=pd.read_excel('\\Users\\ASUS\\Desktop\\task\\results-20220721-144324.xlsx',header=0)
liste=list(data.user_pseudo_id.unique())
nest={"userid":"events"}
for i in liste:
    liste=[]
    for index,rows in data.iterrows():
        if rows["userid"]==i:
            liste.append(rows["eventname"])
    nest[i]=liste
ss=[]
for j in liste:
    obs=nest[j]
    for i in set(obs):
        counting=obs.count(i)
        if counting>1:



print(nest)




"""
data["obs"]=np.where()
print(data)"""