import pandas as pd

df1 = pd.read_csv('data/UsrLogData2019-10-21.csv')
df2 = pd.read_csv('data/UsrLogData2019-10-22.csv')
df3 = pd.read_csv('data/UsrLogData2019-10-23.csv')
df4 = pd.read_csv('data/UsrLogData2019-10-24.csv')

df = df1.append(df2, ignore_index = True)
df = df.append(df3, ignore_index = True)
df = df.append(df4, ignore_index = True)
df.to_csv("data/UsrLogData.csv", index=False)