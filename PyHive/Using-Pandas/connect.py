from pyhive import hive
import pandas as pd

conn = hive.Connection(host="localhost",port = 10000, username="hadoopuser",database = 'logs',auth='NOSASL')

# cur = conn.cursor()
# cur.execute('show databases')
# result = cur.fetchall()
# print(result)
df = pd.read_sql('select * from workinglogs',conn)
print(df)