from pyhive import hive
import pandas as pd
import sys
conn = hive.Connection(host="localhost", username='hadoopuser' , port=10000, database="logs", auth="NOSASL")

try:
    query= pd.read_sql('select * from workinglogs', conn)
    # Getting dataframe whose start time is above 9:30 AM
    late_commers = query[query['workinglogs.start_time'] > '2019-10-24 09:30:00']
    # Getting user names only
    late_commers_usernames = late_commers['workinglogs.user_name']
    late_commers_usernames.to_csv("data/late_commers_users.csv", index=False)
    # print(late_commers_usernames)
except:
    print("Syntax error")

