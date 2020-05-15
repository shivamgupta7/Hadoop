from pyhive import hive
import pandas as pd
import sys
# Establish the connection between hive server and Database
conn = hive.Connection(host="localhost", username='hadoopuser', port=10000, database="logs", auth="NOSASL")

try:
    # load database into hive
    query = pd.read_sql('select * from workinglogs', conn)
    # convert the data into the format of datetime
    query['workinglogs.idle_time'] = pd.to_datetime(query['workinglogs.idle_time'])
    # calculate total idle_hours mean
    highest_idle_hours_data = query[query['workinglogs.idle_time'] > query['workinglogs.idle_time'].mean()]
    # print(highest_idle_hours_data)
    highest_idle_hours_data.to_csv("data/highest_idel_usr_log.csv", index=False)
    # print user_name with highest_idel_hours_user
    # highest_idel_hours_user = highest_idle_hours_data['workinglogs.user_name']
    # print(highest_idel_hours_user)
except:
    print("Syntax error")
