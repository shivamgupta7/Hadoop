from pyhive import hive
import pandas as pd
import sys
#Establish the connection between hive server and Database
conn = hive.Connection(host="localhost", username='hadoopuser', port=10000, database="logs", auth="NOSASL")

try:
    #load database into hive
    query = pd.read_sql('select * from workinglogs', conn)
    #convert the data into the format of datetime
    query['workinglogs.working_hours'] = pd.to_datetime(query['workinglogs.working_hours'])
    #calculate the mean of total working_hours
    highest_avghours_log = query[query['workinglogs.working_hours'] > query['workinglogs.working_hours'].mean()]
    # print(highest_avghours_log)
    highest_avghours_log.to_csv("data/highest_avghours_log.csv", index=False)
    # print the user_name with highest average hours
    # highest_avghours_user = highest_avghours_log['workinglogs.user_name']
    # print(highest_avghours_user)
except:
    print("Syntax error")