from pyhive import hive
import pandas as pd
import sys
# Establish the connection between hive server and Database
conn = hive.Connection(host="localhost", port=10000, username="hadoopuser", database="logs", auth="NOSASL")

try:
    # load database into hive
    query= pd.read_sql('select * from workinglogs', conn)
    # convert the data into the format of datetime
    query['workinglogs.working_hours'] = pd.to_datetime(query['workinglogs.working_hours'])
    # calculate the mean of total working_hours
    lowest_avghour_log = query[query['workinglogs.working_hours'] < query['workinglogs.working_hours'].mean()]
    # print(avghour)
    lowest_avghour_log.to_csv("data/lowest_avghours_user_log.csv", index=False)
    # print the user_name with lowest average hours
    # LOWEST_AVG_HOURS =avghour['workinglogs.user_name']
    # print(LOWEST_AVG_HOURS)
except:
    print("Syntax error")