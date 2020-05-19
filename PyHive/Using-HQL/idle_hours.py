from pyhive import hive
import pandas as pd
import csv

conn = hive.Connection(host="localhost",port = 10000, username="hadoopuser",database = 'logs',auth='NOSASL')

try:
    cursor = conn.cursor()
    cursor.execute('SELECT user_name, from_unixtime(CAST(AVG(unix_timestamp(substr(idle_time,12),"HH:mm:ss"))as bigint),"HH:mm:ss") as avg_hours FROM workinglogs1 GROUP BY user_name ORDER BY avg_hours')
    result = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    out_file = open('output/avg_idle_hours.csv', 'w')
    myFile = csv.writer(out_file)
    myFile.writerow(column_names)
    myFile.writerows(result)
    out_file.close()
    print('Query run successfully.')
except hive.Error as err:
    print('Syntax error')
    print(err)
finally:
    cursor.close()
    conn.close()
    print("Connection is closed")
