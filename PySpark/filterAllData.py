import datetime as dt
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as sqlFun
import subprocess
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

def show_files(path):
    '''
    Show files on given path in HDFS and return all existing paths.
    '''
    some_path = 'hdfs dfs -ls {0}'.format(path).split()
    files = subprocess.check_output(some_path,encoding='utf8').strip().split('\n')
    paths = []
    for path in files[1:]:
        # print(path)
        paths.append(path)
    return paths

def read_file(path,header,inferSchema):
    '''
    Reading file from HDFS.
    '''
    try:
        df = sqlContext.read.csv('hdfs://{0}'.format(path),header = header, inferSchema = inferSchema)
        return df
    except Exception as err:
        print(err)

def cal_idle_hour(detail,users_data):
    '''
    Calculate idle hours. If user do not move mouse and press keyboard in 30 mins or more.
    '''
    global count_idle
    if detail.keyboard == 0 and detail.mouse == 0:
        count_idle += 1
    else:
        count_idle = 0
    if count_idle >= 5:
        if count_idle == 5:
            users_data[detail.user_name]['idle_time'] = users_data[detail.user_name].get('idle_time') + dt.timedelta(0, 1500)
        else:
            users_data[detail.user_name]['idle_time'] = users_data[detail.user_name].get('idle_time') + dt.timedelta(0, 300)

def cal_working_hours(details,date,initial_time,users_data):
    '''
    Calculate working hours. After cal start_time, end_time and idle_time then find working hour = (end_time -start_time) - idle_time
    '''
    delta = (dt.datetime.strptime(date + initial_time, '%Y-%m-%d %H:%M:%S') + (details.end_time - details.start_time)) - details.idle_time
    users_data[details.user_name]['working_hour'] = dt.datetime.strptime(date + initial_time, '%Y-%m-%d %H:%M:%S') + delta
    users_data[details.user_name]['start_time'] = details.start_time
    users_data[details.user_name]['end_time'] = details.end_time

def complete_operation(directory, start_timing, end_timing, initial_time, df_total):
    '''
    In this function doing all operation(like:find start_time, end_time, join tables and uniun all tables and return all combine dataframe into one dataframe
    '''
    try:
        paths = show_files(directory)
        users_data = dict()
        for path in paths:
            path = path.split()[-1]
            log = read_file(path,header=True,inferSchema=True)
            select_column = ['DateTime', 'user_name', 'keyboard', 'mouse']
            log = log.select(*select_column)
            date = dt.datetime.strftime(log.collect()[0][0],'%Y-%m-%d')
            print('Start ' + date)
            log = log[log['DateTime'] >= date + start_timing]
            log = log[log['DateTime'] <= date + end_timing]
            unique_users = log.select('user_name').distinct().rdd.map(lambda r: r[0]).collect()
            start_DateTime = dt.datetime.strptime(date + initial_time, '%Y-%m-%d %H:%M:%S')
    #         print(start_DateTime)
            for user in unique_users:
                users_data[user] = {
                'start_time' : start_DateTime,
                'end_time' : start_DateTime,
                'idle_time': start_DateTime,
                'working_hour' : start_DateTime
                }
            log = log.sort('DateTime')
            startTime = log.groupBy("user_name").agg(sqlFun.min("DateTime").alias('start_time'))
            # print(startTime.show(5))
            endTime = log.groupBy("user_name").agg(sqlFun.max("DateTime").alias('end_time'))
            # print(endTime.show(5))
            for user in unique_users:
                count_idle = 0
                for row in log.rdd.collect():
                    if user == row.user_name:
                        cal_idle_hour(row,users_data)
            # print('Done! Idle hour.')
            data_rows_idle = [Row(**{'user_name': user, **logs}) for user,logs in users_data.items()]
            idleTime = spark.createDataFrame(data_rows_idle).select('user_name', 'idle_time')
            # print(idleTime.show(5))
            data_df = idleTime.join(startTime, on=['user_name'], how='inner').join(endTime, on=['user_name'], how='inner')
            # data_df.show(5)
            for row in data_df.rdd.collect():
                cal_working_hours(row, date, initial_time,users_data)
            # print('Done! Working hour.')
            data_as_rows = [Row(**{'user_name': user, **logs}) for user,logs in users_data.items()]
            data_final = spark.createDataFrame(data_as_rows).select('user_name', 'start_time', 'end_time', 'idle_time', 'working_hour')
            # data_final.show(5)
            print('Done '+ date)
            df_total = df_total.unionAll(data_final)
            users_data={}
        print('Done!')
        return df_total
    except Exception as err:
        print(err)
