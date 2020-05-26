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
