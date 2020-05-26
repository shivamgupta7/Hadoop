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
