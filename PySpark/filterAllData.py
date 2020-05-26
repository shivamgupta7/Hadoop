import datetime as dt
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as sqlFun
import subprocess
from pyspark.sql.types import *

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