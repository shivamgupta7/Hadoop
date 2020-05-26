from pyspark.sql.functions import hour,minute,second,col,avg,when
from pyspark.sql import SQLContext
import pyspark.sql.functions as sqlFun
from filterAllData import read_file, saveAtHDFS

class SparkQuery:

    def high_avg_working_users(self,df):
        '''
        Find average working hours for each users. And then find who is working more then total average hour.
        '''
        df_work = df.drop('idle_time', 'start_time', 'end_time')  # only take working hour column
        # Find average working hour for each user
        df_avg=df_work.groupBy('user_name').agg(sqlFun.from_unixtime(sqlFun.avg(sqlFun.unix_timestamp('working_hour')),'hh:mm:ss').alias('avg_time'))
        # Convert all into hour
        df_avg_hours = df_avg.withColumn('avg_hour', (hour(df_avg['avg_time'])*3600 + minute(df_avg['avg_time'])*60 + second(df_avg['avg_time']))/3600) 
        #calculating average hours
        total_avg_work_hour = df_avg_hours.select(avg('avg_hour')).collect()[0][0]
        high_working_users = df_avg_hours.filter(df_avg_hours['avg_hour'] > total_avg_work_hour).select('user_name')
        return high_working_users

    def lowest_avg_working_user(self,df):
        '''
        Find average working hours for each users. And then find who is working less then total average hour.
        '''
        df_work = df.drop('idle_time', 'start_time', 'end_time')  # only take working hour column
        # Find average working hour for each user
        df_avg=df_work.groupBy('user_name').agg(sqlFun.from_unixtime(sqlFun.avg(sqlFun.unix_timestamp('working_hour')),'hh:mm:ss').alias('avg_time'))
        # Convert all into hour
        df_avg_hours = df_avg.withColumn('avg_hour', (hour(df_avg['avg_time'])*3600 + minute(df_avg['avg_time'])*60 + second(df_avg['avg_time']))/3600)
        #calculating average hours
        total_avg_work_hour = df_avg_hours.select(avg('avg_hour')).collect()[0][0]
        lowest_working_users = df_avg_hours.filter(df_avg_hours['avg_hour'] < total_avg_work_hour).select('user_name')
        return lowest_working_users

    def high_avg_idle_user(self,df):
        '''
        Find average idle hours for each users. And then find who is idle more then total average hour.
        '''
        df_idle = df.drop('working_hour', 'start_time', 'end_time')  # only take working hour column
        # Find average idle hour for each user
        df_avg=df_idle.groupBy('user_name').agg(sqlFun.from_unixtime(sqlFun.avg(sqlFun.unix_timestamp('idle_time')),'hh:mm:ss').alias('avg_time'))
        # Convert all into hour
        df_avg_hours = df_avg.withColumn('avg_hour', (hour(df_avg['avg_time'])*3600 + minute(df_avg['avg_time'])*60 + second(df_avg['avg_time']))/3600)
        #calculating average hours
        total_avg_idle_hour = df_avg_hours.select(avg('avg_hour')).collect()[0][0]
        high_idle_users = df_avg_hours.filter(df_avg_hours['avg_hour'] > total_avg_idle_hour).select('user_name')
        return high_idle_users
    