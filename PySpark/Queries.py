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
    
    def lowest_avg_idle_user(self, df):
        '''
        Find average idle hours for each users. And then find who is idle less then total average hour.
        '''
        df_idle = df.drop('working_hour', 'start_time', 'end_time')  # only take working hour column
        # Find average idle hour for each user
        df_avg=df_idle.groupBy('user_name').agg(sqlFun.from_unixtime(sqlFun.avg(sqlFun.unix_timestamp('idle_time')),'hh:mm:ss').alias('avg_time'))
        # Convert all into hour
        df_avg_hours = df_avg.withColumn('avg_hour', (hour(df_avg['avg_time'])*3600 + minute(df_avg['avg_time'])*60 + second(df_avg['avg_time']))/3600)
        #calculating average hours
        total_avg_idle_hour = df_avg_hours.select(avg('avg_hour')).collect()[0][0]
        lowest_idle_users = df_avg_hours.filter(df_avg_hours['avg_hour'] < total_avg_idle_hour).select('user_name')
        return lowest_idle_users

    def late_commers_user(self, df):
        '''
        Find all user who comes late means after 10:00:00. Count ecach user how many time come late. 
        '''
        late_time = '10:00:00'
        df_start = df.drop('working_hour','end_time', 'idle_time')
        df_start = df_start.withColumn('start_time',sqlFun.from_unixtime(sqlFun.unix_timestamp(df_start.start_time),'hh:mm:ss'))
        late_commers = df_start.filter(sqlFun.col('start_time') > late_time)
        late_user_count = late_commers.groupBy('user_name').count()
        return late_user_count

    def leavesAndPresent(self, df, working_day=4):
        '''
        Find each user leaves and present in total working day.
        '''
        df_present = df.groupBy('user_name').agg(sqlFun.count('user_name').alias('present'))
        df_leave_present = df_present.withColumn('leave', working_day - df_present.present)
        return df_leave_present

def start_program(obj, df):
    # displaying main menu
    print('''-----------Query Menu-----------
        Enter 1. To find highest average working user
        Enter 2. To find lowest average working user
        Enter 3. To find highest average idle user
        Enter 4. To find lowest average idle user
        Enter 5. To find late commers user
        Enter 6. To find leaves and presents in working day for each user
        Enter 7. For Exit
        ''')
    try:
        choice = int(input("Enter your choice: "))
    except Exception as e:  # handling the exception for bad input
        print(e, "\n!!! Invalid Input !!!\n")
    try:
        switcher = {
            1 : obj.high_avg_working_users,
            2 : obj.lowest_avg_working_user,
            3 : obj.high_avg_idle_user,
            4 : obj.lowest_avg_idle_user,
            5 : obj.late_commers_user,
            6 : obj.leavesAndPresent,
            7 : exit()
        }
        func = switcher.get(choice, lambda: print('\nInvalid choice please select correct options.'))
        return func(df)
    except Exception as e:
        print("\n",e)

def main():
    #  Read csv file from hdfs server
    input_path = '/user/spark-file/output'
    output_path = '/user/spark-file/query-output'
    df_log = read_file(input_path,header=True, inferSchema = True)
    print('Rows : ',df_log.count(), 'Columns : ',len(df_log.columns))    # total no of row and column
    obj = SparkQuery()
    output = start_program(obj, df_log)
    saveAtHDFS(output, output_path)
    options = input('\nDo you want to continue?[y/n]: ')
    if options.lower() == 'y':
        main()
    else:
        exit()

if __name__ == "__main__":
    main()