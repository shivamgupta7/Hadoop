import datetime
import pandas as pd
from datetime import date


usr_log = pd.read_csv("data/CpuLogData2019-10-24.csv")
usr_log = usr_log[['DateTime', 'user_name', 'keyboard', 'mouse']]
usr_log = usr_log[usr_log['DateTime'] >= '2019-10-24 08:30:00']
usr_log = usr_log[usr_log['DateTime'] <= '2019-10-24 19:30:00']
unique_users = usr_log.user_name.unique()

users_new_data = dict()

for user in unique_users:
    users_new_data[user] = {'idle_time': datetime.datetime(2019, 10, 24, 0, 0, 0),
                            'working_hours': datetime.datetime(2019, 10, 24, 0, 0, 0),
                            'start_time': None,
                            'end_time': None,
                            }

usr_log.sort_values(by='DateTime', inplace=True)
for user in unique_users:
    count_idle = 0

    for index in usr_log.index:

        if user == usr_log['user_name'][index]:
            if users_new_data[user]['start_time'] is None:
                users_new_data[user]['start_time'] = usr_log['DateTime'][index]

            if users_new_data[user]['end_time'] is None or users_new_data[user]['end_time'] < usr_log['DateTime'][index]:
                users_new_data[user]['end_time'] = usr_log['DateTime'][index]

            if count_idle >= 5:
                if count_idle == 5:
                    users_new_data[usr_log['user_name'][index]]['idle_time'] \
                        = users_new_data[usr_log['user_name'][index]].get('idle_time') \
                          + datetime.timedelta(0, 1500)
                else:
                    users_new_data[usr_log['user_name'][index]]['idle_time'] \
                        = users_new_data[usr_log['user_name'][index]].get('idle_time') \
                          + datetime.timedelta(0, 300)
            if usr_log['keyboard'][index] == 0 and usr_log['mouse'][index] == 0:
                count_idle += 1
            else:
                count_idle = 0

for user in unique_users:
    str_to_time = datetime.datetime.strptime
    users_new_data[user]['working_hours']\
        = (datetime.datetime(2019, 10, 24, 0, 0, 0)
           + (str_to_time(users_new_data[user].get('end_time'), '%Y-%m-%d %H:%M:%S')
              - str_to_time(users_new_data[user].get('start_time'), '%Y-%m-%d %H:%M:%S')))\
          - users_new_data[user].get('idle_time')
    users_new_data[user]['working_hours'] = datetime.datetime(2019, 10, 24, 0, 0, 0) + users_new_data[user].get('working_hours')


usrlog_data = pd.DataFrame(users_new_data)
usrlog_data = usrlog_data.T
usrlog_data.index.name = "user_name"
usrlog_data['date'] = date(2019,10,24)
usrlog_data.reset_index(inplace=True)
usrlog_data.to_csv("data/UsrLogData2019-10-24.csv", index=False)
# print (usrlog_data)

