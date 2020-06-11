# For USF SAIL
# ~Mihir
import botometer
import _json
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from flatten_json import  flatten
import tweepy
import  requests
import datetime

#
#-----------------------
# auth keys and other stuff here

# now it's called rapidapi key (it's in your app, then security)
rapidapi_key = "RAPID_API_PRO_KEY"

# twitter authorization keys
twitter_app_auth = {
    'consumer_key': 'TWITTER_KEY',
    'consumer_secret': 'TWITTER_KEY',
    'access_token': 'TWITTER_KEY',
    'access_token_secret': 'TWITTER_KEY'
}

# Pro API endpoint(HAVE TO PASS THIS!! Currently default is wrong in the package)
botometer_api_url = 'https://botometer-pro.p.rapidapi.com'

# make a method of botometer
bom = botometer.Botometer(botometer_api_url=botometer_api_url,
                          wait_on_ratelimit=True,
                          rapidapi_key=rapidapi_key,
                          **twitter_app_auth)

#-------------------------

# now the plan is to see how long it takes to do this portion with pro-version, 17200 / day quota

chunksize2 = 150 #17200 is daily, the lesser the chunk, higher the numbers of files creations

#*********************** UPDATE THE PATH HERE!!! *************************#
try:
   os.mkdir("./BOT_CSV_USC_MAY") # <---- Here
except OSError as e:
   print("Directory exists. Please change both folders names in this block to whatever.")
   exit()

path = './BOT_CSV_USC_MAY/' # <--- Here too

#************************************************************************#

data2 = pd.read_csv('/work_bgfs/m/mkpatel/USC Data_portions_and Stuff/2020-05/2020-05_USC_en_portions/output_Mihir_1_may.csv',
                chunksize=chunksize2,
                encoding = "ISO-8859-1", usecols=['user_screen_name'])
df2 = data2.get_chunk(chunksize2)
headers = list(df2.keys())
del data2

start_chunk = 0
#daily_quota_count=0
inner_itr_count = 0
incremented_date = datetime.datetime.now().date()+datetime.timedelta(days=1)

data2 = pd.read_csv('/work_bgfs/m/mkpatel/USC Data_portions_and Stuff/2020-05/2020-05_USC_en_portions/output_Mihir.csv',
                chunksize=chunksize2,
                encoding = "ISO-8859-1",
                skiprows=chunksize2*start_chunk, usecols=['user_screen_name'])

for i, df2 in enumerate(data2):
    try:
        print('reading csv...')
        print(df2)
        # print('header: ', list(df2.keys()))
        # print('file header: ', headers)
        #daily_quota_count = daily_quota_count + df2.shape[0] #IMPORATANT! keeps track of when to sleep

        # Access chunks within data
        for chunk in df2:

            data = []
            current_df = df2

            # REMINDER: 17280 / day quota + 0.0001 afterwards! ~(OUTDATED!)~
            #************************************#
            #rapidapi_pro_daily_free = 17280
            #************************************#

            for ind in current_df.index: #ok to run this the slow down from twitter will prevent daily calls
            #for ind in range(0, rapidapi_pro_daily_free):  # otherwise keep this line so we don't get surcharge

                inner_itr_count  = inner_itr_count+1 #incerement how much things get processed
                username_part = current_df['user_screen_name'][ind]
                print("inner_itr_count is: ", inner_itr_count)
                # try-catch the handle all the botometer errors, can be cleaned with R so NA and not NaN
                try:
                    botometer_result_part = bom.check_account(current_df['user_screen_name'][ind])
                except:
                    botometer_result_part = {'cap': {'english': 0, 'universal': 0},
                                             'categories': {'content': 0, 'friend': 0,
                                                            'network': 0, 'sentiment': 0,
                                                            'temporal': 0, 'user': 0},
                                             'display_scores': {'content': 0, 'english': 0, 'friend': 0, 'network': 0,
                                                                'sentiment': 0, 'temporal': 0, 'universal': 0,
                                                                'user': 0},
                                             'scores': {'english': 0, 'universal': 0},
                                             'user': {'id_str': '0', 'screen_name': 'NA'}}
                    data.append([username_part, botometer_result_part])
                    continue

                print('elapsed time:', datetime.datetime.now())
                #put scrore into a dataframe
                data.append([username_part, botometer_result_part])

            # ----------------------

            # here we want to seperate json fields into new columns

            # Convert the list of lists to a list of dicts
            for x in data:
                x[1]['name'] = x[0]

            data2 = [x[1] for x in data]

            # proces lists of lists by flattening it
            def flatten_json(nested_json: dict, exclude: list = [''], sep='_') -> dict:

                # flatten a list of nested dicts.
                out = dict()
                # make them into a column
                def flatten(x: (list, dict, str), name: str = '', exclude=exclude):
                    if type(x) is dict:
                        for a in x:
                            if a not in exclude:
                                flatten(x[a], f'{name}{a}{sep}')
                    elif type(x) is list:
                        i = 0
                        for a in x:
                            flatten(a, f'{name}{i}{sep}')
                            i += 1
                    else:
                        out[name[:-1]] = x

                flatten(nested_json)
                return out

            df = pd.DataFrame([flatten_json(x) for x in data2])

            # select the columns that I want to keep otherwise just comment this portion with #, that way you get all
            df = df[['user_screen_name', 'display_scores_english', 'display_scores_universal',
                           'cap_english', 'cap_universal']]

            # remove all the NA botometer rows
            df = df.drop(df[df.user_screen_name == 'NA'].index)

            # You can now export all outcomes in new csv files
            file_name = 'export_csv_' + str(start_chunk+i) + '.csv'
            save_path = os.path.abspath(
                os.path.join(
                    path, file_name
                )
            )
        print('saving ...')
        df.to_csv(save_path,encoding='utf-8', index=False)

        # this piece is added so if you process your 17250 accounts, it will sleep for a day
        if (inner_itr_count >= 17250 and datetime.datetime.now().day == datetime.datetime.now().day):
            # sleep until 12AM of that day
            # the number of seconds plus some more (for crappy leap year)
            seconds_in_a_day = 86410
            # current date with zero time (00:00:00)
            dt = datetime.datetime.now()
            midnight = datetime.datetime.combine(dt.date(), datetime.time())
            # the number of seconds since the beginning of the day
            seconds_since_midnight = (dt - midnight).seconds
            # the number of seconds remaining until the end of the day
            seconds_until_end_of_day = seconds_in_a_day - seconds_since_midnight
            print("Sleeping until the midnight. Reset-ting counter & incrementing the date.")
            inner_itr_count = 0  # reset the daily quota
            time.sleep(seconds_until_end_of_day)
            incremented_date = datetime.datetime.now().date() + datetime.timedelta(days=1)
        elif (inner_itr_count < 17250 and incremented_date == datetime.datetime.now().date()):
            inner_itr_count = 0
            incremented_date = datetime.datetime.now().date() + datetime.timedelta(days=1)
            print("today's counter been reset to: ", inner_itr_count)
            print("day has been incremented to: ", incremented_date)

    except Exception as e:
        print("error: ", e)
        break


