# For USF SAIL
# ~Mihir
import botometer
import _json
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from flatten_json import flatten
import tweepy
import requests
import datetime

#
# -----------------------
# auth keys and other stuff here

# now it's called rapidapi key (it's in your app, then security)
rapidapi_key = "rapid api key"

# twitter authorization keys
twitter_app_auth = {
    'consumer_key': 'twitter dev key',
    'consumer_secret': 'twitter dev key',
    'access_token': 'twitter dev key',
    'access_token_secret': 'twitter dev key'
}

# Pro API endpoint(HAVE TO PASS THIS!! Currently default is wrong in the package)
botometer_api_url = 'https://botometer-pro.p.rapidapi.com'

# make a method of botometer
bom = botometer.Botometer(botometer_api_url=botometer_api_url,
                          wait_on_ratelimit=True,
                          rapidapi_key=rapidapi_key,
                          **twitter_app_auth)

# -------------------------

# now the plan is to see how long it takes to do this portion with pro-version, 17200 / day quota

chunksize2 = 15  # 17200 is daily, the lesser the chunk, higher the numbers of files creations

# *********************** UPDATE THE PATH HERE!!! *************************#
try:
    os.mkdir("./BOT_csv_news10_YOURNAMEHERE")  # <---- Here
except OSError as e:
    print("Directory exists. Please change both folders names in this block to whatever.")
    exit()

path = './BOT_csv_news10_YOURNAMEHERE/'  # <--- Here too

# ************************************************************************#

data2 = pd.read_csv('LISTOF_userids.csv',
                    chunksize=chunksize2,
                    encoding="ISO-8859-1", usecols=['Screenid'])
df2 = data2.get_chunk(chunksize2)
headers = list(df2.keys())
del data2

start_chunk = 0
# daily_quota_count=0
inner_itr_count = 0
incremented_date = datetime.datetime.now().date() + datetime.timedelta(days=1)

data2 = pd.read_csv('LISTOF_userids.csv',
                    chunksize=chunksize2,
                    encoding="ISO-8859-1",
                    skiprows=chunksize2 * start_chunk, usecols=['Screenid'])

for i, df2 in enumerate(data2):
    try:
        print('reading csv...')
        print(df2)
        # print('header: ', list(df2.keys()))
        # print('file header: ', headers)
        # daily_quota_count = daily_quota_count + df2.shape[0] #IMPORATANT! keeps track of when to sleep

        # Access chunks within data
        for chunk in df2:

            data = []
            current_df = df2

            # REMINDER: 17280 / day quota + 0.0001 afterwards! ~(OUTDATED!)~
            # ************************************#
            # rapidapi_pro_daily_free = 17280
            # ************************************#

            for ind in current_df.index:  # ok to run this the slow down from twitter will prevent daily calls
                # for ind in range(0, rapidapi_pro_daily_free):  # otherwise keep this line so we don't get surcharge

                inner_itr_count = inner_itr_count + 1  # incerement how much things get processed
                username_part = current_df['Screenid'][ind]
                print("inner_itr_count is: ", inner_itr_count)
                # try-catch the handle all the botometer errors, can be cleaned with R so NA and not NaN
                try:
                    botometer_result_part = bom.check_account(current_df['Screenid'][ind])

                except:
                    botometer_result_part = {'cap': {'english': 0, 'universal': 0},
                                             'display_scores_english': {'astroturf': 0, 'fake_follower': 0,
                                                                        'financial': 0, 'other': 0,
                                                                        'overall': 0, 'self_declared': 0,
                                                                        'spammer': 0},
                                             'display_scores_universal': {'astroturf': 0, 'fake_follower': 0,
                                                                          'financial': 0, 'other': 0,
                                                                          'overall': 0, 'self_declared': 0,
                                                                          'spammer': 0},
                                             'raw_scores_english': {'astroturf': 0, 'fake_follower': 0,
                                                                    'financial': 0, 'other': 0,
                                                                    'overall': 0, 'self_declared': 0,
                                                                    'spammer': 0},
                                             'raw_scores_universal': {'astroturf': 0, 'fake_follower': 0,
                                                                      'financial': 0, 'other': 0,
                                                                      'overall': 0, 'self_declared': 0,
                                                                      'spammer': 0},
                                             'user': {'majority_lang': 'NA', 'user_data_id_str': '0',
                                                      'user_data_screen_name': 'NA'}}
                    data.append([username_part, botometer_result_part])
                    continue

                print('elapsed time:', datetime.datetime.now())
                # put scrore into a dataframe
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
            df = df[['user_user_data_screen_name', 'user_user_data_id_str', 'user_majority_lang',
                     'cap_english', 'cap_universal', 'display_scores_english_astroturf',
                     'display_scores_english_fake_follower',
                     'display_scores_english_financial', 'display_scores_english_other',
                     'display_scores_english_overall',
                     'display_scores_english_self_declared', 'display_scores_english_spammer',

                     'display_scores_universal_astroturf', 'display_scores_universal_fake_follower',
                     'display_scores_universal_financial', 'display_scores_universal_other',
                     'display_scores_universal_overall',
                     'display_scores_universal_self_declared', 'display_scores_universal_spammer',

                     'raw_scores_english_astroturf', 'raw_scores_english_fake_follower',
                     'raw_scores_english_financial', 'raw_scores_english_other', 'raw_scores_english_overall',
                     'raw_scores_english_self_declared', 'raw_scores_english_spammer',

                     'raw_scores_universal_astroturf', 'raw_scores_universal_fake_follower',
                     'raw_scores_universal_financial', 'raw_scores_universal_other', 'raw_scores_universal_overall',
                     'raw_scores_universal_self_declared', 'raw_scores_universal_spammer']]

            # remove all the NA botometer rows
            df = df.drop(df[df.user_user_data_screen_name == 'NA'].index)

            # You can now export all outcomes in new csv files
            file_name = 'export_csv_' + str(start_chunk + i) + '.csv'
            save_path = os.path.abspath(
                os.path.join(
                    path, file_name
                )
            )
        print('saving ...')
        df.to_csv(save_path, encoding='utf-8', index=False)

        # this piece is added so if you process your 2000 accounts, it will sleep for a day
        if (inner_itr_count >= 2000 and datetime.datetime.now().day == datetime.datetime.now().day):
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
        elif (inner_itr_count < 2000 and incremented_date == datetime.datetime.now().date()):
            inner_itr_count = 0
            incremented_date = datetime.datetime.now().date() + datetime.timedelta(days=1)
            print("today's counter been reset to: ", inner_itr_count)
            print("day has been incremented to: ", incremented_date)

    except Exception as e:
        print("error: ", e)
        break


