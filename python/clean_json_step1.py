# For USF SAIL
# ~Dre, Mihir

import json
import csv
import io
import glob
import os

#*************************** NOTE ************************************************#
# LINE 54, please put month in the csv file name to prevent confusion             #
# LINE 41 & 50, please give the jsonl file name if single file is to be converted #
#*********************************************************************************#

# creates a .csv file using a Twitter .json file
# the fields have to be set manually

def extract_json(fileobj):
    # Iterates over an open JSONL file and yields
    # decoded lines.  Closes the file once it has been
    # read completely.
    with fileobj:
        for line in fileobj:
            yield json.loads(line)

#Writes into json format
# result = []
# for f in glob.glob("folder_with_all_jsonl/*.jsonl"):
#     with open(f, 'r', encoding='utf-8-sig') as infile:
#         for line in infile.readlines():
#             try:
#                 result.append(json.loads(line)) # read each line of the file
#             except ValueError:
#                 print(f)
#
# #write the file in BOM TO preserve the emojis and special characters
# with open('merged_file.json','w', encoding= 'utf-8-sig') as outfile:
#     json.dump(result, outfile)

#********************** Ignore this if only want to convert single jsonl file ***************#
#writes into a proper jsonl file
# outfile = open('merged_file.jsonl','w', encoding= 'utf-8-sig')
# for f in glob.glob("folder_with_all_jsonl/*.jsonl"):
#     with open(f, 'r', encoding='utf-8-sig') as infile:
#         for line in infile.readlines():
#             outfile.write(line)
# outfile.close()
#*******************************************************************************************#

#make a directory to save
try:
   os.mkdir("./JSONL_TO_CSV_USC_MARCH/") # <---- Here
except OSError as e:
   print("Directory exists. Please change both folders names in this block to whatever.")
   exit()

path = './JSONL_TO_CSV_USC_MARCH/' # <--- Here too

#----------------------- change the location to where all the jsonls are----------------------
filenames = glob.glob("folder_with_all_jsonl/*.jsonl")

#read file by file, write file by file. Simple.

for f in filenames:
    #path to the jsonl file/s (if you want single json file converted, edit the file name here!)
    data_json = io.open(f, mode='r', encoding='utf-8-sig') # Opens in the JSONL file
    data_python = extract_json(data_json)

    out_csv_name = f[f.rfind('\\'):]
    out_csv_name = out_csv_name.split("\\")[1]
    out_csv_name = out_csv_name.split(".")[0]

    # csv writing location
    file_name = ''.join([out_csv_name,'_utf-8','.csv'])
    save_path = os.path.abspath(
        os.path.join(
            path, file_name
        )
    )

    # write entrire jsonl files (append mode 'a', overwriting mode 'w')
    csv_out = io.open(save_path, mode='w', encoding='utf-8-sig') #opens csv file

    # field names
    #if you're adding additional columns pleae don't forget to add them here
    fields = u'tweet_created_at,' \
             u'tweet_id_str,' \
             u'tweet_text,' \
             u'in_reply_to_status_id_str,' \
             u'in_reply_to_user_id_str,' \
             u'in_reply_to_screen_name,' \
             u'tweets_retweet_count,' \
             u'tweets_favourite_count,' \
             u'retweet_created_at,' \
             u'retweet_id_str,' \
             u'retweet_text,' \
             u'retweets_retweet_count,' \
             u'retweets_favourite_count,' \
             u'user_id_str,' \
             u'user_screen_name,' \
             u'user_description,' \
             u'user_location,' \
             u'user_coordinates,' \
             u'user_followers_count,' \
             u'user_friends_count,' \
             u'user_listed_count,' \
             u'user_statuses_count,' \
             u'user_favourites_count,' \
             u'user_profile_creation_at,' \
             u'retweets_user_id_str,' \
             u'retweets_user_screen_name,' \
             u'retweets_user_description,' \
             u'retweets_user_location,' \
             u'retweets_user_coordinates,' \
             u'retweets_user_followers_count,' \
             u'retweets_user_friends_count,' \
             u'retweets_user_listed_count,' \
             u'retweets_user_statuses_count,' \
             u'retweets_user_favourites_count,' \
             u'retweets_user_retweet_count,' \
             u'retweets_user_profile_creation_at,' \
             u'mentions_screen_names,' \
             u'mentions_user_id_str,' \
             u'retweet_mentions_screen_names,' \
             u'retweet_mentions_user_id_str,' \
             u'quoted_mentions_screen_names,' \
             u'quoted_mentions_user_id_str,' \
             u'quoted_created_at,' \
             u'quoted_id_str,' \
             u'quoted_text,' \
             u'quoted_retweet_count,' \
             u'quoted_favourite_count,' \
             u'quoted_user_id_str,' \
             u'quoted_user_screen_name,' \
             u'quoted_user_description,' \
             u'quoted_user_location,' \
             u'quoted_user_coordinates,' \
             u'quoted_user_followers_count,' \
             u'quoted_user_friends_count,' \
             u'quoted_user_listed_count,' \
             u'quoted_user_favourites_count,' \
             u'quoted_user_statuses_count,' \
             u'user_verified,' \
             u'retweet_user_verified,' \
             u'quoted_user_verified,' \
             u'user_place_name,' \
             u'retweet_user_place_name,' \
             u'quote_user_place_name,' \
             u'user_country_code,' \
             u'retweet_user_country_code,' \
             u'quote_user_country_code'

    #write the column names
    csv_out.write(fields)
    csv_out.write(u'\n')

    # This for loop iteratest through json objects, and gets the keys and objects
    # some of the objects do have NULL values (like retweets, mentions etc.) so they're
    # in a try-catch blocks

    for line in data_python:

        #write only the english accounts to file

        if(line.get('lang') == 'en'):

            # in_reply_to_status_id_str
            try:
                in_reply_to_status_id_str = '"' + line.get('in_reply_to_status_id_str').replace('"','""') + '"'
            except:
                in_reply_to_status_id_str = ''

            # in_reply_to_user_id_str
            try:
                in_reply_to_user_id_str = '"' + line.get('in_reply_to_user_id_str').replace('"','""') + '"'
            except:
                in_reply_to_user_id_str = ''

            # in_reply_to_screen_name
            try:
                in_reply_to_screen_name = '"' + line.get('in_reply_to_screen_name').replace('"','""') + '"'
            except:
                in_reply_to_screen_name = ''

            #user description
            try:
                user_description = '"' + line.get('user').get('description').replace('"','""') + '"'
            except:
                user_description = ''

            #user location
            try:
                user_location = '"' +line.get('user').get('location').replace('"','""') + '"'
            except:
                user_location = ''

            #user coordinates
            try:
                get_x = str(line.get('coordinates').get('coordinates')[0])
                get_y = str(line.get('coordinates').get('coordinates')[1])
                user_coordinates = ''.join([get_x, '|',get_y])
            except:
                user_coordinates = ''

            # retweet retweet_count
            try:
                retweets_status_retweet_count = str(line.get('retweeted_status').get('retweet_count'))
            except:
                retweets_status_retweet_count = ''

            # retweet favourite_count
            try:
                retweets_status_favourite_count = str(line.get('retweeted_status').get('favorite_count'))
            except:
                retweets_status_favourite_count = ''

            # retweet created_at
            try:
                retweeted_status_created_at = '"' + line.get('retweeted_status').get('created_at').replace('"', '""') + '"'
            except:
                retweeted_status_created_at = ''

            # retweet id_str
            try:
                retweeted_status_id_str = '"' + line.get('retweeted_status').get('id_str').replace('"', '""') + '"'
            except:
                retweeted_status_id_str = ''

            # retweets texts
            try:
                retweeted_status_full_text = '"' + line.get('retweeted_status').get('full_text').replace('"', '""') + '"'
            except:
                retweeted_status_full_text = ''

            # retweet user_id_str
            try:
                retweeted_status_user_id_str = '"' + line.get('retweeted_status').get('user').get('id_str').replace('"', '""') + '"'
            except:
                retweeted_status_user_id_str = ''

            # retweet user_description
            try:
                retweeted_status_user_description = '"' + line.get('retweeted_status').get('user').get('description').replace('"', '""') + '"'
            except:
                retweeted_status_user_description  = ''

            # retweet user_screen_name
            try:
                retweeted_status_user_screen_name = '"' + line.get('retweeted_status').get('user').get('screen_name').replace('"', '""') + '"'
            except:
                retweeted_status_user_screen_name = ''

            # retweet user_location
            try:
                retweeted_status_user_location = '"' + line.get('retweeted_status').get('user').get('location').replace('"', '""') + '"'
            except:
                retweeted_status_user_location = ''

            # retweet user_coordinates
            try:
                get_x = str(line.get('retweeted_status').get('coordinates').get('coordinates')[0])
                get_y = str(line.get('retweeted_status').get('coordinates').get('coordinates')[1])
                retweeted_status_user_coordinates = ''.join([get_x, '|',get_y])
            except:
                retweeted_status_user_coordinates = ''

            # retweet user_followers_count
            try:
                retweeted_status_user_follower_count = str(line.get('retweeted_status').get('user').get('followers_count'))
            except:
                retweeted_status_user_follower_count = ''

            # retweet user_friends_count
            try:
                retweeted_status_user_friends_count = str(line.get('retweeted_status').get('user').get('friends_count'))
            except:
                retweeted_status_user_friends_count = ''

            # retweet user_listed_count
            try:
                retweeted_status_user_listed_count = str(line.get('retweeted_status').get('user').get('listed_count'))
            except:
                retweeted_status_user_listed_count = ''

            # retweet user_statuses_count
            try:
                retweeted_status_user_statuses_count = str(line.get('retweeted_status').get('user').get('statuses_count'))
            except:
                retweeted_status_user_statuses_count = ''

            # retweet user_favourite_count
            try:
                retweeted_status_user_favourite_count = str(line.get('retweeted_status').get('user').get('favourites_count'))
            except:
                retweeted_status_user_favourite_count = ''

            # retweet user_retweet_count
            try:
                retweeted_status_user_retweet_count = str(line.get('retweeted_status').get('retweet_count'))
            except:
                retweeted_status_user_retweet_count = ''

            # retweet user_profile_creation_date
            try:
                retweeted_status_user_profile_creation_date = '"' + line.get('retweeted_status').get('user').get('created_at').replace('"', '""') + '"'
            except:
                retweeted_status_user_profile_creation_date = ''

            # retweet user_profile_verified
            try:
                retweeted_status_user_profile_verified = line.get('retweeted_status').get('user').get('verified')
            except:
                retweeted_status_user_profile_verified = ''

            # quoted user_profile_verified
            try:
                quoted_status_user_profile_verified = line.get('retweeted_status').get('quoted_status').get('user').get('verified')
            except:
                quoted_status_user_profile_verified = ''

            # retweeted _stated mentions
            try:
                check_mention_present = line.get('retweeted_status').get('entities').get('user_mentions')[0]#.get('text')
                temp = line.get('retweeted_status').get('entities').get('user_mentions')
                retweeted_mentions = ""
                #looks funky in csv, but when loaded into R or python, it comes correctly
                for val in temp:
                    #retweeted_mentions += '"' + val.get('screen_name').replace('"', '""') + '"' + '|'
                    retweeted_mentions= ''.join([retweeted_mentions, str(val.get('screen_name')), '|'])
            except:
                retweeted_mentions = ''

            #mentions user_id_str
            try:
                check_mention_ids_present = line.get('retweeted_status').get('entities').get('user_mentions')[0]#.get('text')
                temp = line.get('retweeted_status').get('entities').get('user_mentions')
                retweeted_mentions_id_str = ""
                #looks funky in csv, but when loaded into R or python, it comes correctly
                for val in temp:
                    #retweeted_mentions_id_str += '"' + val.get('id_str').replace('"', '""') + '"' + '|'
                    retweeted_mentions_id_str= ''.join([retweeted_mentions_id_str, str(val.get('id_str')), '|'])
            except:
                retweeted_mentions_id_str = ''

            #quoted status mentions
            try:
                check_mention_present = line.get('retweeted_status').get('quoted_status').get('entities').get('user_mentions')[0]#.get('text')
                temp = line.get('retweeted_status').get('quoted_status').get('entities').get('user_mentions')
                quoted_mentions = ""
                #looks funky in csv, but when loaded into R or python, it comes correctly
                for val in temp:
                    #quoted_mentions += '"' + val.get('screen_name').replace('"', '""') + '"' + '|'
                    quoted_mentions = ''.join([quoted_mentions, str(val.get('screen_name')), '|'])
            except:
                quoted_mentions = ''

            #mentions user_id_str
            try:
                check_mention_ids_present = line.get('retweeted_status').get('quoted_status').get('entities').get('user_mentions')[0]#.get('text')
                temp = line.get('retweeted_status').get('quoted_status').get('entities').get('user_mentions')
                quoted_mentions_id_str = ""
                #looks funky in csv, but when loaded into R or python, it comes correctly
                for val in temp:
                    #quoted_mentions_id_str += '"' + val.get('id_str').replace('"', '""') + '"' + '|'
                    quoted_mentions_id_str = ''.join([quoted_mentions_id_str, str(val.get('id_str')), '|'])
            except:
                quoted_mentions_id_str = ''

            #mentions
            try:
                check_mention_present = line.get('entities').get('user_mentions')[0]#.get('text')
                temp = line.get('entities').get('user_mentions')
                mentions = ""
                #looks funky in csv, but when loaded into R or python, it comes correctly
                for val in temp:
                    #mentions += '"' + val.get('screen_name').replace('"', '""') + '"' + '|'
                    mentions = ''.join([mentions, str(val.get('screen_name')), '|'])
            except:
                mentions = ''

            #mentions user_id_str
            try:
                check_mention_ids_present = line.get('entities').get('user_mentions')[0]#.get('text')
                temp = line.get('entities').get('user_mentions')
                mentions_id_str = ""
                #looks funky in csv, but when loaded into R or python, it comes correctly
                for val in temp:
                    #mentions_id_str += '"' + val.get('id_str').replace('"', '""') + '"' + '|'
                    mentions_id_str = ''.join([mentions_id_str, str(val.get('id_str')), '|'])
            except:
                mentions_id_str = ''


            # quoted created_at
            try:
                quoted_status_created_at = '"' + line.get('retweeted_status').get('quoted_status').get('created_at').replace('"', '""') + '"'
            except:
                quoted_status_created_at = ''

            # quoted id_str
            try:
                quoted_status_id_str =  str(line.get('retweeted_status').get('quoted_status').get('id_str'))
            except:
                quoted_status_id_str = ''

            # quoted texts
            try:
                quoted_status_full_text = '"' + line.get('retweeted_status').get('quoted_status').get('full_text').replace('"', '""') + '"'
            except:
                quoted_status_full_text = ''

            # quoted retweet_count
            try:
                quoted_status_retweet_count= str(line.get('retweeted_status').get('quoted_status').get('retweet_count'))
            except:
                quoted_status_retweet_count = ''

            # quoted favourite_count
            try:
                quoted_status_favourite_count=  str(line.get('retweeted_status').get('quoted_status').get('favorite_count'))
            except:
                quoted_status_favourite_count= ''

            # quoted user_id_str
            try:
                quoted_status_user_id_str = '"' + line.get('retweeted_status').get('quoted_status').get('user').get('id_str').replace('"', '""') + '"'
            except:
                quoted_status_user_id_str = ''

            # quoted user_description
            try:
                quoted_status_user_description = '"' + line.get('retweeted_status').get('quoted_status').get('user').get('description').replace('"', '""') + '"'
            except:
                quoted_status_user_description  = ''

            # quoted user_screen_name
            try:
                quoted_status_user_screen_name = '"' + line.get('retweeted_status').get('quoted_status').get('user').get('screen_name').replace('"', '""') + '"'
            except:
                quoted_status_user_screen_name = ''

            # quoted user_location
            try:
                quoted_status_user_location = '"' + line.get('retweeted_status').get('quoted_status').get('user').get('location').replace('"', '""') + '"'
            except:
                quoted_status_user_location = ''

            # quoted user_coordinates
            try:
                get_x = str(line.get('retweeted_status').get('quoted_status').get('coordinates').get('coordinates')[0])
                get_y = str(line.get('retweeted_status').get('quoted_status').get('coordinates').get('coordinates')[1])
                quoted_status_user_coordinates = ''.join([get_x, '|',get_y])
            except:
                quoted_status_user_coordinates = ''

            # quoted user_followers_count
            try:
                quoted_status_user_follower_count = str(line.get('retweeted_status').get('quoted_status').get('user').get('followers_count'))
            except:
                quoted_status_user_follower_count = ''

            # quoted user_friends_count
            try:
                quoted_status_user_friends_count = str(line.get('retweeted_status').get('quoted_status').get('user').get('friends_count'))
            except:
                quoted_status_user_friends_count = ''

            # quoted user_listed_count
            try:
                quoted_status_user_listed_count = str(line.get('retweeted_status').get('quoted_status').get('user').get('listed_count'))
            except:
                quoted_status_user_listed_count = ''

            # quoted user_statuses_count
            try:
                quoted_status_user_statuses_count = str(line.get('retweeted_status').get('quoted_status').get('user').get('statuses_count'))
            except:
                quoted_status_user_statuses_count = ''

            # quoted user_favourite_count
            try:
                quoted_status_user_favourite_count = str(line.get('retweeted_status').get('quoted_status').get('user').get('favourites_count'))
            except:
                quoted_status_user_favourite_count = ''

            #  user place full name
            try:
                user_place_name = '"' + line.get('place').get('full_name').replace('"', '""') + '"'
            except:
                user_place_name = ''

            # retweet user_place_full_name
            try:
                retweeted_status_user_place_name = '"' + line.get('retweeted_status').get('place').get('full_name').replace('"', '""') + '"'
            except:
                retweeted_status_user_place_name = ''

            # quoted user_place_full_name
            try:
                quoted_status_user_place_name = '"' + line.get('retweeted_status').get('quoted_status').get('place').get('full_name').replace('"', '""') + '"'
            except:
                quoted_status_user_place_name = ''

            # user_country_code
            try:
                user_country_code = '"' + line.get('place').get('country_code').replace('"', '""') + '"'
            except:
                user_country_code = ''

            # quoted user_place country_code
            try:
                retweet_user_country_code = '"' + line.get('retweeted_status').get('place').get('country_code').replace('"', '""') + '"'
            except:
                retweet_user_country_code = ''

            # quoted user_place country_code
            try:
                quoted_user_country_code = '"' + line.get('retweeted_status').get('quoted_status').get('place').get('country_code').replace('"', '""') + '"'
            except:
                quoted_user_country_code = ''

            #writes a row and gets the fields from the json object
            #screen_name and followers/friends are found on the second level hence two get methods
            row = [line.get('created_at'),
                   str(line.get('id_str')),
                   '"' + line.get('full_text').replace('"','""') + '"', #creates double quotes
                   in_reply_to_status_id_str,
                   in_reply_to_user_id_str,
                   in_reply_to_screen_name,
                   str(line.get('retweet_count')),
                   str(line.get('favorite_count')),
                   retweeted_status_created_at,
                   retweeted_status_id_str,
                   retweeted_status_full_text,
                   retweets_status_retweet_count,
                   retweets_status_favourite_count,
                   line.get('user').get('id_str'),
                   str(line.get('user').get('screen_name')),
                   user_description,
                   user_location,
                   user_coordinates,
                   str(line.get('user').get('followers_count')),
                   str(line.get('user').get('friends_count')),
                   str(line.get('user').get('listed_count')),
                   str(line.get('user').get('statuses_count')),
                   str(line.get('user').get('favourites_count')),
                   str(line.get('user').get('created_at')),
                   retweeted_status_user_id_str,
                   retweeted_status_user_screen_name,
                   retweeted_status_user_description,
                   retweeted_status_user_location,
                   retweeted_status_user_coordinates,
                   retweeted_status_user_follower_count,
                   retweeted_status_user_friends_count,
                   retweeted_status_user_listed_count,
                   retweeted_status_user_statuses_count,
                   retweeted_status_user_favourite_count,
                   retweeted_status_user_retweet_count,
                   retweeted_status_user_profile_creation_date,
                   mentions,
                   mentions_id_str,
                   retweeted_mentions,
                   retweeted_mentions_id_str,
                   quoted_mentions,
                   quoted_mentions_id_str,
                   quoted_status_created_at,
                   quoted_status_id_str,
                   quoted_status_full_text,
                   quoted_status_retweet_count,
                   quoted_status_favourite_count,
                   quoted_status_user_id_str,
                   quoted_status_user_screen_name,
                   quoted_status_user_description,
                   quoted_status_user_location,
                   quoted_status_user_coordinates,
                   quoted_status_user_follower_count,
                   quoted_status_user_friends_count,
                   quoted_status_user_listed_count,
                   quoted_status_user_favourite_count,
                   quoted_status_user_statuses_count,
                   line.get('user').get('verified'),
                   retweeted_status_user_profile_verified,
                   quoted_status_user_profile_verified,
                   user_place_name,
                   retweeted_status_user_place_name,
                   quoted_status_user_place_name,
                   user_country_code,
                   retweet_user_country_code,
                   quoted_user_country_code]

            #write the row line by line with \n inserted at the end
            row_joined = u','.join(str(v) for v in row)
            csv_out.write(row_joined)
            csv_out.write(u'\n')

    #close the csv file for writing
    csv_out.close()
