# For USF SAIL
# ~Dre, Mihir

import json
import csv
import io
import glob

#*************************** NOTE *******************************************#
# LINE 54, please put month in the csv file name to prevent confusion        #
# LINE 50, please give the jsonl file name if single file is to be converted #
#****************************************************************************#

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
outfile = open('merged_file.jsonl','w', encoding= 'utf-8-sig')
for f in glob.glob("SOME_FOLDER/*.jsonl"):
    with open(f, 'r', encoding='utf-8-sig') as infile:
        for line in infile.readlines():
            outfile.write(line)
outfile.close()
#*******************************************************************************************#

#path to the jsonl file/s (if you want single json file converted, edit the file name here!)
data_json = io.open('merged_file.jsonl', mode='r', encoding='utf-8-sig') # Opens in the JSONL file
data_python = extract_json(data_json)

# write entrire jsonl files (append mode 'a', overwriting mode 'w')
csv_out = io.open('usc_tweets_out_utf8.csv', mode='w', encoding='utf-8-sig') #opens csv file

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
         u'user_geo,' \
         u'user_coordinates,' \
         u'user_followers_count,' \
         u'user_friends_count,' \
         u'user_listed_count,' \
         u'user_statuses_count,' \
         u'user_favourite_count,' \
         u'user_profile_creation_at,' \
         u'retweets_user_id_str,' \
         u'retweets_user_screen_name,' \
         u'retweets_user_description,' \
         u'retweets_user_location,' \
         u'retweets_user_geo,' \
         u'retweets_user_coordinates,' \
         u'retweets_user_follower_count,' \
         u'retweets_user_friends_count,' \
         u'retweets_user_listed_count,' \
         u'retweets_user_statuses_count,' \
         u'retweets_user_favourite_count,' \
         u'retweets_user_retweet_count,' \
         u'retweets_user_profile_creation_at,' \
         u'hashtags,' \
         u'mentions_screen_names,' \
         u'mentions_user_id_str'


#write the column names
csv_out.write(fields)
csv_out.write(u'\n')

# This for loop iteratest through json objects, and gets the keys and objects
# some of the objects do have NULL values (like retweets, mentions etc.) so they're
# in a try-catch blocks

for line in data_python:

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

    #user geo location
    try:
        user_geo = '"' + line.get('user').get('geo').replace('"','""') + '"'
    except:
        user_geo = ''

    #user coordinates
    try:
        user_coordinates = '"' + line.get('user').get('coordinates').replace('"','""') + '"'
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

    # retweet user_geo
    try:
        retweeted_status_user_geo = '"' + line.get('retweeted_status').get('user').get('geo').replace('"', '""') + '"'
    except:
        retweeted_status_user_geo = ''

    # retweet user_coordinates
    try:
        retweeted_status_user_coordinates = '"' + line.get('retweeted_status').get('user').get('coordinates').replace('"', '""') + '"'
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

    #hashtags
    try:
        check_hash_present = line.get('entities').get('hashtags')[0].get('text')
        temp = line.get('entities').get('hashtags')
        hastags = ""
        #looks funky in csv, but when loaded into R or python, it comes correctly (it is encoded with utf-8 BOM)
        for val in temp:
            hastags += '"' + val.get('text').replace('"', '""') + '"' + '|'
    except:
        hastags = ''

    #mentions
    try:
        check_mention_present = line.get('entities').get('user_mentions')[0]#.get('text')
        temp = line.get('entities').get('user_mentions')
        mentions = ""
        #looks funky in csv, but when loaded into R or python, it comes correctly
        for val in temp:
            mentions += '"' + val.get('screen_name').replace('"', '""') + '"' + '|'
    except:
        mentions = ''

    #mentions user_id_str
    try:
        check_mention_ids_present = line.get('entities').get('user_mentions')[0]#.get('text')
        temp = line.get('entities').get('user_mentions')
        mentions_id_str = ""
        #looks funky in csv, but when loaded into R or python, it comes correctly
        for val in temp:
            mentions_id_str += '"' + val.get('id_str').replace('"', '""') + '"' + '|'
    except:
        mentions_id_str = ''

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
           user_geo,
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
           retweeted_status_user_geo,
           retweeted_status_user_coordinates,
           retweeted_status_user_follower_count,
           retweeted_status_user_friends_count,
           retweeted_status_user_listed_count,
           retweeted_status_user_statuses_count,
           retweeted_status_user_favourite_count,
           retweeted_status_user_retweet_count,
           retweeted_status_user_profile_creation_date,
           hastags,
           mentions,
           mentions_id_str]

    #write the row line by line with \n inserted at the end
    row_joined = u','.join(row)
    csv_out.write(row_joined)
    csv_out.write(u'\n')

#close the csv file for writing
csv_out.close()
