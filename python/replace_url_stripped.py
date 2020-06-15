# FOR USF SAIL
# ~ Mihir

import csv
import pandas as pd
import numpy as np
import requests
import re
from bs4 import BeautifulSoup
import string
import glob
import sys
import os
import signal
from requests.exceptions import Timeout

#give the in-file name (with full path)
in_file_name= "C:/Users/ThinkPad/SpyderProjects/JsonStuff/JSONL_TO_CSV_USC_MARCH/sfds_utf-8.csv"
out_file_prefix= "sfds_utf-8"

#make a path to save csv
try:
   os.mkdir("./100K_March_URL_GONE/") # <---- Here
except OSError as e:
   print("Directory exists. Please change both folders names in this block to whatever.")
   #exit()

final_path = './100K_March_URL_GONE/' # <--- Current directory

# saves a path
out_file_name = ''.join([out_file_prefix, '_URL_GONE', '.csv'])
save_path = os.path.abspath(
    os.path.join(
        final_path, out_file_name
    )
)

#read a 100k file

df_joined_bots = pd.read_csv(in_file_name, index_col=None, header=0, encoding= 'utf-8-sig',low_memory=False)

# function that eventually gets the title tags from url
def get_title_from_url(url):
    try:
        #check if it's a word of https string
        if(url.startswith('http')):           
            #check if url is valid or returns an html, otherwise cut the time
            try:
                page = requests.get(url, timeout=5)
            except Timeout:
                # return dead url
                return ''
            #if the url is valid, sometimes phisy/ssl-missing site throw error
            html = BeautifulSoup(page.content, 'html.parser')
            #finally get the title if everyting is good
            page_title = html.find('title').text.strip()           
            return page_title
        else:
            return url
    except:
        return ''

# do this to prevent Null error that comes from split fn splitting Null
df_joined_bots['tweet_text'] = df_joined_bots['tweet_text'].fillna("")
df_joined_bots['retweet_text'] = df_joined_bots['retweet_text'].fillna("")
df_joined_bots['quoted_text'] = df_joined_bots['quoted_text'].fillna("")


#Seperating into three O(n) loop is much better then nesting eveyrything
for index, row in df_joined_bots.iterrows():
    # access data using column names
    
    #first let's take care of tweet_text
    s = df_joined_bots.at[index, 'tweet_text']
    l = list(map(lambda x: x, s.split()))
    m = list(map(get_title_from_url, l))
    n = ' '.join(m)
    df_joined_bots.at[index, 'tweet_text']= n

    del s,l,m,n

for index, row in df_joined_bots.iterrows():
    # access data using column names
   
    # then take care of retweets
    s = df_joined_bots.at[index, 'retweet_text']
    l = list(map(lambda x: x, s.split()))    
    m = list(map(get_title_from_url, l))    
    n = ' '.join(m)   
    df_joined_bots.at[index, 'retweet_text']= n

    del s,l,m,n

for index, row in df_joined_bots.iterrows():
    # access data using column names

    # lastly, quoted_text
    s = df_joined_bots.at[index, 'quoted_text']
    l = list(map(lambda x: x, s.split()))
    m = list(map(get_title_from_url, l))
    n = ' '.join(m)
    df_joined_bots.at[index, 'quoted_text']= n

    del s,l,m,n
    
# =============================================================================

#  UNIX ONLY, WIN DOES NOT HAVE ANY SIGALRM
# class TimeoutException(Exception):   # Custom exception class
#     pass
# 
# def timeout_handler(signum, frame):   # Custom signal handler
#     raise TimeoutException
# 
# # Change the behavior of SIGALRM
# signal.signal(signal.SIGALRM, timeout_handler)
# 
# for i in range(3):
#     # Start the timer. Once 5 seconds are over, a SIGALRM signal is sent.
#     signal.alarm(5)    
#     # This try/except loop ensures that 
#     # you'll catch TimeoutException when it's sent.
#     try:
#         x= get_title_from_url("https://www.amd.com") # notorious site without enough html
#     except TimeoutException:
#         continue # continue the for loop if function A takes more than 5 second
#     else:
#         # Reset the alarm
#         signal.alarm(0)
#         

# =============================================================================

df_joined_bots.to_csv(save_path, encoding='utf-8-sig', index=False)