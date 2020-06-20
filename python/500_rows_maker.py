# For USF SAIL
# ~Mihir

import csv
import pandas as pd
import numpy as np
import re
import string
import glob
import sys
import os
import time

#------------- query (consider seperating it)

#*******************************************************************************#
#TO DO: Add Quoted_text (Currently, we do based on the Original and Retweet texts)
#*******************************************************************************#

#Query list for 8 topics

just_mask= ['mask', 'face mask', 'n95', 'cloth', 'fabrik', 'fabrick', 'fabric',
            'medical mask', 'medical-mask', 'face-mask', 'musk', 'face coverings',
            'face-covering', 'face covering', 'cloth-covering','face-cover', 'face cover',
            'bandana', 't-shirt face mask', ' t-shirt mask', 't shirt mask', 'tshirt mask',
            'tshirt-mask', 'reusable face mask', 'surgical-mask', 'surgical mask', 'n90', 'respirator mask',
            'respiratory-mask','respirator-mask','gas mask', 'n99', 'ffp1', 'ffp2', 'ffp3', '3m', 'respiratory mask']

#malria treatments
false_treatment_keywords= ['rx', 'malaria rx', 'hydroxy',
                          'hydroxychloroquine', 'chloroquine',
                          'zithromax', 'azithromycin', 'hydroxee',
                          'chloroqueene','chloroquene', 'arbidol',
                          'remdesivir', 'remdesiveer','steroids',
                          'shuanghuanglian', 'ibuprofen','ibuprofeen',
                          'iboprofein']

#bleach & alcohol
false_bleach_y_alcohol_keywords= ['bleach', 'lysol', 'peroxide',
                                 'chlorine', 'inject', 'ingest',
                                 'lyzol', 'lizol', 'injest',
                                 'ingect', 'bleech', 'oxide',
                                 'perooxide', 'chlorene', 'chloroxide']

#UV lights
false_uv_lamps_keywords= ['uv', 'ultraviolet', 'tanning', 'tanning bed',
                         'ultraviolet radiation']

#drink alcohol
false_drink_alcohol_keywords= ['mouthwash', 'methanol', 'ethanol',
                              'hydroxyl', 'meth', 'ethonol',
                              'corona beer']

#5G towers
false_5g_tech_keywords= ['5g', 'radiation', 'radiowaves', 'radio waves',
                        'radio wave', 'wi fi', '5g wifi', '5g wi fi',
                        '5g waves']

#bioweapon
false_bio_weapon_keywords= ['weapon', 'bioweapon', 'china weapon',
                           'biowarfare', 'bioterrorism',
                           'bio warfare', 'bio terrorism',
                           'bio weapon']

#USA army
false_us_army_keywords= ['cia', 'maatje benassi', 'us military',
                        'soldiers brought', 'american military',
                        'american soldiers', 'us soldiers']

#bill gates
false_bill_gates_keywords= ['bill gates', 'melina gates', 'vatican',
                           'bill gates vaccines', 'vaccines', 'vaccine',
                           'waxine', 'mircochip']


#(1)randomly select  rows (COLLAPSE THE ROWS IF QUOTED OR RETWEETED, ITERATE OVER ONE ROW!!)
# false_treatment_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_bleach_y_alcohol_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_uv_lamps_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_drink_alcohol_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_5g_tech_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_bio_weapon_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_us_army_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]
# false_bill_gates_df= frame.loc[np.random.choice(frame.index, size= rowsToSample)]

#*************************** read the 100k csv here ******************************************#
main_df = pd.read_csv("./good 100k file/filtered_USC_march_100K_file-utf8.csv",
                      low_memory=False, encoding= 'utf-8-sig')

#filter based on bot score
# everything between (0.0-0.4)
level_1_bot_filter_df = main_df.loc[(main_df.cap_english>=0.0) & (main_df.cap_english<0.4)]
print("level_1_bot_filter_df", level_1_bot_filter_df.shape)

# everything between (0.4-0.8)
level_2_bot_filter_df = main_df.loc[(main_df.cap_english>=0.4) & (main_df.cap_english<0.8)]
print("level_1_bot_filter_df", level_2_bot_filter_df.shape)

# everything between (0.8-1.0)
level_3_bot_filter_df = main_df.loc[(main_df.cap_english>=0.8) & (main_df.cap_english<1.0)]
print("level_1_bot_filter_df", level_3_bot_filter_df.shape)

##---
#false_rand_rows = frame.loc[np.random.choice(frame.index, size= rowsToSample)]

# make empty dataframe to add rows for each topic
false_treatment_df= pd.DataFrame()
false_bleach_y_alcohol_df= pd.DataFrame()
false_uv_lamps_df= pd.DataFrame()
false_drink_alcohol_df= pd.DataFrame()
false_5g_tech_df= pd.DataFrame()
false_bio_weapon_df= pd.DataFrame()
false_us_army_df= pd.DataFrame()
false_bill_gates_df= pd.DataFrame()

# def here to convert to lower case, remove all but chars & nums
def fix_each_row(row):
    # I THINK IT'S BETTER TO LET OTHER DO THE CLEANING!
    # Anything except 0..9, a..z and A..Z, replaced with nothing
    #cleaned_row= re.sub("[^0-9a-zA-Z]", " ", row)
    #remove extra spaces
    #cleaned_row= re.sub("\\s+", " ", cleaned_row)
    #return a lower-cased BOM utf string
    return str(row.lower().encode('utf-8-sig').decode('utf-8'))

# temp list to hold a string
# l = []
# l.insert(0, level_3_bot_filter_df.loc[level_3_bot_filter_df.index[0], 'tweet_text'])

#filtering_with_these_queries = false_5g_tech_keywords

#def to get the indices of where the queries lie
def give_me_queried_indices(filtering_with_these_queries, which_level_df):

    #list to hold the indices
    index_list = []

    for index, row in which_level_df.iterrows():
        # temp list to hold a string
        l = []
        #l.insert(0, level_3_bot_filter_df.loc[level_3_bot_filter_df.index[0], 'tweet_text'])
        #print(row['tweet_text'], row['c2'])

        l.insert(0, row['tweet_text'])
        # evaluate the list with keyword
        results = [x for x in l if any(re.search("\\b{}\\b".format(w), fix_each_row(x)) for w in filtering_with_these_queries)]
        #print(results)

        # if list is full, place the index otherwise move on
        if not results:
            pass
        else:
            index_list.append(index)
        # time.sleep(3)

    return index_list

# evaluate the list with keyword
# results = [x for x in l if any(re.search("\\b{}\\b".format(w), fix_each_row(x)) for w in false_bleach_y_alcohol_keywords)]

# Chenge file name here!!!
#exportFileName = '5g_queried_utf-8_low_bots.csv'

#now simply filter
filter_df_level_1  = level_1_bot_filter_df.loc[give_me_queried_indices(just_mask,level_1_bot_filter_df)]
filter_df_level_2  = level_2_bot_filter_df.loc[give_me_queried_indices(just_mask,level_2_bot_filter_df)]
filter_df_level_3  = level_3_bot_filter_df.loc[give_me_queried_indices(just_mask,level_3_bot_filter_df)]

#filter_df_level_1.to_csv(exportFileName, encoding='utf-8-sig', index=False)

def write_me_a_file(exportFileName, filtered_df_at_level):
    filtered_df_at_level.to_csv(exportFileName, encoding='utf-8-sig', index=False)


write_me_a_file('mask_queried_utf-8_low_bots.csv', filter_df_level_1)
write_me_a_file('mask_queried_utf-8_med_bots.csv', filter_df_level_2)
write_me_a_file('mask_queried_utf-8_hig_bots.csv', filter_df_level_3)

#clear the list
#del index_list[:]

#--------- Under construction, maybe, haven't decided it yet -------#
# # ifelseifelse structure try for-loop structure, maybe try-catch wrapped for if elif,
# # write to a dataframe that line in criteria in append mode, 8 files
#
# df_joined.to_csv(exportFileName, encoding='utf-8-sig', index=False)

