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
import random

#*******************************************************************************#
#NOTE: CHANGE COL. EVERYTIME (Currently, we do based on the Original Tweet ONLY)
#*******************************************************************************#

exportFile_1Mil = '1_million_march_USC.csv'

# file path where all your csvs are
path = r'C:/Users/ThinkPad/SpyderProjects/JsonStuff/good 100k file' 
# reads in the file names
all_files = glob.glob(path + "/*.csv")

#list where we store the dataframe, later unlist
li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0, low_memory= False, encoding= 'utf-8-sig')
    li.append(df)

main_df = pd.concat(li, axis=0, ignore_index=True)

#------------- query (consider seperating it)

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


#*************************** Rand Sample 1 million rows here ******************************************#

# randomly sample rows WITHOUT REPLACEMENT
n= 1000 #1000000

main_df= main_df.take(np.random.permutation(len(main_df))[:n])

main_df.to_csv(exportFile_1Mil, encoding='utf-8-sig', index=False)

# write that 1 million file to default path


#filter based 

level_X_filter_df = main_df

main_df = None

##---

# make empty dataframe to add rows for each topic

# def here to convert to lower case, remove all but chars & nums
def fix_each_row(row):
    return str(row.lower().encode('utf-8-sig').decode('utf-8'))


#def to get the indices of where the queries lie
def give_me_queried_indices(filtering_with_these_queries, which_level_df):

    #list to hold the indices
    index_list = []

    for index, row in which_level_df.iterrows():
        # temp list to hold a string
        l = []

        l.insert(0, row['tweet_text']) #CHANGE THE COLUMN NAME HERE!!!
        # evaluate the list with keyword
        results = [x for x in l if any(re.search("\\b{}\\b".format(w), fix_each_row(x)) for w in filtering_with_these_queries)]
        

        # if list is full, place the index otherwise move on
        if not results:
            pass
        else:
            index_list.append(index)
        # time.sleep(3)

    return index_list


#now simply filter
level_X_filter_df  = level_X_filter_df.loc[give_me_queried_indices(just_mask,level_X_filter_df)]


#print the number of rows in file
print(len(level_X_filter_df))

# now only sample 500 rows
n_set= 500 

level_X_filter_df= level_X_filter_df.take(np.random.permutation(len(level_X_filter_df))[:n_set])

#write some files
def write_me_a_file(exportFileName, filtered_df_at_level):
    filtered_df_at_level.to_csv(exportFileName, encoding='utf-8-sig', index=False)


write_me_a_file('mask_queried_utf-8.csv', level_X_filter_df)
