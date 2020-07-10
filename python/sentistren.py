# For USF SAIL
# ~Mihir

import subprocess
import shlex
import os.path
import sys
import re
import emoji
from sentistrength import PySentiStr
from langdetect import detect
import pandas as pd
import glob
import numpy as np

# Note: 
# -Provide absolute path instead of relative path
# -Download Java not Java JDK
# -Recommended to run sentistrength on the csv containing the column that contains full text

#The location of SentiStrength on your computer
SentiStrengthLocation = "C:/Users/ThinkPad/SpyderProjects/sentistrengthStuff/SentiStrength.jar" 

#The location of the unzipped SentiStrength data files on your computer
SentiStrengthLanguageFolder = "C:/Users/ThinkPad/SpyderProjects/sentistrengthStuff/SentiStrength_Data/" 

#Check if the paths are correct (if the paths are correct, you will see no flags thrown)
if not os.path.isfile(SentiStrengthLocation):
    print("SentiStrength not found at: ", SentiStrengthLocation)
if not os.path.isdir(SentiStrengthLanguageFolder):
    print("SentiStrength data folder not found at: ", SentiStrengthLanguageFolder)

# Initiate an object
senti = PySentiStr()

# set paths
senti.setSentiStrengthPath(SentiStrengthLocation) 
senti.setSentiStrengthLanguageFolderPath(SentiStrengthLanguageFolder) 


# Read csv (give your path)
all_files = glob.glob("C:/Users/ThinkPad/SpyderProjects/sentistrengthStuff/entropy files" + "/*.csv")

li = []

#Make a dataframe from appending lists
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0, error_bad_lines=False)
    li.append(df)

main_frame = pd.concat(li, axis=0, ignore_index=True)


#Remove emojis not emoticons function
def give_good_text(text):
    text= re.sub(r'\s+', ' ', text) #remove text formattings
    return emoji.get_emoji_regexp().sub(r'', text)#.decode('utf8')) #uncomment the other part if IDE is not set to utf-8


# =============================================================================
# complete_text = main_frame.at[main_frame.index[0], 'tweet_text'].startswith('RT') and not pd.isnull(main_frame.at[main_frame.index[0], 'retweet_text'])
# =============================================================================

#Fill a list with complete text
complete_text=[]

#traverse through data.frame
for row in main_frame.itertuples():
    #if NaN is encountered then skip the kiloop
    if(pd.isnull(row.tweet_text)):
        complete_text.append(row.tweet_text)
        continue
    #check if tweet beings with RT & adjacent row is not null, if true, then fill with adjacent row
    if(row.tweet_text.startswith('RT') and not pd.isnull(row.retweet_text)):
        complete_text.append(give_good_text(row.retweet_text))
    else:
        complete_text.append(give_good_text(row.tweet_text)) #If doesn't start with RT, then put tweet row

#attach the column
main_frame['complete_text'] = complete_text  

#remove NaN and replace it with 'NA' {we would get [(1,-1,0)] in sentistrength} Rare case
main_frame['complete_text'] = main_frame['complete_text'].fillna('NA')


#To improve scores of sentistren we have to limit our results to english accounts
def check_en(x):
    #check if the text is english, it does fail at spcial utf-8 punctuation
    try: 
        if(detect(x)=='en'):
            return(True)
        else:
            return(False)
    except:
        return(False)


main_frame['eng_only'] = main_frame.complete_text.apply(check_en)


#write to file now TEMP
main_frame.to_csv("temp_with_en.csv", encoding='utf-8', index= False)

#Run sentistrength on completed_text

# =============================================================================
# #Function that runs Sentistrength per-line (custom made, only gives POS, NEG Score)
# def RateSentiment(sentiString):
#     #open a subprocess using shlex to get the command line string into the correct args list format
#     p = subprocess.Popen(shlex.split("java -jar '" + SentiStrengthLocation + "' stdin sentidata '" + 
#                                      SentiStrengthLanguageFolder + "'"),
#                          stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
#     
#     #communicate via stdin the string to be rated. Note that all spaces are replaced with +
#     b = bytes(sentiString.replace(" ","+"), 'utf-8') #Can't send string in Python 3, must send bytes
#     
#     stdout_byte, stderr_text = p.communicate(b)
#     stdout_text = stdout_byte.decode("utf-8")  #convert from byte
#     
#     #remove the tab spacing between the positive and negative ratings
#     stdout_text = stdout_text.rstrip().replace("\t"," ") 
#     
#     return stdout_text + " " + sentiString
# 
# #Give the dual score (pos, neg) eg: this one below gives out (3, -3)
# print(RateSentiment("Power, time, gravity, love. The forces that really kick ass are all invisible.")) 
# =============================================================================

#Fill a list with complete text
senti_stren_tri= senti.getSentiment(main_frame['complete_text'], score='trinary')

#attach the column of sentistrength score
main_frame['senti_stren_tri'] = senti_stren_tri   


#unlist all these sentistrength to their emotion
main_frame[['positive','negative','neutral']] = pd.DataFrame(main_frame.senti_stren_tri.tolist(), index= main_frame.index)


#write a final csv
main_frame.to_csv("temp_with_en.csv", encoding='utf-8', index= False)

#EOF