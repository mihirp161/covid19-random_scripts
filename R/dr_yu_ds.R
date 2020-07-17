# For USF SAIL
# ~Mihir

options(scipen = 9999)
library(data.table)
library(qdapRegex)
library(tidyverse)
library(bit64)
library(tidyr)

#--------------------------- hashtags mentions ---------------------------------------
#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/hastags_mentionsr')
setwd("C:\\Users\\ThinkPad\\SpyderProjects\\USCStuff\\yu_stuff\\mentions_hastags")

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

hash_data <- data.table::rbindlist(temp, fill = T) #make a df


#--------------------------- queried hashtags tweet---------------------------------------------
#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /

#setwd('/work_bgfs/m/mkpatel/SCRIPT/shit_hastags/tweets_filtered')
setwd("C:\\Users\\ThinkPad\\SpyderProjects\\USCStuff\\yu_stuff\\filtered_tweet")

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

tweet_data <- data.table::rbindlist(temp, fill = T) #make a df

#-------------------------------------------------- reply ------------------------------------------
#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/replyr')
setwd("C:\\Users\\ThinkPad\\SpyderProjects\\USCStuff\\yu_stuff\\reply")

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

reply_data <- data.table::rbindlist(temp, fill = T) #make a df

#--------------------------------------------------------- user ------------------------------------------
#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/userr')
setwd("C:\\Users\\ThinkPad\\SpyderProjects\\USCStuff\\yu_stuff\\user")


#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

user_data <- data.table::rbindlist(temp, fill = T) #make a df


#------------------------------------------------------- data wrangle -------------------------------------------

#join tweet, reply, hash on user.id_str & id_str, then user with user.id_str

#1) join reply_ & hash
merged_repl_hash <- dplyr::inner_join(reply_data, hash_data, by = c("user.id_str","id_str") , copy = FALSE)
merged_repl_hash <- merged_repl_hash[!duplicated(merged_repl_hash$id_str), ]

#release memories
rm(reply_data,hash_data)


#2) join 1) and tweet
merged_1_tweet <- dplyr::inner_join(merged_repl_hash, tweet_data, by = c("user.id_str","id_str") , copy = FALSE)
merged_1_tweet <- merged_1_tweet[!duplicated(merged_1_tweet$id_str), ]

rm(tweet_data,merged_repl_hash)

#3) join 2) and user df

merged_1_2_user <- dplyr::inner_join(merged_1_tweet, user_data, by = c("user.id_str", "user.screen_name") , copy = FALSE)
merged_1_2_user <- merged_1_2_user[!duplicated(merged_1_2_user$id_str), ]

rm(user_data,merged_1_tweet)

#---------------------------------------------- make structure  -------------------------------------------------
#attach hashtags
merged_1_2_user$Hashtag <- sapply(qdapRegex::rm_hash(merged_1_2_user$complete_texts, extract = T), paste0, collapse=";")

#attach mentions by grouping all the texts
merged_1_2_user$Mentions <- sapply(stringr::str_extract_all(transform(merged_1_2_user, 
                                                                      newcol= paste(
                                                                        dplyr::mutate(merged_1_2_user, 
                                                                                      mycoltext = dplyr::coalesce(extended_tweet.full_text, 
                                                                                                                  text))$mycoltext,
                                                                        dplyr::mutate(merged_1_2_user, 
                                                                                      mycolrttext = dplyr::coalesce(retweeted_status.extended_tweet.full_text,
                                                                                                                    retweeted_status.text))$mycolrttext,
                                                                        sep=" "))$newcol,
                                                            "@[[:alnum:]_]+"), 
                                   paste0, collapse=";")


#copy mentions to new column
merged_1_2_user$Target_Screen_name <- merged_1_2_user$Mentions

#make new rows
merged_1_2_user <- tidyr::separate_rows(merged_1_2_user, Target_Screen_name, sep = ";")

#find if tweet user retweeted
merged_1_2_user$Source_Retweets <- ifelse(startsWith(merged_1_2_user$text,"RT"), 1, 0)

#find if tweet user has replied 
merged_1_2_user$Source_Replies <- ifelse(startsWith(merged_1_2_user$text,"@"), 1, 0)

#make fina df
merged_1_2_user <- merged_1_2_user %>%
  dplyr::select(Hashtag, 
                Screenname= user.screen_name,
                Target_Screen_name,
                Source_Replies,
                Source_Retweets,
                Source_joined= user.created_at,
                Source_location= user.location,
                Source_description= user.description,
                Source_verified= user.verified,
                Source_followers= user.followers_count,
                Source_following= user.friends_count,
                Date= created_at,
                Tweet_text= complete_texts,
                Mentions)

readr::write_excel_csv(merged_1_2_user, "plandemic_data_march_june.csv")
     
