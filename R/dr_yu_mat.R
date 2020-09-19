# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This file was created to facilitate Dr Yu's work. He has a specific csv format
#* so use this file to make what he likes. This one greates a dfm like matrix.
##-------------

options(scipen = 9999)
library(data.table)
library(qdapRegex)
library(tidyverse)
#library(bit64)
library(tidyr)

#--------------------------- hashtags mentions ---------------------------------------
#Read in the hashtag and mention files

# set the path of where the csv files are and end the file path with / character. In windows swap \ with /

setwd("/files_path/")

#filenames
files <- list.files( pattern="*.csv$")

#read
temp <- lapply(files, function(x) readr::read_csv(x))

hash_data <- data.table::rbindlist(temp, fill = T) #make a df


#--------------------------- queried hashtags tweet---------------------------------------------
#Read in files that have been filtered with queried hastags

setwd("/files_path/")

#filenames
files <- list.files( pattern="*.csv$")

#read
temp <- lapply(files, function(x) readr::read_csv(x))

tweet_data <- data.table::rbindlist(temp, fill = T) #make a df

#-------------------------------------------------- reply ------------------------------------------
#Read in the csv files containing replies

setwd("/files_path/")

#filenames
files <- list.files( pattern="*.csv$")

#read
temp <- lapply(files, function(x) readr::read_csv(x))

reply_data <- data.table::rbindlist(temp, fill = T) #make a df

#--------------------------------------------------------- user ------------------------------------------
#Read in the csv file that have all the user related information

setwd("/files_path/")


#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

user_data <- data.table::rbindlist(temp, fill = T) #make a df


setwd("..")

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

#attach mentions by grouping all the texts then extrating only the @ part.
# FYI, coalesce() function merges columns and takes adjacent values if any value is NA
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

#get the dates
merged_1_2_user$just_date <- as.Date(merged_1_2_user$created_at, format ="%a %b %d %H:%M:%S %z %Y", tz = "GMT") 

#------------------------------------------------- making the "matrix" --------------------------------------------

#keep the columns which needs to be talliedc
merged_1_2_user <- merged_1_2_user %>% 
                    dplyr::select(just_date, Hashtag, user.screen_name)

#group and count the columns
combination_tbl <- merged_1_2_user %>%
                    dplyr::group_by(just_date, user.screen_name) %>%
                    dplyr::tally()

#change the date to monthname-day
combination_tbl$just_date <- format(combination_tbl$just_date, format = "%b-%d")

#pivot the table
combination_tbl <- combination_tbl %>%
                    tidyr::pivot_wider(names_from = just_date, values_from = n) %>%
                    dplyr::mutate(
                      dplyr::across(dplyr::everything(), ~replace_na(.x, 0))
                    )

#write the data.frame to file
readr::write_excel_csv(combination_tbl, "matrix_file.csv")

#EOF