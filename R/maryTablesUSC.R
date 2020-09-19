# SAIL Labs
# ~Dre, Mihir

## Description:
##-------------
#* Converts json and jsonl files to csvs. Takes a batch script to detect file inputs.
#* This is a "full" version of the json to csv file. Then it moves all the data to
#* designated locations.
##-------------


options(scipen = 999999)
library(dplyr)
library(readr)
library(stringr)
library(tidyselect)
library(ndjson)
library(data.table)
library(tibble)
library(purrr)

#------------------------------------------- File paths and destination paths ---------------------------------#

args <- commandArgs(trailingOnly = FALSE)
fileNames_all <- args[6] 
fileLocation <- fileNames_all

print(paste0("Reading.....", fileLocation))

endLocation_pre <- stringr::str_extract_all(fileLocation, '[^/]+')
endLocation_pre <- sapply(endLocation_pre,tail, 1) #access the last elemnt of list, it will have .jsonl

print(paste0("true file name: ", endLocation_pre))

#-------------------------------- Read in the json/l files ---------------------------------------------------#

#read the json here and make whatever
message("starting...")

parsedTweets <- ndjson::stream_in(fileLocation, cls="dt") %>% dplyr::filter(lang == "en")

parsedTweets[parsedTweets == "NA"]  <- ""

#------------------------------ Get only the distict columns ------------------------------------------------#

cols <- c(created_at= NA_real_,
          id_str= NA_real_,
          full_text= NA_real_, 
          in_reply_to_status_id_str= NA_real_,
          in_reply_to_user_id_str= NA_real_,
          in_reply_to_screen_name= NA_real_,
          retweet_count= NA_real_,
          favorite_count= NA_real_,
          retweeted_status.created_at= NA_real_,
          retweeted_status.id_str= NA_real_,
          retweeted_status.full_text= NA_real_, 
          retweeted_status.retweet_count= NA_real_,
          retweeted_status.favorite_count= NA_real_,
          user.id_str= NA_real_, 
          user.screen_name= NA_real_,
          user.description= NA_real_,
          user.location= NA_real_, 
          user.followers_count= NA_real_, 
          user.friends_count= NA_real_,
          user.listed_count= NA_real_,
          user.statuses_count= NA_real_, 
          user.favourites_count= NA_real_, 
          user.created_at= NA_real_, 
          retweeted_status.user.id_str= NA_real_,
          retweeted_status.user.screen_name= NA_real_,
          retweeted_status.user.description= NA_real_,
          retweeted_status.user.location= NA_real_,
          retweeted_status.user.followers_count= NA_real_,
          retweeted_status.user.friends_count= NA_real_, 
          retweeted_status.user.listed_count= NA_real_,
          retweeted_status.user.statuses_count= NA_real_,
          retweeted_status.user.favourites_count= NA_real_,
          retweeted_status.retweet_count= NA_real_,
          retweeted_status.user.created_at= NA_real_,
          retweeted_status.quoted_status.created_at= NA_real_,
          retweeted_status.quoted_status.id_str= NA_real_,
          retweeted_status.quoted_status.full_text= NA_real_,
          retweeted_status.quoted_status.retweet_count= NA_real_,
          retweeted_status.quoted_status.favorite_count= NA_real_,
          retweeted_status.quoted_status.user.id_str= NA_real_,
          retweeted_status.quoted_status.user.screen_name= NA_real_,
          retweeted_status.quoted_status.user.description= NA_real_,
          retweeted_status.quoted_status.user.location= NA_real_,
          retweeted_status.quoted_status.user.followers_count= NA_real_,
          retweeted_status.quoted_status.user.friends_count= NA_real_,
          retweeted_status.quoted_status.user.listed_count= NA_real_,
          retweeted_status.quoted_status.user.favourites_count= NA_real_,
          retweeted_status.quoted_status.user.statuses_count= NA_real_,
          place.full_name= NA_real_,
          retweeted_status.place.full_name= NA_real_,
          retweeted_status.quoted_status.place.full_name= NA_real_,
          place.country_code= NA_real_,
          retweeted_status.place.country_code= NA_real_,
          retweeted_status.quoted_status.place.country_code= NA_real_,
          user.verified= NA_real_, 
          retweeted_status.user.verified= NA_real_,
          retweeted_status.quoted_status.user.verified= NA_real_,
          quoted_status.place.full_name= NA_real_,
          quoted_status.user.location= NA_real_,
          user.name= NA_real_,
          user.time_zone= NA_real_,
          user.url= NA_real_,
          place.id= NA_real_)


parsedTweets<- tibble::add_column(parsedTweets, !!!cols[dplyr::setdiff(names(cols), names(parsedTweets))])

#singly columns, put additional ones in the furture
distinct_cols <- c("created_at",
                   "id_str",
                   #"timestamp_ms",
                   "full_text", 
                   "in_reply_to_status_id_str",
                   "in_reply_to_user_id_str",
                   "in_reply_to_screen_name",
                   "retweet_count",
                   "favorite_count",
                   "retweeted_status.created_at",
                   "retweeted_status.id_str",
                   "retweeted_status.full_text", 
                   "retweeted_status.retweet_count",
                   "retweeted_status.favorite_count",
                   "user.id_str", 
                   "user.screen_name",
                   "user.description",
                   "user.location", 
                   "user.followers_count", 
                   "user.friends_count",
                   "user.listed_count",
                   "user.statuses_count", 
                   "user.favourites_count", 
                   "user.created_at", 
                   "retweeted_status.user.id_str",
                   "retweeted_status.user.screen_name",
                   "retweeted_status.user.description",
                   "retweeted_status.user.location",
                   "retweeted_status.user.followers_count",
                   "retweeted_status.user.friends_count", 
                   "retweeted_status.user.listed_count",
                   "retweeted_status.user.statuses_count",
                   "retweeted_status.user.favourites_count",
                   "retweeted_status.retweet_count",
                   "retweeted_status.user.created_at",
                   "retweeted_status.quoted_status.created_at",
                   "retweeted_status.quoted_status.id_str",
                   "retweeted_status.quoted_status.full_text",
                   "retweeted_status.quoted_status.retweet_count",
                   "retweeted_status.quoted_status.favorite_count",
                   "retweeted_status.quoted_status.user.id_str",
                   "retweeted_status.quoted_status.user.screen_name",
                   "retweeted_status.quoted_status.user.description",
                   "retweeted_status.quoted_status.user.location",
                   "retweeted_status.quoted_status.user.followers_count",
                   "retweeted_status.quoted_status.user.friends_count",
                   "retweeted_status.quoted_status.user.listed_count",
                   "retweeted_status.quoted_status.user.favourites_count",
                   "retweeted_status.quoted_status.user.statuses_count",
                   "place.full_name",
                   "retweeted_status.place.full_name",
                   "retweeted_status.quoted_status.place.full_name",
                   "place.country_code",
                   "retweeted_status.place.country_code",
                   "retweeted_status.quoted_status.place.country_code",
                   "user.verified", 
                   "retweeted_status.user.verified",
                   "retweeted_status.quoted_status.user.verified",
                   "quoted_status.place.full_name",
                   "quoted_status.user.location",
                   "user.name",
                   "user.time_zone",
                   "user.url",
                   "place.id")

# make a distinct column df

distinct_dat <- parsedTweets %>% 
                dplyr::select(dplyr::all_of(distinct_cols))


distinct_dat <- tibble::add_column(distinct_dat, timestamp_ms = "", .before = "full_text")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.extended_tweet.full_text = "", .after = "user.url")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.quote_count = "", .after = "place.id")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.reply_count = "", .after = "user.time_zone")
distinct_dat <- tibble::add_column(distinct_dat, extended_tweet.full_text = "", .after = "user.verified")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.quoted_status.extended_tweet.full_text = "", .after = "place.country_code")

message("binding cols done...")

#----------------------------------------------------------------------------------------------------------

# creates filename from existing string

locationData <- distinct_dat %>% 
  dplyr::select(id_str,
                place.full_name,
                place.id,
                quoted_status.place.full_name,
                retweeted_status.quoted_status.place.full_name,
                user.location,
                quoted_status.user.location,
                retweeted_status.quoted_status.user.location,
                retweeted_status.user.location,
                #place.country_code,
                retweeted_status.place.country_code,
                retweeted_status.place.full_name)%>% #,
                dplyr::rename(tweet.id_str = id_str)
                #retweeted_status.quoted_status.place.country_code) %>%
                #dplyr::rename(tweet.id_str = id_str)

#print(head(locationData))

userData <- distinct_dat %>% 
  dplyr::select(user.id_str,
                user.screen_name, 
                user.name,
                user.location,
                user.description, 
                user.friends_count,
                user.followers_count,
                user.favourites_count,
                user.statuses_count,
                user.time_zone,
                user.verified, 
                user.created_at,
                user.url)

#print(head(userData))

tweetData <- distinct_dat %>% 
  dplyr::select(user.id_str,
                user.screen_name,
                id_str, 
                created_at,
                text= full_text,
                extended_tweet.full_text,
                timestamp_ms, 
                retweeted_status.created_at,
                retweeted_status.id_str,
                retweeted_status.extended_tweet.full_text,
                retweeted_status.text= retweeted_status.full_text,
                retweeted_status.user.id_str,
                retweeted_status.user.screen_name,
                retweeted_status.user.location,
                retweeted_status.user.verified,
                retweeted_status.user.followers_count,
                retweeted_status.user.friends_count,
                retweeted_status.user.statuses_count,
                retweeted_status.retweet_count,
                retweeted_status.quote_count,
                retweeted_status.reply_count,
                retweeted_status.favorite_count,
                place.id)

#print(head(tweetData))


locationDataLoc <- "/location_path/"
userDataLoc <- "/user_path/"
tweetDataLoc <- "/tweet_path/"


endLocation_pre <- sub('\\..*$', '', endLocation_pre)

readr::write_excel_csv(x = locationData, path = paste0(locationDataLoc,endLocation_pre, "_locationTable.csv"))
readr::write_excel_csv(x = tweetData, path = paste0(tweetDataLoc,endLocation_pre, "_tweetData.csv"))
readr::write_excel_csv(x = userData, path = paste0(userDataLoc,endLocation_pre, "_userData.csv"))

print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()

#EOF