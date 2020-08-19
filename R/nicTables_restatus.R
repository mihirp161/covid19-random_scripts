# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This file get the hydrated json/l file which we made for Dr Nic and only makes csvs
#* with columns which we couldn't get from our stream collecting method.
##-------------

options(scipen = 999999)
library(dplyr)
library(readr)
library(stringr)
library(tidyselect)
library(ndjson)
library(data.table)

#Read in the json/l file

parsedTweets <- ndjson::stream_in("tweet_id_str_Mihir_plan.jsonl", cls="dt") %>% dplyr::filter(lang == "en")

parsedTweets[parsedTweets == "NA"]  <- ""

# select only the items we need
final_df <- parsedTweets %>% dplyr::select(id_str, retweet_count, favorite_count, full_text)

#write to a file now
readr::write_excel_csv(final_df, "plandemic_rested_status_nic.csv")

#EOF