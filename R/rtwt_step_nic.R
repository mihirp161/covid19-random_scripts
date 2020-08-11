# FOR USF SAIL
# ~Dre, Mihir

options(scipen = 99999)
library(httpuv)
library(rtweet)
library(tidyverse)

setwd('C:\\Users\\ThinkPad\\Downloads\\Conspiracy Theory-selected')

# # authenticate via web browser
# token <- rtweet::create_token(
#                     app = "nic_tweet_id_str",
#                     consumer_key = api_key,
#                     consumer_secret = api_secret_key)
# 
# 
# ## store api keys 
# api_key <- "XKdUTglQIhHnt8gPyYHmovYrJ"
# api_secret_key <- "1aZbXHQg4M2ItLoE88GWRlLlWswglW0p1Hdj9Wev8jT67I7ctA"
# access_token <- "1184096866402590720-BXIReNmN4zzAV7aEF6VqnIx0KwBMcR"
# access_token_secret <- "xPskjFYk6SKlTG0qHBzfGL4TqBHPzl1CqJxfyqCw9Gz1J"
# 
# ## authenticate via web browser
# token <- rtweet::create_token(
#                     app = "nic_tweet_id_str",
#                     consumer_key = api_key,
#                     consumer_secret = api_secret_key,
#                     access_token = access_token,
#                     access_secret = access_token_secret)

#rtweet::get_token()

id_str_df <- readr::read_csv("tweet_id_str_Dre.csv")

id_str_df$id_str <- as.character(id_str_df$id_str)

# split the df every 90k
groups <- (split(id_str_df, (seq(nrow(id_str_df))-1) %/% 90000))


setwd('..')


#loop through end of each split, and get the status id
for (i in seq_along(groups)) {
  
  #get all the status from 90k group
  x <- rtweet::lookup_statuses(groups[[i]]$id_str)
  
  #shadow copy do we can rbind dataframe
  if(i <= 1){
    df <- x
    df <- df[0,]
  }
  
  #but the df together
  df <- dplyr::bind_rows(df, x)

  
  message("timeout for 15 mins...")
  #sleep for 15 min
  Sys.sleep(901)
}

#write to file
readr::write_excel_csv(df, "df_nic_status_complete.csv")