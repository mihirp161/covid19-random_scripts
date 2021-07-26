options(scipen = 9999999, digits = 22)
library(data.table)
library(dplyr)
library(bit64)
library(readxl)
library(openxlsx)
library(lubridate)

#------------------------------------------- file reading and collapsing ----------------------------------------------------

# first read all the files in BOT_CSV folder (Give the path to folder, ending /)
setwd('./stream_rest_botometer/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x))
botcsv_data <- data.table::rbindlist(temp, fill = T) #make a df


#create a directory
setwd('..') #goes back once


#read stream data
stream_data <- readxl::read_xlsx("CH_stream_API.xlsx")

#converts id_str to int64
stream_data$user.id_str <- bit64::as.integer64(stream_data$user.id_str)

#join the bot and stream data
bot_and_stream_df <- dplyr::left_join(stream_data, botcsv_data %>%
                                                    dplyr::select(user_user_data_id_str,cap_english, cap_universal),
                                      by= c("user.id_str" = "user_user_data_id_str"))

# filter botometer values and assign whether user is a bot

bot_and_stream_df <- bot_and_stream_df %>%
                        dplyr::mutate(bot_or_not = dplyr::case_when(cap_english <= 0.7 ~ "not_a_bot",
                                                                    cap_english > 0.7 ~ "bot",
                                                                    is.na(cap_english) ~ "unknown",))

#write xlsx file
openxlsx::write.xlsx(bot_and_stream_df, "CH_stream_API_with_botometer.xlsx")



# descriptive stats

#num
users_tallied_tweets <- bot_and_stream_df %>%
                          dplyr::group_by(user.screen_name,user.id_str,bot_or_not) %>%
                          dplyr::summarise(tweets_made = dplyr::n())


tweet_compositionbots_notbot <- bot_and_stream_df %>%
                        dplyr::group_by(bot_or_not) %>%
                        dplyr::summarise(tweet_composition = dplyr::n())



hourly_activity <- bot_and_stream_df %>%
  dplyr::mutate(date= as.POSIXct(created_at, 
                                 format="%a %b %d %H:%M:%S +0000 %Y", tz="GMT")) %>%
  dplyr:: group_by(date= lubridate::floor_date(date, unit = "1 hour"),
                   bot_or_not) %>%
  dplyr::summarize(tweet_activity_per_group=dplyr::n())



list_of_datasets <- list("#ofTweetPerUsernameId" = users_tallied_tweets,
                         "batch_1TweetsComposition" = tweet_compositionbots_notbot,
                         "hourlyActivity"= hourly_activity)

write.xlsx(list_of_datasets, file = "streameddata_batch1_rawstats.xlsx")
