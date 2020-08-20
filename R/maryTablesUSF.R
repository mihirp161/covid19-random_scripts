# SAIL Labs
# ~Dre, Mihir

## Description:
##-------------
#* THis is similar to USC data processing technique. However there are couple of arrays in USF 
#* data that have different name. So to reduce the variable conflicts, we just created another script.
#* This will take a batch script, and read each json file, the convert it to its csv equivalent.
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

endLocation_pre <- substr(fileLocation, 41, 53)

#-------------------------------- Read in the json/l files ---------------------------------------------------#
#read the json here and make whatever
message("starting with...")
message(endLocation_pre)

parsedTweets <- ndjson::stream_in(fileLocation, cls="dt") %>% dplyr::filter(lang == "en")

parsedTweets[parsedTweets == "NA"]  <- ""

##------------------------------ Combine all the multiple columns into one, by '|'---------------------------#

#*************TO DO*************#
#* CONVERT THESE TO FUNCTION    #
#*******************************#

# Then here, & because we only care about the column names, we will read only the first row of the file
multi_cols_names <- colnames(parsedTweets)

# empty dataframe with size of original data
multiples_dat <- data.frame(row.names = 1:nrow(parsedTweets))

#---------------- Mentions

# Mentions screen names
if(identical(grep("^entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$mentions_screen_name <- ""
}else{
  mentions_screen_name <- c(multi_cols_names[grep("^entities.user_mentions.*_name$", multi_cols_names, ignore.case = T)])
  multiples_dat$mentions_screen_name <- apply(parsedTweets[ , mentions_screen_name, with= F] ,
                                              1 , paste , collapse = '; ')
  
  multiples_dat$mentions_screen_name <- gsub('NA|NA; ', '', multiples_dat$mentions_screen_name)
}

#mentions id_strs
if(identical(grep("^entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$mentions_id_str <- ""
}else{
  mentions_id_str <- c(multi_cols_names[grep("^entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
  multiples_dat$mentions_id_str <- apply(parsedTweets[ , mentions_id_str, with= F] ,
                                         1 , paste , collapse = '; ')
  
  multiples_dat$mentions_id_str <- gsub('NA|NA; ', '', multiples_dat$mentions_id_str)
}

# Retweet mentions screen names
if(identical(grep("^retweeted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$rt_mentions_screen_name <- ""
}else{
  rt_mentions_screen_name <- c(multi_cols_names[grep("^retweeted_status.entities.user_mentions.*_name$",
                                                     multi_cols_names, ignore.case = T)])
  multiples_dat$rt_mentions_screen_name <- apply(parsedTweets[ , rt_mentions_screen_name, with= F] ,
                                                 1 , paste , collapse = '; ')
  
  multiples_dat$rt_mentions_screen_name <- gsub('NA|NA; ', '', multiples_dat$rt_mentions_screen_name)
}

#Retweet mentions id_strs
if(identical(grep("^retweeted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$rt_mentions_id_str <- ""
}else{
  rt_mentions_id_str <- c(multi_cols_names[grep("^retweeted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
  multiples_dat$rt_mentions_id_str <- apply(parsedTweets[ , rt_mentions_id_str, with= F] ,
                                            1 , paste , collapse = '; ')
  
  multiples_dat$rt_mentions_id_str <- gsub('NA|NA; ', '', multiples_dat$rt_mentions_id_str)
}

#------------------------- quoted

# Quoted mentions screen names
if(identical(grep("^retweeted_status.quoted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$rt_qt_mentions_screen_name <- ""
}else{
  rt_qt_mentions_screen_name <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.user_mentions.*_name$",
                                                     multi_cols_names, ignore.case = T)])
  multiples_dat$rt_qt_mentions_screen_name <- apply(parsedTweets[ , rt_qt_mentions_screen_name, with= F] ,
                                                 1 , paste , collapse = '; ')
  
  multiples_dat$rt_qt_mentions_screen_name <- gsub('NA|NA; ', '', multiples_dat$rt_qt_mentions_screen_name)
}

#Quoted mentions id_strs
if(identical(grep("^retweeted_status.quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$rt_qt_mentions_id_str <- ""
}else{
  rt_qt_mentions_id_str <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
  multiples_dat$rt_qt_mentions_id_str <- apply(parsedTweets[ , rt_qt_mentions_id_str, with= F] ,
                                            1 , paste , collapse = '; ')
  
  multiples_dat$rt_qt_mentions_id_str <- gsub('NA|NA; ', '', multiples_dat$rt_qt_mentions_id_str)
}

# Quoted mentions screen names
if(identical(grep("^quoted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$qt_mentions_screen_name <- ""
}else{
  qt_mentions_screen_name <- c(multi_cols_names[grep("^quoted_status.entities.user_mentions.*_name$",
                                                     multi_cols_names, ignore.case = T)])
  multiples_dat$qt_mentions_screen_name <- apply(parsedTweets[ , qt_mentions_screen_name, with= F] ,
                                                 1 , paste , collapse = '; ')
  
  multiples_dat$qt_mentions_screen_name <- gsub('NA|NA; ', '', multiples_dat$qt_mentions_screen_name)
}

#Quoted mentions id_strs
if(identical(grep("^quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$qt_mentions_id_str <- ""
}else{
  qt_mentions_id_str <- c(multi_cols_names[grep("^quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
  multiples_dat$qt_mentions_id_str <- apply(parsedTweets[ , qt_mentions_id_str, with= F] ,
                                            1 , paste , collapse = '; ')
  
  multiples_dat$qt_mentions_id_str <- gsub('NA|NA; ', '', multiples_dat$qt_mentions_id_str)
}


#---------------- Hashtags
# get hashtags for the user
if(identical(grep("^entities.hashtags.*.text$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$hashtags <- ""
}else{
  hashtags <- c(multi_cols_names[grep("^entities.hashtags.*.text$", multi_cols_names, ignore.case = T)])
  multiples_dat$hashtags <- apply(parsedTweets[ , hashtags, with= F] ,
                                  1 , paste , collapse = '; ')
  
  multiples_dat$hashtags <- gsub('NA|NA; ', '', multiples_dat$hashtags)
}

# get hashtags for the retweeted user
if(identical(grep("^retweeted_status.entities.hashtags.*.text$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$rt_hashtags <- ""
}else{
  rt_hashtags <- c(multi_cols_names[grep("^retweeted_status.entities.hashtags.*.text$", multi_cols_names, ignore.case = T)])
  multiples_dat$rt_hashtags <- apply(parsedTweets[ , rt_hashtags, with= F] ,
                                     1 , paste , collapse = '; ')
  
  multiples_dat$rt_hashtags <- gsub('NA|NA; ', '', multiples_dat$rt_hashtags)
}

#------------------------- quoted

# get hashtags for the retweeted user
if(identical(grep("^retweeted_status.quoted_status.entities.hashtags.*.text$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$rt_qt_hashtags <- ""
}else{
  rt_qt_hashtags <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.hashtags.*.text$", multi_cols_names, ignore.case = T)])
  multiples_dat$rt_qt_hashtags <- apply(parsedTweets[ , rt_qt_hashtags, with= F] ,
                                     1 , paste , collapse = '; ')
  
  multiples_dat$rt_qt_hashtags <- gsub('NA|NA; ', '', multiples_dat$rt_qt_hashtags)
}

# get hashtags for the retweeted user
if(identical(grep("^quoted_status.entities.hashtags.*.text$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$qt_hashtags <- ""
}else{
  qt_hashtags <- c(multi_cols_names[grep("^quoted_status.entities.hashtags.*.text$", multi_cols_names, ignore.case = T)])
  multiples_dat$qt_hashtags <- apply(parsedTweets[ , qt_hashtags, with= F] ,
                                     1 , paste , collapse = '; ')
  
  multiples_dat$qt_hashtags <- gsub('NA|NA; ', '', multiples_dat$qt_hashtags)
}


#------------------------------ Get only the distict columns ------------------------------------------------#

cols <- c(created_at= NA_real_,
          id_str= NA_real_,
          timestamp_ms= NA_real_,
          text= NA_real_, 
          in_reply_to_status_id_str= NA_real_,
          in_reply_to_user_id_str= NA_real_,
          in_reply_to_screen_name= NA_real_,
          retweet_count= NA_real_,
          favorite_count= NA_real_,
          retweeted_status.created_at= NA_real_,
          retweeted_status.id_str= NA_real_, 
          retweeted_status.text= NA_real_, 
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
          retweeted_status.quoted_status.text= NA_real_,
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
          retweeted_status.extended_tweet.full_text= NA_real_,
          retweeted_status.quote_count= NA_real_,
          retweeted_status.reply_count= NA_real_,
          place.id= NA_real_,
          entities.hashtags.0.text= NA_real_,
          entities.hashtags.0.indices.1= NA_real_,
          entities.hashtags.0.indices.0= NA_real_,
          extended_tweet.full_text= NA_real_,
          retweeted_status.in_reply_to_status_id_str= NA_real_,
          retweeted_status.in_reply_to_screen_name= NA_real_,
          retweeted_status.in_reply_to_user_id_str= NA_real_,
          quoted_status.user.id_str= NA_real_,
          quoted_status.created_at= NA_real_,
          quoted_status.text= NA_real_,
          quoted_status.user.screen_name= NA_real_,
          quoted_status.extended_tweet.full_text= NA_real_,
          quoted_status.id_str= NA_real_,
          quoted_status.in_reply_to_status_id_str= NA_real_,
          quoted_status.in_reply_to_user_id_str= NA_real_,
          quoted_status.in_reply_to_screen_name= NA_real_,
          quoted_status.retweeted_status.in_reply_to_status_id_str= NA_real_,
          quoted_status.user.verified= NA_real_,
          quoted_status.user.followers_count= NA_real_,
          quoted_status.user.friends_count= NA_real_,
          quoted_status.user.statuses_count= NA_real_,
          quoted_status.retweet_count= NA_real_,
          quoted_status.quote_count= NA_real_,
          quoted_status.reply_count= NA_real_,
          quoted_status.favorite_count= NA_real_,
          retweeted_status.quoted_status.user.id_str= NA_real_,
          retweeted_status.quoted_status.created_at= NA_real_,
          retweeted_status.quoted_status.text= NA_real_,
          retweeted_status.quoted_status.user.screen_name= NA_real_,
          retweeted_status.quoted_status.extended_tweet.full_text= NA_real_,
          retweeted_status.quoted_status.id_str= NA_real_,
          retweeted_status.quoted_status.in_reply_to_status_id_str= NA_real_,
          retweeted_status.quoted_status.in_reply_to_user_id_str= NA_real_,
          retweeted_status.quoted_status.in_reply_to_screen_name= NA_real_,
          retweeted_status.quoted_status.user.verified= NA_real_,
          retweeted_status.quoted_status.user.followers_count= NA_real_,
          retweeted_status.quoted_status.user.friends_count= NA_real_,
          retweeted_status.quoted_status.user.statuses_count= NA_real_,
          retweeted_status.quoted_status.retweet_count= NA_real_,
          retweeted_status.quoted_status.quote_count= NA_real_,
          retweeted_status.quoted_status.reply_count= NA_real_,
          retweeted_status.quoted_status.favorite_count= NA_real_,
          quoted_status.quoted_status_id_str= NA_real_)


#put these columns if they don't exist
parsedTweets<- tibble::add_column(parsedTweets, !!!cols[dplyr::setdiff(names(cols), names(parsedTweets))])

#singly columns, put additional ones in the furture
distinct_cols <- c("created_at",
                   "id_str",
                   "timestamp_ms",
                   "text", 
                   "in_reply_to_status_id_str",
                   "in_reply_to_user_id_str",
                   "in_reply_to_screen_name",
                   "retweet_count",
                   "favorite_count",
                   "retweeted_status.created_at",
                   "retweeted_status.id_str", 
                   "retweeted_status.text", 
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
                   "retweeted_status.quoted_status.text",
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
                   "retweeted_status.extended_tweet.full_text",
                   "retweeted_status.quote_count",
                   "retweeted_status.reply_count",
                   "place.id",
                   "entities.hashtags.0.text",
                   "entities.hashtags.0.indices.1",
                   "entities.hashtags.0.indices.0",
                   "extended_tweet.full_text",
                   "retweeted_status.in_reply_to_status_id_str",
                   "retweeted_status.in_reply_to_screen_name",
                   "retweeted_status.in_reply_to_user_id_str",
                   "quoted_status.user.id_str",
                   "quoted_status.created_at",
                   "quoted_status.text",
                   "quoted_status.user.screen_name",
                   "quoted_status.extended_tweet.full_text",
                   "quoted_status.id_str",
                   "quoted_status.in_reply_to_status_id_str",
                   "quoted_status.in_reply_to_user_id_str",
                   "quoted_status.in_reply_to_screen_name",
                   "quoted_status.retweeted_status.in_reply_to_status_id_str",
                   "quoted_status.user.verified",
                   "quoted_status.user.followers_count",
                   "quoted_status.user.friends_count",
                   "quoted_status.user.statuses_count",
                   "quoted_status.retweet_count",
                   "quoted_status.quote_count",
                   "quoted_status.reply_count",
                   "quoted_status.favorite_count",
                   "retweeted_status.quoted_status.user.id_str",
                   "retweeted_status.quoted_status.created_at",
                   "retweeted_status.quoted_status.text",
                   "retweeted_status.quoted_status.user.screen_name",
                   "retweeted_status.quoted_status.extended_tweet.full_text",
                   "retweeted_status.quoted_status.id_str",
                   "retweeted_status.quoted_status.in_reply_to_status_id_str",
                   "retweeted_status.quoted_status.in_reply_to_user_id_str",
                   "retweeted_status.quoted_status.in_reply_to_screen_name",
                   "retweeted_status.quoted_status.user.verified",
                   "retweeted_status.quoted_status.user.followers_count",
                   "retweeted_status.quoted_status.user.friends_count",
                   "retweeted_status.quoted_status.user.statuses_count",
                   "retweeted_status.quoted_status.retweet_count",
                   "retweeted_status.quoted_status.quote_count",
                   "retweeted_status.quoted_status.reply_count",
                   "retweeted_status.quoted_status.favorite_count",
                   "quoted_status.quoted_status_id_str") 

# make a distinct column df

distinct_dat <- parsedTweets %>% dplyr::select(dplyr::all_of(distinct_cols))

distinct_dat <- dplyr::bind_cols(distinct_dat, multiples_dat)

#----------------------------------------------------------------------------------------------------------

locationData <- distinct_dat %>% select(id_str,
                                        place.full_name,
                                        place.id,
                                        quoted_status.place.full_name,
                                        retweeted_status.quoted_status.place.full_name,
                                        user.location,
                                        quoted_status.user.location,
                                        retweeted_status.quoted_status.user.location,
                                        retweeted_status.user.location,
                                        retweeted_status.place.country_code,
                                        retweeted_status.place.full_name) %>%
                      rename(tweet.id_str = id_str)

userData <- distinct_dat %>% select(user.id_str,
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

tweetData <- distinct_dat %>% select(user.id_str,
                                     user.screen_name,
                                     id_str, 
                                     created_at,
                                     text,
                                     extended_tweet.full_text,
                                     timestamp_ms, 
                                     retweeted_status.created_at,
                                     retweeted_status.id_str,
                                     retweeted_status.extended_tweet.full_text,
                                     retweeted_status.text,
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

hashtagData <- distinct_dat %>%
  select(user.id_str,
         id_str,
         entities.hashtags.0.indices.0,
         entities.hashtags.0.indices.1,
         entities.hashtags.0.text
  )

replyData <- distinct_dat %>% 
  dplyr::select(user.id_str,
                id_str,
                in_reply_to_status_id_str,
                in_reply_to_user_id_str,
                in_reply_to_screen_name,
                retweeted_status.in_reply_to_status_id_str,
                retweeted_status.in_reply_to_screen_name,
                retweeted_status.in_reply_to_user_id_str)



#----------------------- quoted

quotedData <- distinct_dat %>%
  dplyr::select(user.id_str,
                id_str,
                quoted_status.user.id_str,
                quoted_status.created_at,
                quoted_status.text,
                quoted_status.user.screen_name,
                quoted_status.extended_tweet.full_text,
                quoted_status.id_str,
                quoted_status.in_reply_to_status_id_str,
                quoted_status.in_reply_to_user_id_str,
                quoted_status.in_reply_to_screen_name,
                quoted_status.retweeted_status.in_reply_to_status_id_str,
                quoted_status.user.verified,
                quoted_status.user.followers_count,
                quoted_status.user.friends_count,
                quoted_status.user.statuses_count,
                quoted_status.retweet_count,
                quoted_status.quote_count,
                quoted_status.reply_count,
                quoted_status.favorite_count,
                retweeted_status.quoted_status.user.id_str,
                retweeted_status.quoted_status.created_at,
                retweeted_status.quoted_status.text,
                retweeted_status.quoted_status.user.screen_name,
                retweeted_status.quoted_status.extended_tweet.full_text,
                retweeted_status.quoted_status.id_str,
                retweeted_status.quoted_status.in_reply_to_status_id_str,
                retweeted_status.quoted_status.in_reply_to_user_id_str,
                retweeted_status.quoted_status.in_reply_to_screen_name,
                retweeted_status.quoted_status.user.verified,
                retweeted_status.quoted_status.user.followers_count,
                retweeted_status.quoted_status.user.friends_count,
                retweeted_status.quoted_status.user.statuses_count,
                retweeted_status.quoted_status.retweet_count,
                retweeted_status.quoted_status.quote_count,
                retweeted_status.quoted_status.reply_count,
                retweeted_status.quoted_status.favorite_count,
                quoted_status.quoted_status_id_str,
                rt_qt_mentions_screen_name,
                rt_qt_mentions_id_str,
                rt_qt_hashtags,
                qt_mentions_screen_name,
                qt_mentions_id_str,
                qt_hashtags)

#----------------------- mentions and hastags


#pick out these distinct columns
distinct_dat2 <-  distinct_dat %>%
                    dplyr::select(user.id_str,
                                  id_str,
                                  mentions_screen_name,
                                  mentions_id_str,
                                  rt_mentions_screen_name,
                                  rt_mentions_id_str,
                                  hashtags,
                                  rt_hashtags)

parsedTweets2 <- distinct_dat2

#--------------------

replyDataLoc <- "/reply_data_path/"
locationDataLoc <- "/location_data_path/"
userDataLoc <- "/user_data_path/"
tweetDataLoc <- "/tweet_data_path/"
hashtagDataLoc <- "/hashtag_data_path/"
mentHashDataLoc <- "/mention_data_path/"
quotedDataLoc <- "/quoted_data_path/"


endLocation_pre <- substr(fileLocation, 41, 53)

readr::write_excel_csv(x = locationData, path = paste0(locationDataLoc,endLocation_pre, "_locationTable.csv"))
readr::write_excel_csv(x = parsedTweets2, path = paste0(mentHashDataLoc,endLocation_pre, "_mentionHastagsTable.csv"))

readr::write_excel_csv(x = replyData, path = paste0(replyDataLoc,endLocation_pre, "_replyTable.csv"))
readr::write_excel_csv(x = tweetData, path = paste0(tweetDataLoc,endLocation_pre, "_tweetData.csv"))
readr::write_excel_csv(x = userData, path = paste0(userDataLoc,endLocation_pre, "_userData.csv"))
readr::write_excel_csv(x = quotedData, path = paste0(quotedDataLoc,endLocation_pre, "_quotedData.csv"))

message("finished with tabbles for: ")
message(endLocation_pre)
print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()

#EOF