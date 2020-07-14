# FOR USF SAIL
# Dre, Mihir

options(scipen = 999999)
#install.packages("dplyr",repos='http://cran.us.r-project.org')
library("dplyr")
#install.packages("readr",repos='http://cran.us.r-project.org')
library("readr")
#install.packages("stringr",repos='http://cran.us.r-project.org')
library("stringr")
#install.packages("tidyselect",repos='http://cran.us.r-project.org')
library("tidyselect")
#install.packages("ndjson", repos='http://cran.us.r-project.org')
library("ndjson")#, lib.loc="/tmp/RtmpDG5v7Q/downloaded_packages")
#install.packages("doParallel")
#library("doParallel")
library("data.table")
# creates a list of all json files in dir as a full string
library("tibble")

library("purrr")

#--------------------------------------------- Parllel logic----------------------------------------------------#
#fileNames <- fileNames_all

# cl <- makePSOCKcluster(5)
# registerDoParallel(cl)

#setwd("WHATEVER-YOUR-PATH-IS")

#------------------------------------------- File paths and destination paths ---------------------------------#
#setwd("/shares_bgfs/si_twitter/Dred-MPColab/Mary_files_April_jsons/")
args <- commandArgs(trailingOnly = FALSE)
fileNames_all <- args[6] #list.files("/shares_bgfs/si_twitter/Dred-MPColab/Mary_files_April_jsons/", "*.jsonl$", full.names = FALSE)
fileLocation <- fileNames_all

print(paste0("Reading.....", fileLocation))

endLocation_pre <- stringr::str_extract_all(fileLocation, '[^/]+')
endLocation_pre <- sapply(endLocation_pre,tail, 1) #access the last elemnt of list, it will have .jsonl

print(paste0("true file name: ", endLocation_pre))
#endLocation_pre <- substr(fileLocation, 88, 120) #not wroking as hoped

# creates a list of all json files in dir as a shortname
#fileNamesShort <- list.files("/shares_bgfs/si_twitter/covid19/USC_March_jsonls_Need_Conversions/", "*_part1.jsonl$", full.names = FALSE)
# creates likst off all csv files in completed dir
#finishedFiles <- list.files("/shares_bgfs/si_twitter/covid19/processedCSVs/", "*.csv", full.names = FALSE)
# establishes output file location
#parsedFolder <- "/Users/dre/Downloads/coronaVirus/test-folder/"
#print(parsedFolder)
#endlocationPost <- paste0(parsedFolder, endLocation_pre, ".csv")
#print(endlocationPost)
#for(i in seq_along(fileNames_all)) {
#-------------------------------- Read in the json/l files ---------------------------------------------------#
#fileNames_all[i]
#read the json here and make whatever
message("starting...")

parsedTweets <- ndjson::stream_in(fileLocation, cls="dt") %>% dplyr::filter(lang == "en")
#parsedTweets2 <- ndjson::stream_in("/Users/dre/Downloads/coronaVirus/demo/2020-05-29-04.json", cls="dt") %>% dplyr::filter(lang == "en")

# data <- files %>%
#         map_df(~fromJSON(file.path(path, .), flatten = TRUE))
# Remove all the NA's in the dataframe
#parsedTweets <- gsub("NA", "", parsedTweets)
parsedTweets[parsedTweets == "NA"]  <- ""
#na.omit(parsedTweets)

##------------------------------ Combine all the multiple columns into one, by '|'---------------------------#

#*************TO DO*************#
#* CONVERT THESE TO FUNCTION    #
#*******************************#

# # Then here, & because we only care about the column names, we will read only the first row of the file
# multi_cols_names <- colnames(parsedTweets)
# 
# # empty dataframe with size of original data
# multiples_dat <- data.frame(row.names = 1:nrow(parsedTweets))
# 
# 
# #------------Coordinates
# # add tweeted coordinates if they exist
# if(identical(grep("^coordinates.coordinates.*", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$user_coordinates <- ""
#   
# }else{
#   user_coordinates <- c(multi_cols_names[grep("^coordinates.coordinates.*", 
#                                               multi_cols_names, ignore.case = T)])
#   
#   multiples_dat$user_coordinates <- apply(parsedTweets[ , user_coordinates, with= F] , 1 , paste , collapse = '|')
# }
# 
# # add retweeted coordinates if they exist
# if(identical(grep("^retweeted_status.coordinates.coordinates.*", multi_cols_names, ignore.case = T),integer(0))){
#   
#   multiples_dat$retweeted_user_coordinates <- ""
# }else{
#   retweeted_user_coordinates <- c(multi_cols_names[grep("^retweeted_status.coordinates.coordinates.*", 
#                                                         multi_cols_names, ignore.case = T)])
#   
#   multiples_dat$retweeted_user_coordinates <- apply(parsedTweets[ , retweeted_user_coordinates, with= F] , 
#                                                     1 , paste , collapse = '|')
# }
# 
# #check if quoted coordinated exist, add it accordingly
# if(identical(grep("^retweeted_status.quoted_status.coordinates.coordinates.*", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$quoted_user_coordinates <- ""
# }else{
#   quoted_user_coordinates <- c(multi_cols_names[grep("retweeted_status.quoted_status.coordinates.coordinates.*", 
#                                                      multi_cols_names, ignore.case = T)])
#   multiples_dat$quoted_user_coordinates <- apply(parsedTweets[ , quoted_user_coordinates, with= F] , 
#                                                  1 , paste , collapse = '|')
# }
# 
# #---------------- Mentions
# 
# # Mentions screen names
# if(identical(grep("^entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$mentions_screen_name <- ""
# }else{
#   mentions_screen_name <- c(multi_cols_names[grep("^entities.user_mentions.*_name$", multi_cols_names, ignore.case = T)])
#   multiples_dat$mentions_screen_name <- apply(parsedTweets[ , mentions_screen_name, with= F] , 
#                                               1 , paste , collapse = '|')
# }
# 
# #mentions id_strs
# if(identical(grep("^entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$mentions_id_str <- ""
# }else{
#   mentions_id_str <- c(multi_cols_names[grep("^entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
#   multiples_dat$mentions_id_str <- apply(parsedTweets[ , mentions_id_str, with= F] , 
#                                          1 , paste , collapse = '|')
# }
# 
# # Retweet mentions screen names
# if(identical(grep("^retweeted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$rt_mentions_screen_name <- ""
# }else{
#   rt_mentions_screen_name <- c(multi_cols_names[grep("^retweeted_status.entities.user_mentions.*_name$", 
#                                                      multi_cols_names, ignore.case = T)])
#   multiples_dat$rt_mentions_screen_name <- apply(parsedTweets[ , rt_mentions_screen_name, with= F] , 
#                                                  1 , paste , collapse = '|')
# }
# 
# #Retweet mentions id_strs
# if(identical(grep("^retweeted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$rt_mentions_id_str <- ""
# }else{
#   rt_mentions_id_str <- c(multi_cols_names[grep("^retweeted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
#   multiples_dat$rt_mentions_id_str <- apply(parsedTweets[ , rt_mentions_id_str, with= F] , 
#                                             1 , paste , collapse = '|')
# }
# 
# # Quoted mentions screen names
# if(identical(grep("^retweeted_status.quoted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$qu_mentions_screen_name <- ""
# }else{
#   qu_mentions_screen_name <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.user_mentions.*_name$", 
#                                                      multi_cols_names, ignore.case = T)])
#   multiples_dat$qu_mentions_screen_name <- apply(parsedTweets[ , qu_mentions_screen_name, with= F] , 
#                                                  1 , paste , collapse = '|')
# }
# 
# # Quoted mentions id_strs
# if(identical(grep("^retweeted_status.quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
#   multiples_dat$qu_mentions_id_str <- ""
# }else{
#   qu_mentions_id_str <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
#   multiples_dat$qu_mentions_id_str <- apply(parsedTweets[ , qu_mentions_id_str, with= F] , 
#                                             1 , paste , collapse = '|')
# }

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
                   #"retweeted_status.extended_tweet.full_text",
                   #"retweeted_status.quote_count",
                   #"retweeted_status.reply_count",
                   "place.id")#,
                   # "entities.hashtags.0.text",
                   # "entities.hashtags.0.indices.1",
                   # "entities.hashtags.0.indices.0") #,
                   #"retweeted_status.entities.urls.url",
                   #"#retweeted_status.extended_entities.media.media_url_https")

# make a distinct column df

distinct_dat <- parsedTweets %>% 
                dplyr::select(dplyr::all_of(distinct_cols))


# 
# namevector <- c("timestamp_ms",
#                 "retweeted_status.extended_tweet.full_text",
#                 "retweeted_status.quote_count",
#                 "retweeted_status.reply_count",
#                 "extended_tweet.full_text",
#                 "retweeted_status.quoted_status.extended_tweet.full_text")
# 
# distinct_dat[ , namevector] <- ""

distinct_dat <- tibble::add_column(distinct_dat, timestamp_ms = "", .before = "full_text")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.extended_tweet.full_text = "", .after = "user.url")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.quote_count = "", .after = "place.id")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.reply_count = "", .after = "user.time_zone")
distinct_dat <- tibble::add_column(distinct_dat, extended_tweet.full_text = "", .after = "user.verified")
distinct_dat <- tibble::add_column(distinct_dat, retweeted_status.quoted_status.extended_tweet.full_text = "", .after = "place.country_code")

message("binding cols done...")


#parsedTweets <- data.table::rbindlist(list(distinct_dat, multiples_dat), fill=TRUE) #no need
#parsedTweets <- dplyr::bind_cols(distinct_dat, multiples_dat)

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
  #               retweeted_status.quoted_status.place.country_code) %>%
  # dplyr::rename(tweet.id_str = id_str)

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
                # in_reply_to_status_id_str,
                # in_reply_to_user_id_str,
                # in_reply_to_screen_name,
                # retweet_count,
                # favorite_count,
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
                place.id) #,
                # retweeted_status.quoted_status.created_at,
                # retweeted_status.quoted_status.id_str,
                # retweeted_status.quoted_status.retweet_count,
                # retweeted_status.quoted_status.favorite_count,
                # retweeted_status.quoted_status.user.id_str,
                # retweeted_status.quoted_status.user.screen_name,
                # retweeted_status.quoted_status.user.description,
                # retweeted_status.quoted_status.user.location,
                # retweeted_status.quoted_status.user.followers_count,
                # retweeted_status.quoted_status.user.friends_count,
                # retweeted_status.quoted_status.user.listed_count,
                # retweeted_status.quoted_status.user.favourites_count,
                # retweeted_status.quoted_status.user.statuses_count,
                # retweeted_status.quoted_status.extended_tweet.full_text,
                # retweeted_status.quoted_status.text= retweeted_status.quoted_status.full_text,
                #retweeted_status.quoted_status.user.verified)

#print(head(tweetData))

# hashtagData <- distinct_dat %>%
#   select(user.id_str,
#          id_str,
#          entities.hashtags.0.indices.0,
#          entities.hashtags.0.indices.1,
#          entities.hashtags.0.text
#   )

#circeFiles <- "/shares_bgfs/si_twitter/NEW_Hydrated_USC_Twitter_data (Reconstructed USC data)/rawJson/04-12.jsonl"
locationDataLoc <- "/shares_bgfs/si_twitter/covid19/USC_tables/location/"
userDataLoc <- "/shares_bgfs/si_twitter/covid19/USC_tables/user/"
tweetDataLoc <- "/shares_bgfs/si_twitter/covid19/USC_tables/tweet/"
#hashtagDataLoc <- "/shares_bgfs/si_twitter/covid19/USC_tables/hashtag/"


endLocation_pre <- sub('\\..*$', '', endLocation_pre)

readr::write_excel_csv(x = locationData, path = paste0(locationDataLoc,endLocation_pre, "_locationTable.csv"))
#readr::write_excel_csv(x = hashtagData, path = paste0(hashtagDataLoc,endLocation_pre, "_hashtagTable.csv"))
readr::write_excel_csv(x = tweetData, path = paste0(tweetDataLoc,endLocation_pre, "_tweetData.csv"))
readr::write_excel_csv(x = userData, path = paste0(userDataLoc,endLocation_pre, "_userData.csv"))

print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()
