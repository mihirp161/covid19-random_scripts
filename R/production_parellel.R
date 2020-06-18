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


#--------------------------------------------- Parllel logic----------------------------------------------------#
#fileNames <- fileNames_all

# cl <- makePSOCKcluster(5)
# registerDoParallel(cl)

#setwd("WHATEVER-YOUR-PATH-IS")

#------------------------------------------- File paths and destination paths ---------------------------------#
setwd("C:\\Users\\ThinkPad\\SpyderProjects\\JsonStuff\\folder_with_all_jsonl\\")

fileNames_all <- list.files("C:\\Users\\ThinkPad\\SpyderProjects\\JsonStuff\\folder_with_all_jsonl\\", "*.jsonl$", full.names = FALSE)

# creates a list of all json files in dir as a shortname
#fileNamesShort <- list.files("/shares_bgfs/si_twitter/covid19/USC_March_jsonls_Need_Conversions/", "*_part1.jsonl$", full.names = FALSE)
# creates likst off all csv files in completed dir
#finishedFiles <- list.files("/shares_bgfs/si_twitter/covid19/processedCSVs/", "*.csv", full.names = FALSE)

# establishes output file location
parsedFolder <- "C:\\Users\\ThinkPad\\SpyderProjects\\JsonStuff\\Attempt to fix\\"

for(i in seq_along(fileNames_all)) {
  #-------------------------------- Read in the json/l files ---------------------------------------------------#
  #fileNames_all[i]
  #read the json here and make whatever
  parsedTweets <- ndjson::stream_in(fileNames_all[i], cls="dt") %>% dplyr::filter(lang == "en")
  
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
  
  # Then here, & because we only care about the column names, we will read only the first row of the file
  multi_cols_names <- colnames(parsedTweets)
  
  # empty dataframe with size of original data
  multiples_dat <- data.frame(row.names = 1:nrow(parsedTweets))
  
  
  #------------Coordinates
  # add tweeted coordinates if they exist
  if(identical(grep("^coordinates.coordinates.*", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$user_coordinates <- ""
    
  }else{
    user_coordinates <- c(multi_cols_names[grep("^coordinates.coordinates.*", 
                                                multi_cols_names, ignore.case = T)])
    
    multiples_dat$user_coordinates <- apply(parsedTweets[ , user_coordinates, with= F] , 1 , paste , collapse = '|')
  }
  
  # add retweeted coordinates if they exist
  if(identical(grep("^retweeted_status.coordinates.coordinates.*", multi_cols_names, ignore.case = T),integer(0))){
     
    multiples_dat$retweeted_user_coordinates <- ""
  }else{
    retweeted_user_coordinates <- c(multi_cols_names[grep("^retweeted_status.coordinates.coordinates.*", 
                                                          multi_cols_names, ignore.case = T)])
    
    multiples_dat$retweeted_user_coordinates <- apply(parsedTweets[ , retweeted_user_coordinates, with= F] , 
                                                      1 , paste , collapse = '|')
  }
  
  #check if quoted coordinated exist, add it accordingly
  if(identical(grep("^retweeted_status.quoted_status.coordinates.coordinates.*", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$quoted_user_coordinates <- ""
  }else{
    quoted_user_coordinates <- c(multi_cols_names[grep("retweeted_status.quoted_status.coordinates.coordinates.*", 
                                                       multi_cols_names, ignore.case = T)])
    multiples_dat$quoted_user_coordinates <- apply(parsedTweets[ , quoted_user_coordinates, with= F] , 
                                                      1 , paste , collapse = '|')
  }
  
  #---------------- Mentions
  
  # Mentions screen names
  if(identical(grep("^entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$mentions_screen_name <- ""
  }else{
    mentions_screen_name <- c(multi_cols_names[grep("^entities.user_mentions.*_name$", multi_cols_names, ignore.case = T)])
    multiples_dat$mentions_screen_name <- apply(parsedTweets[ , mentions_screen_name, with= F] , 
                                                   1 , paste , collapse = '|')
  }
  
  #mentions id_strs
  if(identical(grep("^entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$mentions_id_str <- ""
  }else{
    mentions_id_str <- c(multi_cols_names[grep("^entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
    multiples_dat$mentions_id_str <- apply(parsedTweets[ , mentions_id_str, with= F] , 
                                                1 , paste , collapse = '|')
  }
  
  # Retweet mentions screen names
  if(identical(grep("^retweeted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$rt_mentions_screen_name <- ""
  }else{
    rt_mentions_screen_name <- c(multi_cols_names[grep("^retweeted_status.entities.user_mentions.*_name$", 
                                                    multi_cols_names, ignore.case = T)])
    multiples_dat$rt_mentions_screen_name <- apply(parsedTweets[ , rt_mentions_screen_name, with= F] , 
                                                1 , paste , collapse = '|')
  }
  
  #Retweet mentions id_strs
  if(identical(grep("^retweeted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$rt_mentions_id_str <- ""
  }else{
    rt_mentions_id_str <- c(multi_cols_names[grep("^retweeted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
    multiples_dat$rt_mentions_id_str <- apply(parsedTweets[ , rt_mentions_id_str, with= F] , 
                                           1 , paste , collapse = '|')
  }
  
  # Quoted mentions screen names
  if(identical(grep("^retweeted_status.quoted_status.entities.user_mentions.*_name$", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$qu_mentions_screen_name <- ""
  }else{
    qu_mentions_screen_name <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.user_mentions.*_name$", 
                                                       multi_cols_names, ignore.case = T)])
    multiples_dat$qu_mentions_screen_name <- apply(parsedTweets[ , qu_mentions_screen_name, with= F] , 
                                                   1 , paste , collapse = '|')
  }
  
  # Quoted mentions id_strs
  if(identical(grep("^retweeted_status.quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T),integer(0))){
    multiples_dat$qu_mentions_id_str <- ""
  }else{
    qu_mentions_id_str <- c(multi_cols_names[grep("^retweeted_status.quoted_status.entities.user_mentions.*_str$", multi_cols_names, ignore.case = T)])
    multiples_dat$qu_mentions_id_str <- apply(parsedTweets[ , qu_mentions_id_str, with= F] , 
                                              1 , paste , collapse = '|')
  }
  
  #------------------------------ Get only the distict columns ------------------------------------------------#
  #singly columns, put additional ones in the furture
  distinct_cols <- c("created_at",
                     "id_str",
                     "full_text", # user tweeted | userTweeted
                     "in_reply_to_status_id_str",
                     "in_reply_to_user_id_str",
                     "in_reply_to_screen_name",
                     "retweet_count",
                     "favorite_count",
                     "retweeted_status.created_at",
                     "retweeted_status.id_str", # retweet id | retweetOriginalID
                     "retweeted_status.full_text", # retweet txt | retweetOriginalText
                     "retweeted_status.retweet_count",
                     "retweeted_status.favorite_count",
                     "user.id_str", # user id | userID
                     "user.screen_name", #userHandle |userHandle
                     "user.description", # profile description | userDescription
                     "user.location", #user location from description | userLocation
                     "user.followers_count", # number of followers | userFollowerCount
                     "user.friends_count", # number of people user follows | userFollowingCount
                     "user.listed_count",
                     "user.statuses_count", # number of tweets | userNumberOfTweets
                     "user.favourites_count", # number of times the user liked a status | userNumberOfLikedStatuses
                     "user.created_at", # time when user created account | userAccountCreationDate
                     "retweeted_status.user.id_str", # # retweet userid | retweetOriginalUserID
                     "retweeted_status.user.screen_name",# retweeted user screen name | retweetOriginalUserHandle
                     "retweeted_status.user.description",
                     "retweeted_status.user.location",
                     "retweeted_status.user.followers_count", #retweeted user followers count | retweetOriginalUserFollowerCount
                     "retweeted_status.user.friends_count", # retweeted user following count | retweetOriginalUserFollowingCount
                     "retweeted_status.user.listed_count",
                     "retweeted_status.user.statuses_count", # retweeted user total number of tweets 
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
                     "user.verified", # blue check mark | userAccountBlueCheckmark
                     "retweeted_status.user.verified",# blue check markretweetOriginalUserAccountBlueCheckmark
                     "retweeted_status.quoted_status.user.verified") #,
                     ##"retweeted_status.entities.urls.url", # retweet Original Link| retweetLink
                     ##"#retweeted_status.extended_entities.media.media_url_https", # retweet media| retweetOriginalMedia)
  
  
  # make a distinct column df
  
  distinct_dat <- parsedTweets %>% dplyr::select(dplyr::all_of(distinct_cols))
  
  parsedTweets <- dplyr::bind_rows(distinct_dat, multiples_dat)
  
  #----------------------------------------------------------------------------------------------------------
  
  # names <- c("userHandle", "userID", 
  #            "userLocation","userTweeted", 
  #            "userFollowerCount", "userFollowingCount",
  #            "userNumberOfLikedStatuses", "userNumberOfTweets",
  #            "userAccountCreationDate", "userDescription",
  #            "userAccountBlueCheckmark", "retweetOriginalID",
  #            "retweetOriginalText", "retweetLink",
  #            "retweetOriginalMedia","retweetOriginalUserID",
  #            "retweetOriginalUserHandle", "retweetOriginalUserFollowerCount",
  #            "retweetOriginalUserFollowingCount", "retweetOriginalUserNumberOfTweets", 
  #            "retweetOriginalUserAccountBlueCheckmark", "retweetTotalCount")
  #colnames(parsedTweets) <- names
  # creates filename from existing string
  
  # Then writes csv with excel format to preserve special chars
  readr::write_excel_csv(parsedTweets, path=paste0(parsedFolder,sub('\\..*$','',fileNames_all[i]),".csv"))
  
  remove(parsedTweets) 
  # outputs last processed file
  print(paste0("Finished with: ",sub('\\..*$','', i)))
}

# loop to save memory
#remove(parsedTweets)

#stopCluster(cl)

