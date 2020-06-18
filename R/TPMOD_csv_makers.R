# FOR USF SAIL
# ~Mihir

library(readr)
library(dplyr)

#***********************NOTE!!!!******************#
#*"retweeted_status.quoted_status.user.created_at",
#*
#* (Don't forget to uncomment the column above in
#* distinct_cols in applicable cases)
#* 
#*************************************************#

#get the folder path
setwd("C:/Users/ThinkPad/SpyderProjects/JsonStuff/Attempt to fix/")

#get all the files
files <- list.files(pattern = "*.csv")

# #get the file names so we can overwrite them
# names <- sub('\\..*$','', files)

#read each file in to a list
dat <- lapply(files, readr::read_csv)

# these are the columns that are good
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
                   #"retweeted_status.quoted_status.user.created_at",
                   "place.full_name",
                   "retweeted_status.place.full_name",
                   "retweeted_status.quoted_status.place.full_name",
                   "place.country_code",
                   "retweeted_status.place.country_code",
                   "retweeted_status.quoted_status.place.country_code",
                   "user.verified", # blue check mark | userAccountBlueCheckmark
                   "retweeted_status.user.verified",# blue check markretweetOriginalUserAccountBlueCheckmark
                   "retweeted_status.quoted_status.user.verified") #,

for(i in seq_along(dat)){
  
  truncated_dat <- dat[[i]]
  
  #reduce the dataframe to what we need
  truncated_dat <- truncated_dat %>% dplyr::select(dplyr::all_of(distinct_cols))
  
  #remove the rows if create_at has an NA. These are garbage that may be left over by old dfs
  truncated_dat <- truncated_dat[!is.na(truncated_dat$created_at), ]
  
  #overwrite with good columns in excel format
  readr::write_excel_csv(truncated_dat, path=paste0(sub('\\..*$','',files[i]),".csv"))
  
  remove(truncated_dat)
}