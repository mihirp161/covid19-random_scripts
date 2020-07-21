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


#------------------------------------------- File paths and destination paths ---------------------------------#
#setwd("/shares_bgfs/si_twitter/Dred-MPColab/Mary_files_April_jsons/")
args <- commandArgs(trailingOnly = FALSE)
fileNames_all <- args[6] 
fileLocation <- fileNames_all

print(paste0("Reading.....", fileLocation))

endLocation_pre <- stringr::str_extract_all(fileLocation, '[^/]+')
endLocation_pre <- sapply(endLocation_pre,tail, 1) #access the last elemnt of list, it will have .jsonl

print(paste0("true file name: ", endLocation_pre))

#-------------------------------- Read in the json/l files ---------------------------------------------------#
#fileNames_all[i]
#read the json here and make whatever
message("starting...")


fileLocation <- "2020-07-17-18.json"
parsedTweets <- ndjson::stream_in(fileLocation, cls="dt") %>% dplyr::filter(lang == "en")

parsedTweets[parsedTweets == "NA"]  <- ""


##------------------------------ Combine all the multiple columns into one, by ';'---------------------------#

#*************TO DO*************#
#* CONVERT THESE TO FUNCTION    #
#*******************************#

# Then here, & because we only care about the column names, we will read only the first row of the file
multi_cols_names <- colnames(parsedTweets)

# empty dataframe with size of original data
multiples_dat <- data.frame(row.names = 1:nrow(parsedTweets))

#---------------- Mentions

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

#---------------------------------- quoted tags

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

cols <- c(user.id_str= NA_real_,
          id_str= NA_real_,
          created_at= NA_real_,
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

parsedTweets<- tibble::add_column(parsedTweets, !!!cols[dplyr::setdiff(names(cols), names(parsedTweets))])

#singly columns, put additional ones in the furture
distinct_cols <- c("created_at",
                   "user.id_str",
                   "id_str",
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

#------------------------------ Get only the distict columns ------------------------------------------------#

#pick out these distinct columns
distinct_dat <- parsedTweets %>% dplyr::select(dplyr::all_of(distinct_cols))

# make a distinct column df
distinct_dat <- dplyr::bind_cols(distinct_dat, multiples_dat)

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

message("binding cols done...")

#----------------------------------------------------------------------------------------------------------

#update the path
quotedDataLoc <- "C:\\Users\\ThinkPad\\Downloads\\test\\"

endLocation_pre <- sub('\\..*$', '', endLocation_pre)

readr::write_excel_csv(x = quotedData, path = paste0(quotedDataLoc,endLocation_pre, "_quotedData.csv"))


print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()