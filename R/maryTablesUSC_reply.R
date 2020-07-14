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

#------------------------------------------- File paths and destination paths ---------------------------------#
args <- commandArgs(trailingOnly = FALSE)
fileNames_all <- args[6] #list.files("/shares_bgfs/si_twitter/Dred-MPColab/Mary_files_April_jsons/", "*.jsonl$", full.names = FALSE)
fileLocation <- fileNames_all

print(paste0("Reading.....", fileLocation))

endLocation_pre <- stringr::str_extract_all(fileLocation, '[^/]+')
endLocation_pre <- sapply(endLocation_pre,tail, 1) #access the last elemnt of list, it will have .jsonl

print(paste0("true file name: ", endLocation_pre))

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


#------------------------------ Get only the distict columns ------------------------------------------------#
#singly columns, put additional ones in the furture
distinct_cols <- c("user.id_str",
                   "id_str",
                   "in_reply_to_status_id_str",
                   "in_reply_to_user_id_str",
                   "in_reply_to_screen_name",
                   "retweeted_status.in_reply_to_status_id_str",
                   "retweeted_status.in_reply_to_screen_name",
                   "retweeted_status.in_reply_to_user_id_str") 

# make a distinct column df

distinct_dat <- parsedTweets %>% dplyr::select(dplyr::all_of(distinct_cols))

message("binding cols done...")

#----------------------------------------------------------------------------------------------------------
# get the replies
replyData <- distinct_dat %>% 
              dplyr::select(user.id_str,
                            id_str,
                            in_reply_to_status_id_str,
                            in_reply_to_user_id_str,
                            in_reply_to_screen_name,
                            retweeted_status.in_reply_to_status_id_str,
                            retweeted_status.in_reply_to_screen_name,
                            retweeted_status.in_reply_to_user_id_str)

replyDataLoc <- "/shares_bgfs/si_twitter/covid19/USC_tables/reply_to/" #change your too DreD

endLocation_pre <- sub('\\..*$', '', endLocation_pre) #get before .json

readr::write_excel_csv(x = replyData, path = paste0(replyDataLoc,endLocation_pre, "_replyTable.csv"))

print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()
