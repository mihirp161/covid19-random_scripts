# SAIL Labs
# ~Dre, Mihir

## Description:
##-------------
#* Same as other tables, this one gets a batch file with json/l inputs and converts them to dataframe.
#* Then we capture only the reply objects and make csvs with them.
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

replyDataLoc <- "/path_to_reply_directory/" #change yours too DreD

endLocation_pre <- sub('\\..*$', '', endLocation_pre) #gets before .json

readr::write_excel_csv(x = replyData, path = paste0(replyDataLoc,endLocation_pre, "_replyTable.csv"))

print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()

#EOF
