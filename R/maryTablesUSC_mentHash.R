# SAIL Labs
# ~Dre, Mihir

## Description:
##-------------
#* This file converts jsons to csvs and make mentions and hashtags. Then places them
#* in their designated place. This takes a batch file to read through each json/l file.
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


##------------------------------ Combine all the multiple columns into one, by ';'---------------------------#

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

#------------------------------ Get only the distict columns ------------------------------------------------#
#singly columns, put additional ones in the furture
distinct_cols <- c("user.id_str",
                   "id_str") 

#pick out these distinct columns
distinct_dat <- parsedTweets %>% dplyr::select(dplyr::all_of(distinct_cols))

parsedTweets <- dplyr::bind_cols(distinct_dat, multiples_dat)


message("binding cols done...")

#----------------------------------------------------------------------------------------------------------

#update the path
mentHashDataLoc <- "mentions_hashtags_path"

endLocation_pre <- sub('\\..*$', '', endLocation_pre)

readr::write_excel_csv(x = parsedTweets, path = paste0(mentHashDataLoc,endLocation_pre, "_mentionHastagsTable.csv"))


print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()

#EOF