# SAIL Labs
# ~Dre, Mihir

## Description:
##-------------
#* This file make a csv file according to what Dr Nic needs. This one specifically
#* focuses on tweet and location related data. Again, uses batch file, and converts json/l file
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

#---------------- hashtag
# get hashtags for the user
if(identical(grep("^entities.hashtags.*.text$", multi_cols_names, ignore.case = T),integer(0))){
  multiples_dat$hashtags <- ""
}else{
  hashtags <- c(multi_cols_names[grep("^entities.hashtags.*.text$", multi_cols_names, ignore.case = T)])
  multiples_dat$hashtags <- apply(parsedTweets[ , hashtags, with= F] ,
                                  1 , paste , collapse = '; ')
  
  multiples_dat$hashtags <- gsub('NA|NA; ', '', multiples_dat$hashtags)
}


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

#mentions symbols text
if(identical(grep("^entities.symbols.*.text$", colnames(parsedTweets), ignore.case = T),integer(0))){
  multiples_dat$symbols <- ""
}else{
  symbols <- c(multi_cols_names[grep("^entities.symbols.*.text$", colnames(parsedTweets), ignore.case = T)])
  multiples_dat$symbols <- apply(parsedTweets[ , symbols, with= F] ,
                                 1 , paste , collapse = '; ')
  
  multiples_dat$symbols <- gsub('NA|NA; ', '', multiples_dat$symbols)
}

#mentions bbox
if(identical(grep("^place.bounding_box.coordinates\\.", colnames(parsedTweets), ignore.case = T),integer(0))){
  multiples_dat$bbox <- ""
}else{
  bbox <- c(multi_cols_names[grep("^place.bounding_box.coordinates\\.", colnames(parsedTweets), ignore.case = T)])
  multiples_dat$bbox <- apply(parsedTweets[ , bbox, with= F] ,
                              1 , paste , collapse = '; ')
  
  multiples_dat$bbox <- gsub('NA|NA; ', '', multiples_dat$bbox)
}

#------------------------------ Get only the distict columns ------------------------------------------------#

cols <- c(id_str= NA_real_,
          created_at= NA_real_,
          user.id_str= NA_real_,
          user.screen_name= NA_real_,
          full_text= NA_real_, #turn this into comment when USC data is present
          extended_tweet.full_text= NA_real_, #turn this into comment when USF data is present
          source= NA_real_,
          is_quote_status= NA_real_,
          retweeted= NA_real_,
          favorite_count= NA_real_,
          retweet_count= NA_real_,
          place.url= NA_real_,
          place.name= NA_real_,
          place.full_name= NA_real_,
          place.place_type= NA_real_,
          place.country= NA_real_,
          place.country_code= NA_real_,
          geo.coordinates.0= NA_real_,
          geo.coordinates.1= NA_real_,
          coordinates.coordinates.0= NA_real_,
          coordinates.coordinates.1= NA_real_)

parsedTweets<- tibble::add_column(parsedTweets, !!!cols[dplyr::setdiff(names(cols), names(parsedTweets))])

#singly columns, put additional ones in the furture
distinct_cols <- c("id_str",
                   "created_at",
                   "user.id_str",
                   "user.screen_name",
                   "full_text", #turn this into comment when USC data is present
                   "extended_tweet.full_text", #turn this into comment when USF data is present
                   "source",
                   "is_quote_status",
                   "retweeted",
                   "favorite_count",
                   "retweet_count",
                   "place.url",
                   "place.name",
                   "place.full_name",
                   "place.place_type",
                   "place.country",
                   "place.country_code",
                   "geo.coordinates.0",
                   "geo.coordinates.1",
                   "coordinates.coordinates.0",
                   "coordinates.coordinates.1")

#------------------------------ Get only the distict columns ------------------------------------------------#

#pick out these distinct columns
distinct_dat <- parsedTweets %>% dplyr::select(dplyr::all_of(distinct_cols))

# make a distinct column df
distinct_dat <- dplyr::bind_cols(distinct_dat, multiples_dat)

nicData <- distinct_dat %>%
         dplyr::select(id_str,
                       created_at,
                       user.id_str,
                       user.screen_name,
                       full_text, #turn this into comment when USC data is present
                       extended_tweet.full_text, #turn this into comment when USF data is present
                       source,
                       is_quote_status,
                       retweeted,
                       favorite_count,
                       retweet_count,
                       place.url,
                       place.name,
                       place.full_name,
                       place.place_type,
                       place.country,
                       place.country_code,
                       geo.coordinates.0,
                       geo.coordinates.1,
                       coordinates.coordinates.0,
                       coordinates.coordinates.1,
                       bbox,
                       symbols,
                       mentions_id_str,
                       mentions_screen_name,
                       hashtags)

message("binding cols done...")

#----------------------------------------------------------------------------------------------------------

#update the path
nicDataLoc <- '/nic_data_path/'

endLocation_pre <- sub('\\..*$', '', endLocation_pre)

readr::write_excel_csv(x = nicData, path = paste0(nicDataLoc,endLocation_pre, "_nicData.csv"))


print(paste0("Finished with: ",endLocation_pre))
rm(list=ls())
quit()

#EOF